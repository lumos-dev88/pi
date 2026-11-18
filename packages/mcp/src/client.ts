import { setTimeout as sleep } from "node:timers/promises";
import type { CallToolResult } from "./protocol/content.ts";
import {
	isJsonRpcId,
	isJsonRpcNotification,
	isJsonRpcResponse,
	isObject,
	JSON_RPC_ERROR_CODES,
	type JsonRpcId,
	type JsonRpcMessage,
	type JsonRpcRequest,
	type JsonRpcResponse,
	McpAbortError,
	McpConnectionClosedError,
	McpError,
	McpTimeoutError,
	toError,
} from "./protocol/jsonrpc.ts";
import { readTaskSnapshot, type TaskHandle, type TaskSnapshot } from "./protocol/tasks.ts";
import {
	CLIENT_CAPABILITIES_META,
	CLIENT_INFO_META,
	type ClientCapabilities,
	type Implementation,
	type ListResourcesResult,
	type ListResourceTemplatesResult,
	LOG_LEVEL_META,
	PROTOCOL_VERSION_META,
	type ProgressNotification,
	type ReadResourceResult,
	type Resource,
	type ResourceTemplate,
	type Root,
	SERVER_INFO_META,
	type ServerCapabilities,
	SUBSCRIPTION_ID_META,
	TASKS_EXTENSION,
	TASKS_PROTOCOL_VERSION,
	type Tool,
} from "./protocol/types.ts";
import type { McpTransport } from "./transports/transport.ts";

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
const MAX_LIST_PAGES = 1_000;
const TOOLS_LIST_CHANGED = "notifications/tools/list_changed";

type ClientState = "idle" | "connecting" | "connected" | "closed";
type NotificationListener = (params: unknown) => void;
type ErrorListener = (error: Error) => void;
type CloseListener = () => void;
type RequestHandler = (params: unknown, context: { signal: AbortSignal }) => unknown | Promise<unknown>;

interface ToolSubscription {
	controller: AbortController;
	requestId?: JsonRpcId;
	acknowledged: boolean;
}

export interface McpClientOptions extends Implementation {
	capabilities?: ClientCapabilities;
	requestTimeoutMs?: number;
	roots?: readonly Root[] | (() => readonly Root[] | Promise<readonly Root[]>);
}

export interface McpRequestOptions {
	signal?: AbortSignal;
	timeoutMs?: number;
	onProgress?: (progress: ProgressNotification) => void;
}

/** Connection information; stateless servers may omit their identity. */
export interface McpConnectionInfo {
	protocolVersion: typeof TASKS_PROTOCOL_VERSION;
	capabilities: ServerCapabilities;
	serverInfo?: Implementation;
	instructions?: string;
}

interface RequestMetadata extends Record<string, unknown> {
	[PROTOCOL_VERSION_META]: typeof TASKS_PROTOCOL_VERSION;
	[CLIENT_CAPABILITIES_META]: ClientCapabilities;
	[CLIENT_INFO_META]: Implementation;
}

interface PendingRequest {
	resolve: (value: unknown) => void;
	reject: (reason: unknown) => void;
	timeoutMs: number;
	timer: ReturnType<typeof setTimeout> | undefined;
	signal: AbortSignal | undefined;
	onAbort: () => void;
	/** Aborted when the request settles, so the transport does not dispatch (or retry) it afterwards. */
	sendController: AbortController;
	mayCreateTask: boolean;
	onProgress: ((progress: ProgressNotification) => void) | undefined;
	progressToken: JsonRpcId | undefined;
}

function invalid(message: string): McpError {
	return new McpError(JSON_RPC_ERROR_CODES.invalidRequest, message);
}

function validateDiscoverResult(value: unknown): McpConnectionInfo {
	if (
		!isObject(value) ||
		value.resultType !== "complete" ||
		!Array.isArray(value.supportedVersions) ||
		!value.supportedVersions.includes(TASKS_PROTOCOL_VERSION) ||
		!isObject(value.capabilities) ||
		(value.instructions !== undefined && typeof value.instructions !== "string")
	) {
		throw invalid(`Invalid MCP server/discover result or server does not support ${TASKS_PROTOCOL_VERSION}`);
	}
	const info = isObject(value._meta) ? value._meta[SERVER_INFO_META] : undefined;
	if (info !== undefined && (!isObject(info) || typeof info.name !== "string" || typeof info.version !== "string")) {
		throw invalid("Invalid MCP server/discover server identity");
	}
	return {
		protocolVersion: TASKS_PROTOCOL_VERSION,
		capabilities: value.capabilities,
		serverInfo: info as Implementation | undefined,
		instructions: value.instructions,
	};
}

/** One page of a paginated list: the items under `key`, each checked by `isItem`. */
function validateListPage(
	method: string,
	key: string,
	value: unknown,
	isItem: (item: Record<string, unknown>) => boolean,
): { items: Record<string, unknown>[]; nextCursor?: string } {
	const items = isObject(value) ? value[key] : undefined;
	if (!isObject(value) || !Array.isArray(items)) throw invalid(`Invalid MCP ${method} result`);
	for (const item of items) {
		if (!isObject(item) || !isItem(item)) throw invalid(`Invalid entry in MCP ${method} result`);
	}
	// Some servers end pagination with `null` or `""` instead of omitting the cursor.
	const nextCursor = value.nextCursor === null || value.nextCursor === "" ? undefined : value.nextCursor;
	if (nextCursor !== undefined && typeof nextCursor !== "string") throw invalid(`Invalid MCP ${method} cursor`);
	return { items, ...(nextCursor === undefined ? {} : { nextCursor }) };
}

const isTool = (tool: Record<string, unknown>) => typeof tool.name === "string" && isObject(tool.inputSchema);
// `name` is required by the spec, but some servers omit it; the URI stands in.
const isResource = (resource: Record<string, unknown>) =>
	typeof resource.uri === "string" && (resource.name === undefined || typeof resource.name === "string");
const isResourceTemplate = (template: Record<string, unknown>) =>
	typeof template.uriTemplate === "string" && (template.name === undefined || typeof template.name === "string");

function toResource(item: Record<string, unknown>): Resource {
	return { ...item, name: item.name ?? item.uri } as Resource;
}

function toResourceTemplate(item: Record<string, unknown>): ResourceTemplate {
	return { ...item, name: item.name ?? item.uriTemplate } as ResourceTemplate;
}

function pageCursor(page: { nextCursor?: string }): { nextCursor?: string } {
	return page.nextCursor === undefined ? {} : { nextCursor: page.nextCursor };
}

function validateReadResourceResult(value: unknown): ReadResourceResult {
	if (!isObject(value) || !Array.isArray(value.contents)) throw invalid("Invalid MCP resources/read result");
	for (const contents of value.contents) {
		if (
			!isObject(contents) ||
			typeof contents.uri !== "string" ||
			(typeof contents.text !== "string" && typeof contents.blob !== "string")
		) {
			throw invalid("Invalid contents in MCP resources/read result");
		}
	}
	return value as unknown as ReadResourceResult;
}

/**
 * `content` is required by the spec, but servers that only return `structuredContent` omit it (the SDK defaults it too).
 * `structuredContent` may be any JSON value, as MCP 2026-07-28 allows.
 */
function validateCallToolResult(value: unknown): CallToolResult {
	if (!isObject(value) || (value.content !== undefined && !Array.isArray(value.content))) {
		throw invalid("Invalid MCP tools/call result");
	}
	if (value.resultType !== undefined && value.resultType !== "complete") {
		throw invalid(`Unexpected MCP tools/call resultType: ${String(value.resultType)}`);
	}
	return (value.content === undefined ? { ...value, content: [] } : value) as unknown as CallToolResult;
}

export class McpClient {
	readonly options: Readonly<McpClientOptions>;
	private state: ClientState = "idle";
	private transport: McpTransport | undefined;
	private nextRequestId = 1;
	private serverInfoValue: Implementation | undefined;
	private serverCapabilitiesValue: ServerCapabilities | undefined;
	private instructionsValue: string | undefined;
	private protocolVersionValue: typeof TASKS_PROTOCOL_VERSION | undefined;
	private readonly metadata: RequestMetadata;
	private tasks: { metadata: RequestMetadata; lifetime: AbortController } | undefined;
	private toolSubscription: ToolSubscription | undefined;
	private lateTasks: Map<JsonRpcId, NodeJS.Timeout> | undefined;
	private pending = new Map<JsonRpcId, PendingRequest>();
	private progressRequests = new Map<JsonRpcId, JsonRpcId>();
	private requestHandlers = new Map<string, RequestHandler>();
	private notificationListeners = new Map<string, Set<NotificationListener>>();
	private errorListeners = new Set<ErrorListener>();
	private closeListeners = new Set<CloseListener>();
	private disposers: (() => void)[] = [];

	constructor(options: McpClientOptions) {
		this.options = Object.freeze({ ...options });
		const capabilities = { ...options.capabilities };
		if (options.roots && capabilities.roots === undefined) capabilities.roots = {};
		const { [TASKS_EXTENSION]: _tasks, ...extensions } = capabilities.extensions ?? {};
		this.metadata = {
			[PROTOCOL_VERSION_META]: TASKS_PROTOCOL_VERSION,
			[CLIENT_INFO_META]: {
				name: options.name,
				version: options.version,
				...(options.title === undefined ? {} : { title: options.title }),
			},
			[CLIENT_CAPABILITIES_META]: { ...capabilities, extensions },
		};
		const roots = options.roots;
		if (roots) {
			this.requestHandlers.set("roots/list", async () => ({
				roots: [...(typeof roots === "function" ? await roots() : roots)],
			}));
		}
	}

	get connectionState(): ClientState {
		return this.state;
	}

	get serverInfo(): Implementation | undefined {
		return this.serverInfoValue;
	}

	get serverCapabilities(): ServerCapabilities | undefined {
		return this.serverCapabilitiesValue;
	}

	get instructions(): string | undefined {
		return this.instructionsValue;
	}

	get protocolVersion(): typeof TASKS_PROTOCOL_VERSION | undefined {
		return this.protocolVersionValue;
	}

	async connect(transport: McpTransport): Promise<McpConnectionInfo> {
		if (this.state !== "idle") throw new Error(`Cannot connect MCP client in ${this.state} state`);
		this.state = "connecting";
		this.transport = transport;
		this.disposers = [
			transport.onMessage((message) => this.handleMessage(message)),
			// Transport errors are reported only. Pending requests fail when the transport closes.
			transport.onError((error) => this.emitError(error)),
			transport.onClose(() => this.handleTransportClose()),
		];

		try {
			await transport.start();
			const result = validateDiscoverResult(await this.requestInternal("server/discover", undefined, {}, true));
			const serverExtensions = result.capabilities.extensions;
			if (isObject(serverExtensions) && Object.hasOwn(serverExtensions, TASKS_EXTENSION)) {
				const capabilities = this.metadata[CLIENT_CAPABILITIES_META];
				this.tasks = {
					metadata: {
						...this.metadata,
						[CLIENT_CAPABILITIES_META]: {
							...capabilities,
							extensions: { ...capabilities.extensions, [TASKS_EXTENSION]: {} },
						},
					},
					lifetime: new AbortController(),
				};
			}
			this.protocolVersionValue = result.protocolVersion;
			this.serverInfoValue = result.serverInfo;
			this.serverCapabilitiesValue = result.capabilities;
			this.instructionsValue = result.instructions;
			this.state = "connected";
			this.subscribeToTools();
			return result;
		} catch (error) {
			await this.close().catch(() => {});
			throw error;
		}
	}

	request<Result = unknown>(
		method: string,
		params?: Record<string, unknown>,
		options: McpRequestOptions = {},
	): Promise<Result> {
		return this.requestInternal(method, params, options, false) as Promise<Result>;
	}

	notify(method: string, params?: Record<string, unknown>): Promise<void> {
		const transport = this.requireTransport(false);
		return transport.send({ jsonrpc: "2.0", method, ...(params === undefined ? {} : { params }) });
	}

	setRequestHandler(method: string, handler: RequestHandler): () => void {
		this.requestHandlers.set(method, handler);
		return () => {
			if (this.requestHandlers.get(method) === handler) this.requestHandlers.delete(method);
		};
	}

	onNotification(method: string, listener: NotificationListener): () => void {
		const listeners = this.notificationListeners.get(method) ?? new Set<NotificationListener>();
		this.notificationListeners.set(method, listeners);
		listeners.add(listener);
		if (method === TOOLS_LIST_CHANGED) this.subscribeToTools();
		return () => {
			listeners.delete(listener);
			if (listeners.size !== 0) return;
			this.notificationListeners.delete(method);
			if (method === TOOLS_LIST_CHANGED && this.toolSubscription) {
				const subscription = this.toolSubscription;
				this.toolSubscription = undefined;
				subscription.controller.abort();
			}
		};
	}

	onError(listener: ErrorListener): () => void {
		this.errorListeners.add(listener);
		return () => this.errorListeners.delete(listener);
	}

	/** Called once when the connection closes, whether the transport dropped or `close()` was called. */
	onClose(listener: CloseListener): () => void {
		this.closeListeners.add(listener);
		return () => this.closeListeners.delete(listener);
	}

	private subscribeToTools(): void {
		if (
			this.state !== "connected" ||
			this.toolSubscription ||
			this.serverCapabilitiesValue?.tools?.listChanged !== true ||
			!this.notificationListeners.has(TOOLS_LIST_CHANGED)
		) {
			return;
		}
		const subscription: ToolSubscription = { controller: new AbortController(), acknowledged: false };
		this.toolSubscription = subscription;
		const { signal } = subscription.controller;
		void this.requestInternal(
			"subscriptions/listen",
			{ notifications: { toolsListChanged: true } },
			{ signal },
			false,
			subscription,
		)
			.catch((error) => {
				if (!signal.aborted && this.state === "connected") this.emitError(error);
			})
			.finally(() => {
				if (this.toolSubscription === subscription) this.toolSubscription = undefined;
			});
	}

	async listTools(options: McpRequestOptions = {}): Promise<Tool[]> {
		return (await this.listAll("tools/list", "tools", isTool, options)) as unknown as Tool[];
	}

	/** Every resource, following `nextCursor` through all pages. */
	async listResources(options: McpRequestOptions = {}): Promise<Resource[]> {
		return (await this.listAll("resources/list", "resources", isResource, options)).map(toResource);
	}

	/** One page of resources, starting at `cursor`. */
	async listResourcesPage(cursor?: string, options: McpRequestOptions = {}): Promise<ListResourcesResult> {
		const page = await this.listPage("resources/list", "resources", isResource, cursor, options);
		return { resources: page.items.map(toResource), ...pageCursor(page) };
	}

	/** Every resource template, following `nextCursor` through all pages. */
	async listResourceTemplates(options: McpRequestOptions = {}): Promise<ResourceTemplate[]> {
		const templates = await this.listAll(
			"resources/templates/list",
			"resourceTemplates",
			isResourceTemplate,
			options,
		);
		return templates.map(toResourceTemplate);
	}

	/** One page of resource templates, starting at `cursor`. */
	async listResourceTemplatesPage(
		cursor?: string,
		options: McpRequestOptions = {},
	): Promise<ListResourceTemplatesResult> {
		const page = await this.listPage(
			"resources/templates/list",
			"resourceTemplates",
			isResourceTemplate,
			cursor,
			options,
		);
		return { resourceTemplates: page.items.map(toResourceTemplate), ...pageCursor(page) };
	}

	async readResource(uri: string, options: McpRequestOptions = {}): Promise<ReadResourceResult> {
		return validateReadResourceResult(await this.request("resources/read", { uri }, options));
	}

	private async listPage(
		method: string,
		key: string,
		isItem: (item: Record<string, unknown>) => boolean,
		cursor: string | undefined,
		options: McpRequestOptions,
	): Promise<{ items: Record<string, unknown>[]; nextCursor?: string }> {
		return validateListPage(
			method,
			key,
			await this.request(method, cursor === undefined ? undefined : { cursor }, options),
			isItem,
		);
	}

	/** Every item of a paginated list method. */
	private async listAll(
		method: string,
		key: string,
		isItem: (item: Record<string, unknown>) => boolean,
		options: McpRequestOptions,
	): Promise<Record<string, unknown>[]> {
		const items: Record<string, unknown>[] = [];
		const cursors = new Set<string>();
		let cursor: string | undefined;
		for (let pageNumber = 0; pageNumber < MAX_LIST_PAGES; pageNumber++) {
			const page = await this.listPage(method, key, isItem, cursor, options);
			items.push(...page.items);
			if (page.nextCursor === undefined) return items;
			if (cursors.has(page.nextCursor))
				throw new Error(`MCP ${method} returned duplicate cursor: ${page.nextCursor}`);
			cursors.add(page.nextCursor);
			cursor = page.nextCursor;
		}
		throw new Error(`MCP ${method} exceeded ${MAX_LIST_PAGES} pages`);
	}

	async callTool(
		name: string,
		args?: Record<string, unknown>,
		options: McpRequestOptions = {},
	): Promise<CallToolResult> {
		const params = { name, ...(args === undefined ? {} : { arguments: args }) };
		const result = await this.request("tools/call", params, options);
		if (this.tasks && isObject(result) && result.resultType === "task") {
			return this.finishTask(readTaskSnapshot(result, "task"), options, this.tasks.lifetime.signal);
		}
		return validateCallToolResult(result);
	}

	private async finishTask(
		handle: TaskHandle,
		options: McpRequestOptions,
		lifetime: AbortSignal,
	): Promise<CallToolResult> {
		const signal = options.signal ? AbortSignal.any([lifetime, options.signal]) : lifetime;
		const requestOptions = { signal, timeoutMs: options.timeoutMs };
		const { taskId } = handle;
		let task: TaskHandle | TaskSnapshot = handle;
		let lastProgress: string | undefined;
		let progress = 0;
		let answered: Set<string> | undefined;
		try {
			for (;;) {
				if (signal.aborted) throw signal.reason;
				if (task.resultType === "complete") {
					switch (task.status) {
						case "completed":
							return validateCallToolResult(task.result);
						case "failed":
							throw task.error;
						case "cancelled":
							throw new McpAbortError(task.statusMessage ?? `MCP task ${taskId} was cancelled`);
					}
				}
				if (options.onProgress) {
					const message = `Task ${taskId}: ${task.statusMessage ?? task.status}`;
					if (message !== lastProgress) {
						lastProgress = message;
						try {
							options.onProgress({ progressToken: taskId, progress: ++progress, message });
						} catch (error) {
							this.emitError(error);
						}
					}
				}
				if (task.resultType === "complete" && task.status === "input_required") {
					answered ??= new Set();
					const inputResponses = await this.answerTaskInputs(task.inputRequests, answered, signal);
					if (Object.keys(inputResponses).length) {
						await this.request("tasks/update", { taskId, inputResponses }, requestOptions);
					}
				}
				// Only tasks/get returns the payload, even if the handle already reports a terminal state.
				if (task.resultType === "complete" || task.status === "working") {
					await sleep(task.pollIntervalMs, undefined, { signal });
				}
				task = readTaskSnapshot(await this.request("tasks/get", { taskId }, requestOptions), "complete", taskId);
			}
		} catch (error) {
			if ((task.status === "working" || task.status === "input_required") && this.state === "connected") {
				this.cancelTask(taskId);
			}
			if (lifetime.aborted) throw lifetime.reason;
			if (options.signal?.aborted) throw new McpAbortError();
			throw error;
		}
	}

	private async answerTaskInputs(
		requests: Record<string, unknown>,
		answered: Set<string>,
		signal: AbortSignal,
	): Promise<Record<string, unknown>> {
		const metadata = this.tasks?.metadata[CLIENT_CAPABILITIES_META];
		const responses: Record<string, unknown> = Object.create(null);
		for (const [id, request] of Object.entries(requests)) {
			if (answered.has(id)) continue;
			signal.throwIfAborted();
			if (!isObject(request) || typeof request.method !== "string") throw invalid("Invalid MCP task input request");
			const capability =
				request.method === "roots/list"
					? metadata?.roots
					: request.method === "sampling/createMessage"
						? metadata?.sampling
						: request.method === "elicitation/create"
							? metadata?.elicitation
							: undefined;
			const handler = capability && this.requestHandlers.get(request.method);
			if (!handler) throw invalid(`Unsupported MCP task input method: ${request.method}`);
			responses[id] = (await handler(request.params, { signal })) ?? {};
			answered.add(id);
		}
		return responses;
	}

	private cancelTask(taskId: string): void {
		// Cancellation is cooperative. Do not hold up the caller or use its already aborted signal.
		void this.request("tasks/cancel", { taskId }, { timeoutMs: 1_000 }).catch(() => {});
	}

	async close(): Promise<void> {
		const transport = this.transport;
		this.transport = undefined;
		this.disposeTransportListeners();
		this.markClosed(new McpConnectionClosedError());
		await transport?.close();
	}

	private async requestInternal(
		method: string,
		params: Record<string, unknown> | undefined,
		options: McpRequestOptions,
		allowConnecting: boolean,
		subscription?: ToolSubscription,
	): Promise<unknown> {
		const transport = this.requireTransport(allowConnecting);
		if (options.signal?.aborted) throw new McpAbortError();
		const id = this.nextRequestId++;
		if (subscription) subscription.requestId = id;
		const progressToken = options.onProgress ? id : undefined;
		const metadata =
			this.tasks && (method === "tools/call" || method.startsWith("tasks/")) ? this.tasks.metadata : this.metadata;
		const message: JsonRpcRequest = {
			jsonrpc: "2.0",
			id,
			method,
			params: {
				...params,
				_meta: {
					...(isObject(params?._meta) ? params._meta : {}),
					...metadata,
					...(this.notificationListeners.has("notifications/message") ? { [LOG_LEVEL_META]: "info" } : {}),
					...(progressToken === undefined ? {} : { progressToken }),
				},
			},
		};
		return new Promise<unknown>((resolve, reject) => {
			const entry: PendingRequest = {
				resolve,
				reject,
				timeoutMs: options.timeoutMs ?? this.options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS,
				timer: undefined,
				signal: options.signal,
				onAbort: () =>
					this.cancelPending(id, new McpAbortError(), true, String(options.signal?.reason ?? "Aborted")),
				sendController: new AbortController(),
				mayCreateTask: this.tasks !== undefined && method === "tools/call",
				onProgress: options.onProgress,
				progressToken,
			};
			this.pending.set(id, entry);
			if (progressToken !== undefined) this.progressRequests.set(progressToken, id);
			options.signal?.addEventListener("abort", entry.onAbort, { once: true });
			this.armTimeout(id, entry);
			transport
				.send(message, { signal: entry.sendController.signal })
				.catch((error) => this.cancelPending(id, error, false));
		});
	}

	private requireTransport(allowConnecting: boolean): McpTransport {
		if (this.transport && (this.state === "connected" || (allowConnecting && this.state === "connecting"))) {
			return this.transport;
		}
		throw new McpConnectionClosedError(`MCP client is ${this.state}`);
	}

	private handleMessage(message: JsonRpcMessage): void {
		if (isJsonRpcResponse(message)) {
			this.handleResponse(message);
			return;
		}
		if (isJsonRpcNotification(message)) {
			this.handleNotification(message.method, message.params);
			return;
		}
		this.emitError(new McpError(JSON_RPC_ERROR_CODES.invalidRequest, "Received invalid JSON-RPC message"));
	}

	private handleResponse(message: JsonRpcResponse): void {
		const entry = this.pending.get(message.id);
		if (!entry) {
			const timer = this.lateTasks?.get(message.id);
			if (timer) {
				clearTimeout(timer);
				this.lateTasks?.delete(message.id);
				if ("result" in message && isObject(message.result) && message.result.resultType === "task") {
					try {
						this.cancelTask(readTaskSnapshot(message.result, "task").taskId);
					} catch (error) {
						this.emitError(error);
					}
				}
			} else {
				this.emitError(new Error(`Received response for unknown MCP request ${String(message.id)}`));
			}
			return;
		}
		this.removePending(message.id, entry);
		if ("error" in message) entry.reject(new McpError(message.error.code, message.error.message, message.error.data));
		else entry.resolve(message.result);
	}

	private handleNotification(method: string, params: unknown): void {
		if (method === "notifications/progress") this.handleProgress(params);
		if (method === TOOLS_LIST_CHANGED || method === "notifications/subscriptions/acknowledged") {
			const subscription = this.toolSubscription;
			const metadata = isObject(params) && isObject(params._meta) ? params._meta : undefined;
			const id = metadata?.[SUBSCRIPTION_ID_META];
			if (!subscription || !isJsonRpcId(id) || id !== subscription.requestId) return;
			if (method === TOOLS_LIST_CHANGED && !subscription.acknowledged) return;
			if (method === "notifications/subscriptions/acknowledged") {
				const notifications = isObject(params) ? params.notifications : undefined;
				const entry = this.pending.get(id);
				if (!entry || !isObject(notifications) || notifications.toolsListChanged !== true) {
					subscription.controller.abort();
					return;
				}
				subscription.acknowledged = true;
				clearTimeout(entry.timer);
				entry.timer = undefined;
			}
		}
		if (
			method === "notifications/cancelled" &&
			isObject(params) &&
			isJsonRpcId(params.requestId) &&
			params.requestId === this.toolSubscription?.requestId
		) {
			this.cancelPending(params.requestId, new McpAbortError(), false);
		}
		for (const listener of this.notificationListeners.get(method) ?? []) {
			try {
				listener(params);
			} catch (error) {
				this.emitError(error);
			}
		}
	}

	private handleProgress(params: unknown): void {
		if (!isObject(params) || !isJsonRpcId(params.progressToken) || typeof params.progress !== "number") return;
		const requestId = this.progressRequests.get(params.progressToken);
		const entry = requestId === undefined ? undefined : this.pending.get(requestId);
		if (requestId === undefined || !entry) return;
		this.armTimeout(requestId, entry);
		try {
			entry.onProgress?.(params as unknown as ProgressNotification);
		} catch (error) {
			this.emitError(error);
		}
	}

	private armTimeout(id: JsonRpcId, entry: PendingRequest): void {
		clearTimeout(entry.timer);
		if (!Number.isFinite(entry.timeoutMs) || entry.timeoutMs <= 0) return;
		entry.timer = setTimeout(() => {
			this.cancelPending(id, new McpTimeoutError(entry.timeoutMs), true, "Request timed out");
		}, entry.timeoutMs);
	}

	private cancelPending(id: JsonRpcId, error: unknown, notifyServer: boolean, reason?: string): void {
		const entry = this.pending.get(id);
		if (!entry) return;
		const transport = this.transport;
		const streamCancellation = transport?.requestCancellation === "stream";
		if (notifyServer && entry.mayCreateTask && !streamCancellation) {
			// Over stdio, the task handle can still arrive after the original request was cancelled.
			this.lateTasks ??= new Map();
			const lateTasks = this.lateTasks;
			const timer = setTimeout(() => lateTasks.delete(id), DEFAULT_REQUEST_TIMEOUT_MS).unref();
			lateTasks.set(id, timer);
		}
		this.removePending(id, entry);
		entry.reject(error);
		if (notifyServer && transport && !streamCancellation) {
			void transport
				.send({
					jsonrpc: "2.0",
					method: "notifications/cancelled",
					params: { requestId: id, ...(reason ? { reason } : {}) },
				})
				.catch((sendError) => this.emitError(sendError));
		}
	}

	private removePending(id: JsonRpcId, entry: PendingRequest): void {
		this.pending.delete(id);
		if (entry.timer) clearTimeout(entry.timer);
		if (entry.progressToken !== undefined) this.progressRequests.delete(entry.progressToken);
		entry.signal?.removeEventListener("abort", entry.onAbort);
		entry.sendController.abort();
	}

	private rejectPending(error: unknown): void {
		for (const [id, entry] of this.pending) {
			this.removePending(id, entry);
			entry.reject(error);
		}
	}

	private handleTransportClose(): void {
		this.markClosed(new McpConnectionClosedError());
	}

	/** Idempotent: rejects pending requests, ends polling, and flips the state. */
	private markClosed(error: Error): void {
		const wasClosed = this.state === "closed";
		this.state = "closed";
		this.tasks?.lifetime.abort(error);
		if (this.lateTasks) {
			for (const timer of this.lateTasks.values()) clearTimeout(timer);
			this.lateTasks.clear();
		}
		this.rejectPending(error);
		if (wasClosed) return;
		for (const listener of [...this.closeListeners]) {
			try {
				listener();
			} catch (listenerError) {
				this.emitError(listenerError);
			}
		}
	}

	private emitError(error: unknown): void {
		const normalized = toError(error);
		for (const listener of this.errorListeners) listener(normalized);
	}

	private disposeTransportListeners(): void {
		for (const dispose of this.disposers.splice(0)) dispose();
	}
}
