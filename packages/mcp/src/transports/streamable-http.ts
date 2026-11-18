import type { AuthProvider, McpFetch } from "../auth-provider.ts";
import {
	isJsonRpcRequest,
	isJsonRpcResponse,
	isObject,
	JSON_RPC_ERROR_CODES,
	type JsonRpcId,
	type JsonRpcMessage,
	McpAbortError,
	McpConnectionClosedError,
	parseJsonRpcMessage,
	toError,
} from "../protocol/jsonrpc.ts";
import { TASKS_PROTOCOL_VERSION } from "../protocol/types.ts";
import {
	DEFAULT_MAX_MESSAGE_BYTES,
	type McpTransport,
	type McpTransportSendOptions,
	TransportEvents,
} from "./transport.ts";

const MAX_ERROR_BODY_BYTES = 8 * 1024;
const ERROR_MESSAGE_BODY_CHARS = 500;

export interface SseEvent {
	event?: string;
	data: string;
	id?: string;
}

export interface ConsumeSseOptions {
	maxEventBytes?: number;
	onEvent(event: SseEvent): void;
}

export async function consumeSseStream(stream: ReadableStream<Uint8Array>, options: ConsumeSseOptions): Promise<void> {
	const reader = stream.getReader();
	const decoder = new TextDecoder();
	let buffered = "";
	let eventName: string | undefined;
	let eventId: string | undefined;
	let dataLines: string[] = [];
	// Bytes of the pending event's data, including the "\n" joins, so events streamed as many
	// short `data:` lines without a terminating blank line cannot grow without bound.
	let dataBytes = 0;
	const maxEventBytes = options.maxEventBytes ?? DEFAULT_MAX_MESSAGE_BYTES;

	const dispatch = () => {
		if (dataLines.length === 0) {
			eventName = undefined;
			eventId = undefined;
			return;
		}
		const data = dataLines.join("\n");
		options.onEvent({ ...(eventName ? { event: eventName } : {}), data, ...(eventId ? { id: eventId } : {}) });
		eventName = undefined;
		eventId = undefined;
		dataLines = [];
		dataBytes = 0;
	};

	const processLine = (line: string) => {
		if (line === "") {
			dispatch();
			return;
		}
		if (line.startsWith(":")) return;
		const colon = line.indexOf(":");
		const field = colon < 0 ? line : line.slice(0, colon);
		let value = colon < 0 ? "" : line.slice(colon + 1);
		if (value.startsWith(" ")) value = value.slice(1);
		if (field === "data") {
			dataBytes += Buffer.byteLength(value) + (dataLines.length > 0 ? 1 : 0);
			if (dataBytes > maxEventBytes) throw new Error(`MCP SSE event exceeds ${maxEventBytes} bytes`);
			dataLines.push(value);
		} else if (field === "event") eventName = value;
		else if (field === "id" && !value.includes("\0")) eventId = value;
	};

	// SSE lines end with CRLF, LF, or a lone CR.
	let skipLeadingLf = false;
	const consumeLines = () => {
		let start = 0;
		for (let i = 0; i < buffered.length; i++) {
			const char = buffered[i];
			if (char !== "\r" && char !== "\n") continue;
			processLine(buffered.slice(start, i));
			if (char === "\r") {
				if (i + 1 < buffered.length) {
					if (buffered[i + 1] === "\n") i++;
				} else {
					skipLeadingLf = true;
				}
			}
			start = i + 1;
		}
		buffered = buffered.slice(start);
	};

	try {
		while (true) {
			const { done, value } = await reader.read();
			if (done) break;
			let text = decoder.decode(value, { stream: true });
			// A CRLF split across chunks: the CR already ended the line, so drop the LF.
			if (skipLeadingLf && text.length > 0) {
				if (text.startsWith("\n")) text = text.slice(1);
				skipLeadingLf = false;
			}
			buffered += text;
			consumeLines();
			if (Buffer.byteLength(buffered) > maxEventBytes)
				throw new Error(`MCP SSE event exceeds ${maxEventBytes} bytes`);
		}
		let tail = decoder.decode();
		if (skipLeadingLf && tail.startsWith("\n")) tail = tail.slice(1);
		buffered += tail;
		consumeLines();
		if (buffered) processLine(buffered);
		dispatch();
	} catch (error) {
		// Releasing the lock alone leaves the response open; the original error wins over a failed cancel.
		await reader.cancel().catch(() => {});
		throw error;
	} finally {
		reader.releaseLock();
	}
}

export interface StreamableHttpTransportOptions {
	url: string | URL;
	headers?: Record<string, string>;
	fetch?: McpFetch;
	maxMessageBytes?: number;
	authProvider?: AuthProvider;
}

export class McpHttpError extends Error {
	readonly status: number;
	readonly body: string;

	constructor(status: number, message: string, body = "") {
		super(message);
		this.name = "McpHttpError";
		this.status = status;
		this.body = body;
	}
}

export class McpAuthRequiredError extends McpHttpError {
	readonly wwwAuthenticate: string | null;

	constructor(response: Response, body = "") {
		super(401, "MCP server requires authentication", body);
		this.name = "McpAuthRequiredError";
		this.wwwAuthenticate = response.headers.get("www-authenticate");
	}
}

function contentType(response: Response): string | undefined {
	return response.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase();
}

/** 401, or 403 with an `insufficient_scope` bearer challenge (step-up authorization). */
function needsAuthorization(response: Response): boolean {
	if (response.status === 401) return true;
	if (response.status !== 403) return false;
	return /(?:^|[\s,])error="?insufficient_scope"?/i.test(response.headers.get("www-authenticate") ?? "");
}

function discard(response: Response): Promise<void> {
	return response.body?.cancel().catch(() => {}) ?? Promise.resolve();
}

/**
 * Reads a response body as text, decoding chunks as they arrive. Once the body exceeds `maxBytes`, the rest is
 * cancelled and the text holds only the first `maxBytes` bytes.
 */
async function readBodyText(response: Response, maxBytes: number): Promise<{ text: string; truncated: boolean }> {
	if (!response.body) return { text: "", truncated: false };
	const reader = response.body.getReader();
	const decoder = new TextDecoder();
	let text = "";
	let remaining = maxBytes;
	try {
		while (true) {
			const { done, value } = await reader.read();
			if (done) return { text: text + decoder.decode(), truncated: false };
			if (value.byteLength > remaining) {
				await reader.cancel().catch(() => {});
				// Decoding a subarray copies only the kept bytes, so the result never references the dropped rest.
				return { text: text + decoder.decode(value.subarray(0, remaining), { stream: true }), truncated: true };
			}
			remaining -= value.byteLength;
			text += decoder.decode(value, { stream: true });
		}
	} finally {
		reader.releaseLock();
	}
}

function describeHttpFailure(status: number, body: string): string {
	const text = body.trim();
	const snippet = text.length > ERROR_MESSAGE_BODY_CHARS ? `${text.slice(0, ERROR_MESSAGE_BODY_CHARS - 3)}...` : text;
	return `MCP HTTP request failed with status ${status}${snippet ? `: ${snippet}` : ""}`;
}

function requestHeaders(message: JsonRpcMessage): Record<string, string> {
	if (!isJsonRpcRequest(message)) return {};
	const headers: Record<string, string> = { "Mcp-Method": message.method };
	if (!isObject(message.params)) return headers;
	const name =
		message.method === "tasks/get" || message.method === "tasks/update" || message.method === "tasks/cancel"
			? message.params.taskId
			: message.method === "resources/read"
				? message.params.uri
				: message.method === "tools/call" || message.method === "prompts/get"
					? message.params.name
					: undefined;
	if (typeof name === "string") {
		const encode = /[^\x20-\x7e\t]/.test(name) || name.trim() !== name || /^=\?base64\?.*\?=$/.test(name);
		headers["Mcp-Name"] = encode ? `=?base64?${Buffer.from(name).toString("base64")}?=` : name;
	}
	return headers;
}

export class StreamableHttpTransport extends TransportEvents implements McpTransport {
	readonly requestCancellation = "stream";
	readonly url: URL;
	readonly options: Readonly<StreamableHttpTransportOptions>;
	private fetch: McpFetch;
	private controller = new AbortController();
	private started = false;
	private closed = false;

	constructor(options: StreamableHttpTransportOptions) {
		super();
		this.options = Object.freeze({ ...options, headers: options.headers ? { ...options.headers } : undefined });
		this.url = new URL(options.url);
		const fetch = options.fetch ?? globalThis.fetch;
		// Call fetch without a receiver. `this.fetch(...)` and `context.fetch(...)` would pass the transport or
		// the auth context as `this`, which Cloudflare Workers reject for the platform fetch ("Illegal invocation").
		this.fetch = (input, init) => fetch(input, init);
	}

	async start(): Promise<void> {
		if (this.started) throw new Error("MCP Streamable HTTP transport already started");
		if (this.closed) throw new McpConnectionClosedError();
		this.started = true;
	}

	async send(message: JsonRpcMessage, { signal }: McpTransportSendOptions = {}): Promise<void> {
		if (!this.started || this.closed) throw new McpConnectionClosedError();
		const response = await this.authorizedFetch(
			{
				headers: {
					accept: "application/json, text/event-stream",
					"content-type": "application/json",
					...requestHeaders(message),
				},
				body: JSON.stringify(message),
			},
			signal,
		);
		if (!response.ok) {
			const body = (await readBodyText(response, MAX_ERROR_BODY_BYTES).catch(() => ({ text: "" }))).text;
			if (response.status === 401) throw new McpAuthRequiredError(response, body);
			if (isJsonRpcRequest(message) && contentType(response) === "application/json") {
				let reply: unknown;
				try {
					reply = JSON.parse(body);
				} catch {
					// Non-MCP HTTP errors keep the existing error type.
				}
				if (isJsonRpcResponse(reply) && reply.id === message.id && "error" in reply) {
					this.emitMessage(reply);
					return;
				}
			}
			throw new McpHttpError(response.status, describeHttpFailure(response.status, body), body);
		}

		if (!isJsonRpcRequest(message)) {
			// Notifications are acknowledged with 202 and carry no reply.
			await discard(response);
			return;
		}
		if (response.status === 202 || response.status === 204) {
			throw new McpHttpError(response.status, `MCP server accepted request ${message.method} without a response`);
		}
		const type = contentType(response);
		if (type === "application/json") {
			const maxBytes = this.options.maxMessageBytes ?? DEFAULT_MAX_MESSAGE_BYTES;
			const { text, truncated } = await readBodyText(response, maxBytes);
			if (truncated) throw new Error(`MCP JSON response exceeds ${maxBytes} bytes`);
			if (!signal?.aborted) this.emitMessage(parseJsonRpcMessage(JSON.parse(text)));
			return;
		}
		if (type === "text/event-stream" && response.body) {
			void this.consumeResponseStream(response.body, message.id, signal);
			return;
		}
		await discard(response);
		throw new McpHttpError(response.status, `Unsupported MCP response content type: ${type ?? "missing"}`);
	}

	async close(): Promise<void> {
		if (this.closed) return;
		this.closed = true;
		this.controller.abort();
		this.emitClose();
	}

	/**
	 * Fetch with auth headers. A 401 (or a 403 asking for more scope) is handed to the auth provider
	 * once, and the request is retried with whatever credentials it left behind.
	 *
	 * The request signal cancels dispatch or its response stream without aborting shared
	 * authorization recovery, which other requests may wait for.
	 */
	private async authorizedFetch(
		init: { headers: Record<string, string>; body?: string },
		signal?: AbortSignal,
	): Promise<Response> {
		const onUnauthorized = this.options.authProvider?.onUnauthorized?.bind(this.options.authProvider);
		const responseSignal = signal ? AbortSignal.any([this.controller.signal, signal]) : this.controller.signal;
		for (let attempt = 0; ; attempt++) {
			const token = await this.options.authProvider?.token();
			if (responseSignal.aborted) throw new McpAbortError();
			const headers = new Headers(this.options.headers);
			headers.delete("Mcp-Session-Id");
			headers.delete("Last-Event-ID");
			headers.delete("Mcp-Method");
			headers.delete("Mcp-Name");
			for (const [name, value] of Object.entries(init.headers)) headers.set(name, value);
			headers.set("MCP-Protocol-Version", TASKS_PROTOCOL_VERSION);
			if (token) headers.set("Authorization", `Bearer ${token}`);
			const response = await this.fetch(this.url, {
				method: "POST",
				headers,
				body: init.body,
				signal: responseSignal,
			});
			if (attempt > 0 || !onUnauthorized || !needsAuthorization(response)) return response;
			try {
				await onUnauthorized({ response, serverUrl: this.url, fetch: this.fetch, token });
			} finally {
				await discard(response);
			}
		}
	}

	private async consumeResponseStream(
		body: ReadableStream<Uint8Array>,
		requestId: JsonRpcId,
		signal?: AbortSignal,
	): Promise<void> {
		let answered = false;
		let failure: unknown;
		try {
			await consumeSseStream(body, {
				maxEventBytes: this.options.maxMessageBytes ?? DEFAULT_MAX_MESSAGE_BYTES,
				onEvent: (event) => {
					if (answered || signal?.aborted || (event.event !== undefined && event.event !== "message")) return;
					if (!event.data.trim()) return;
					let message: JsonRpcMessage;
					try {
						message = parseJsonRpcMessage(JSON.parse(event.data));
					} catch (error) {
						this.emitError(error);
						return;
					}
					if (isJsonRpcResponse(message) && message.id === requestId) answered = true;
					this.emitMessage(message);
				},
			});
		} catch (error) {
			failure = error;
		}
		if (answered || this.closed || signal?.aborted) return;
		const reason = failure === undefined ? "stream ended without a response" : toError(failure).message;
		this.emitMessage({
			jsonrpc: "2.0",
			id: requestId,
			error: { code: JSON_RPC_ERROR_CODES.internalError, message: `MCP response stream failed: ${reason}` },
		});
	}
}
