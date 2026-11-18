import { afterEach, describe, expect, it, vi } from "vitest";
import {
	type JsonRpcMessage,
	type JsonRpcRequest,
	McpAbortError,
	McpClient,
	type McpClientOptions,
	McpConnectionClosedError,
	McpError,
	McpTimeoutError,
	type ServerCapabilities,
	TASKS_PROTOCOL_VERSION,
} from "../src/index.ts";
import { createInMemoryTransportPair, type InMemoryTransport } from "../src/testing/index.ts";

const tasksExtension = "io.modelcontextprotocol/tasks";
const metadata = {
	"io.modelcontextprotocol/protocolVersion": TASKS_PROTOCOL_VERSION,
	"io.modelcontextprotocol/clientInfo": { name: "test-client", version: "2.0.0" },
};
const tasksCapabilities = { extensions: { [tasksExtension]: {} } };
const serverInfo = { name: "test-server", version: "1.0.0" };

interface TestServer {
	transport: InMemoryTransport;
	messages: JsonRpcMessage[];
	setHandler(method: string, handler: (request: JsonRpcRequest) => unknown | Promise<unknown>): void;
}

async function createServer(
	capabilities: ServerCapabilities = { tools: {}, ...tasksCapabilities },
): Promise<{ clientTransport: InMemoryTransport; server: TestServer }> {
	const pair = createInMemoryTransportPair();
	const handlers = new Map<string, (request: JsonRpcRequest) => unknown | Promise<unknown>>();
	const messages: JsonRpcMessage[] = [];
	pair.server.onMessage((message) => {
		messages.push(message);
		if (!("id" in message) || !("method" in message)) return;
		const request = message as JsonRpcRequest;
		const handler = handlers.get(request.method);
		queueMicrotask(async () => {
			let reply: JsonRpcMessage;
			try {
				if (!handler) throw new McpError(-32601, `Method not found: ${request.method}`);
				reply = { jsonrpc: "2.0", id: request.id, result: await handler(request) };
			} catch (error) {
				const mcpError = error instanceof McpError ? error : new McpError(-32603, String(error));
				reply = {
					jsonrpc: "2.0",
					id: request.id,
					error: { code: mcpError.code, message: mcpError.message, data: mcpError.data },
				};
			}
			await pair.server.send(reply).catch((error) => {
				// The client can close immediately after it has sent a cooperative cancellation.
				if (!(error instanceof McpConnectionClosedError)) throw error;
			});
		});
	});
	await pair.server.start();
	const server: TestServer = {
		transport: pair.server,
		messages,
		setHandler(method, handler) {
			handlers.set(method, handler);
		},
	};
	server.setHandler("server/discover", () => ({
		resultType: "complete",
		supportedVersions: [TASKS_PROTOCOL_VERSION],
		capabilities,
		_meta: { "io.modelcontextprotocol/serverInfo": serverInfo },
		instructions: "Use test tools.",
	}));
	return { clientTransport: pair.client, server };
}

async function connect(options?: Partial<McpClientOptions>, capabilities?: ServerCapabilities) {
	const { clientTransport, server } = await createServer(capabilities);
	const client = new McpClient({ name: "test-client", version: "2.0.0", ...options });
	await client.connect(clientTransport);
	return { client, server };
}

describe("MCP Tasks", () => {
	it("polls two task handles independently and preserves both final results and progress", async () => {
		const { client, server } = await connect();
		try {
			const started = new Set<string>();
			server.setHandler("tools/call", ({ params }) => {
				const { name, _meta } = params as { name: string; _meta: unknown };
				expect(_meta).toMatchObject({
					...metadata,
					"io.modelcontextprotocol/clientCapabilities": tasksCapabilities,
				});
				started.add(name);
				return { resultType: "task", taskId: name, status: "working", statusMessage: "queued", pollIntervalMs: 1 };
			});
			const polled = new Set<string>();
			server.setHandler("tasks/get", ({ params }) => {
				const { taskId, _meta } = params as { taskId: string; _meta: unknown };
				expect(started).toEqual(new Set(["first", "second"]));
				expect(_meta).toEqual({ ...metadata, "io.modelcontextprotocol/clientCapabilities": tasksCapabilities });
				const common = { resultType: "complete", taskId, pollIntervalMs: 1 };
				if (!polled.has(taskId)) {
					polled.add(taskId);
					return { ...common, status: "working", statusMessage: "running" };
				}
				return {
					...common,
					status: "completed",
					result: { content: [{ type: "text", text: taskId }], isError: taskId === "second" },
				};
			});
			const firstProgress = vi.fn();
			const secondProgress = vi.fn();
			const result = await Promise.all([
				client.callTool("first", {}, { onProgress: firstProgress }),
				client.callTool("second", {}, { onProgress: secondProgress }),
			]);
			expect(result).toEqual([
				{ content: [{ type: "text", text: "first" }], isError: false },
				{ content: [{ type: "text", text: "second" }], isError: true },
			]);
			expect(firstProgress.mock.calls.map(([event]) => event.message)).toEqual([
				"Task first: queued",
				"Task first: running",
			]);
			expect(secondProgress.mock.calls.map(([event]) => event.message)).toEqual([
				"Task second: queued",
				"Task second: running",
			]);
		} finally {
			await client.close();
		}
	});

	it("uses stateless requests without Tasks when the server does not advertise it", async () => {
		const { client, server } = await connect(undefined, { tools: {} });
		try {
			server.setHandler("tools/call", ({ params }) => {
				expect(params).toMatchObject({
					_meta: { ...metadata, "io.modelcontextprotocol/clientCapabilities": { extensions: {} } },
				});
				return { resultType: "complete", structuredContent: 3 };
			});
			expect((await client.callTool("immediate")).structuredContent).toBe(3);
			server.setHandler("tools/call", () => ({ resultType: "task", taskId: "unexpected", status: "working" }));
			await expect(client.callTool("unexpected")).rejects.toThrow("Unexpected MCP tools/call resultType: task");
		} finally {
			await client.close();
		}
	});

	it("answers roots through task input exactly once, not a standalone request", async () => {
		const root = { uri: "file:///workspace", name: "workspace" };
		const { client, server } = await connect({ roots: [root] });
		try {
			const standaloneId = "standalone-roots";
			await server.transport.send({ jsonrpc: "2.0", id: standaloneId, method: "roots/list" });
			server.setHandler("tools/call", () => ({
				resultType: "task",
				taskId: "needs-roots",
				status: "input_required",
				pollIntervalMs: 1,
			}));
			let polls = 0;
			server.setHandler("tasks/get", () => {
				const common = { resultType: "complete", taskId: "needs-roots", pollIntervalMs: 1 };
				return ++polls <= 2
					? { ...common, status: "input_required", inputRequests: { roots: { method: "roots/list" } } }
					: { ...common, status: "completed", result: { content: [{ type: "text", text: "used the root" }] } };
			});
			const updates = vi.fn(({ params }: JsonRpcRequest) => {
				expect(params).toMatchObject({ taskId: "needs-roots", inputResponses: { roots: { roots: [root] } } });
				return { resultType: "complete" };
			});
			server.setHandler("tasks/update", updates);
			expect(await client.callTool("roots")).toEqual({ content: [{ type: "text", text: "used the root" }] });
			expect(updates).toHaveBeenCalledTimes(1);
			expect(server.messages.some((message) => "id" in message && message.id === standaloneId)).toBe(false);
		} finally {
			await client.close();
		}
	});

	it("cancels the task by ID when aborted during the delay between polls", async () => {
		const { client, server } = await connect();
		try {
			server.setHandler("tools/call", () => ({
				resultType: "task",
				taskId: "slow",
				status: "working",
				pollIntervalMs: 60_000,
			}));
			const cancellation = Promise.withResolvers<unknown>();
			server.setHandler("tasks/cancel", ({ params }) => {
				cancellation.resolve(params);
				return { resultType: "complete" };
			});
			const controller = new AbortController();
			const running = Promise.withResolvers<void>();
			const call = client.callTool("slow", {}, { signal: controller.signal, onProgress: () => running.resolve() });
			await running.promise;
			controller.abort();
			await expect(call).rejects.toBeInstanceOf(McpAbortError);
			expect(await cancellation.promise).toMatchObject({ taskId: "slow" });
			expect(server.messages.filter((message) => "method" in message).map((message) => message.method)).toEqual([
				"server/discover",
				"tools/call",
				"tasks/cancel",
			]);
		} finally {
			await client.close();
		}
	});

	it("cancels a handle that arrives after its originating request was aborted", async () => {
		const { client, server } = await connect();
		try {
			const started = Promise.withResolvers<void>();
			const handle = Promise.withResolvers<unknown>();
			server.setHandler("tools/call", () => {
				started.resolve();
				return handle.promise;
			});
			const cancelled = Promise.withResolvers<unknown>();
			server.setHandler("tasks/cancel", ({ params }) => {
				cancelled.resolve(params);
				return { resultType: "complete" };
			});
			const controller = new AbortController();
			const call = client.callTool("late", {}, { signal: controller.signal });
			await started.promise;
			controller.abort();
			await expect(call).rejects.toBeInstanceOf(McpAbortError);
			handle.resolve({ resultType: "task", taskId: "late", status: "working" });
			expect(await cancelled.promise).toMatchObject({ taskId: "late" });
		} finally {
			await client.close();
		}
	});

	it("surfaces task errors and rejects when a connection closes while waiting to poll", async () => {
		const { client, server } = await connect();
		try {
			server.setHandler("tools/call", () => ({ resultType: "task", taskId: "fail", status: "failed" }));
			server.setHandler("tasks/get", () => ({
				resultType: "complete",
				status: "failed",
				taskId: "fail",
				error: { code: -32603, message: "internal task error", data: { reason: "backend" } },
			}));
			await expect(client.callTool("fail")).rejects.toMatchObject({
				code: -32603,
				message: "internal task error",
				data: { reason: "backend" },
			});
			server.setHandler("tools/call", () => ({
				resultType: "task",
				taskId: "slow",
				status: "working",
				pollIntervalMs: 60_000,
			}));
			const running = Promise.withResolvers<void>();
			const call = client.callTool("slow", {}, { onProgress: () => running.resolve() });
			await running.promise;
			const rejects = expect(call).rejects.toBeInstanceOf(McpConnectionClosedError);
			await client.close();
			await rejects;
		} finally {
			await client.close();
		}
	});
});

afterEach(() => {
	vi.useRealTimers();
});

describe("McpClient", () => {
	it("discovers the only supported protocol before exposing server information", async () => {
		const { client, server } = await connect();
		expect(client.connectionState).toBe("connected");
		expect(client.protocolVersion).toBe(TASKS_PROTOCOL_VERSION);
		expect(client.serverInfo).toEqual(serverInfo);
		expect(client.serverCapabilities).toEqual({ tools: {}, ...tasksCapabilities });
		expect(client.instructions).toBe("Use test tools.");
		expect(server.messages).toEqual([
			{
				jsonrpc: "2.0",
				id: 1,
				method: "server/discover",
				params: {
					_meta: { ...metadata, "io.modelcontextprotocol/clientCapabilities": { extensions: {} } },
				},
			},
		]);
		await client.close();
	});

	it("paginates tools and preserves protocol tool definitions", async () => {
		const { client, server } = await connect();
		server.setHandler("tools/list", (request) => {
			const cursor = (request.params as { cursor?: string } | undefined)?.cursor;
			return cursor === undefined
				? {
						tools: [{ name: "search", description: "Search", inputSchema: { type: "object" } }],
						nextCursor: "page-2",
					}
				: {
						tools: [
							{
								name: "read",
								inputSchema: { type: "object" },
								outputSchema: { type: "object" },
								annotations: { readOnlyHint: true },
							},
						],
						// Some servers end pagination with an empty cursor instead of omitting it.
						nextCursor: "",
					};
		});
		expect(await client.listTools()).toEqual([
			{ name: "search", description: "Search", inputSchema: { type: "object" } },
			{
				name: "read",
				inputSchema: { type: "object" },
				outputSchema: { type: "object" },
				annotations: { readOnlyHint: true },
			},
		]);
		await client.close();
	});

	it("lists and reads resources", async () => {
		const { client, server } = await connect();
		server.setHandler("resources/list", (request) =>
			(request.params as { cursor?: string } | undefined)?.cursor === undefined
				? { resources: [{ uri: "file:///a", name: "a", mimeType: "text/plain" }], nextCursor: "2" }
				: { resources: [{ uri: "file:///b" }] },
		);
		server.setHandler("resources/templates/list", () => ({
			resourceTemplates: [{ uriTemplate: "repo://{owner}/{repo}", name: "repo" }],
		}));
		server.setHandler("resources/read", (request) => ({
			contents: [{ uri: (request.params as { uri: string }).uri, text: "hello" }],
		}));
		// A missing name falls back to the URI.
		expect(await client.listResources()).toEqual([
			{ uri: "file:///a", name: "a", mimeType: "text/plain" },
			{ uri: "file:///b", name: "file:///b" },
		]);
		expect(await client.listResourceTemplates()).toEqual([{ uriTemplate: "repo://{owner}/{repo}", name: "repo" }]);
		// Single pages pass the cursor through.
		expect(await client.listResourcesPage()).toEqual({
			resources: [{ uri: "file:///a", name: "a", mimeType: "text/plain" }],
			nextCursor: "2",
		});
		expect(await client.listResourcesPage("2")).toEqual({ resources: [{ uri: "file:///b", name: "file:///b" }] });
		expect(await client.readResource("file:///a")).toEqual({ contents: [{ uri: "file:///a", text: "hello" }] });

		server.setHandler("resources/read", () => ({ contents: [{ uri: "file:///a" }] }));
		await expect(client.readResource("file:///a")).rejects.toThrow("Invalid contents in MCP resources/read result");
		server.setHandler("resources/list", () => ({ resources: [{ name: "no uri" }] }));
		await expect(client.listResources()).rejects.toThrow("Invalid entry in MCP resources/list result");
		await client.close();
	});

	it("returns structured tool content and surfaces JSON-RPC errors", async () => {
		const { client, server } = await connect();
		server.setHandler("tools/call", (request) => {
			const params = request.params as { name: string; arguments?: Record<string, unknown> };
			if (params.name === "fail") throw new McpError(1234, "tool failed", { retryable: false });
			return {
				resultType: "complete",
				content: [{ type: "text", text: "ok" }],
				structuredContent: { count: params.arguments?.count },
			};
		});
		expect(await client.callTool("count", { count: 3 })).toEqual({
			resultType: "complete",
			content: [{ type: "text", text: "ok" }],
			structuredContent: { count: 3 },
		});
		await expect(client.callTool("fail")).rejects.toMatchObject({
			name: "McpError",
			code: 1234,
			message: "tool failed",
			data: { retryable: false },
		});
		await client.close();
	});

	it("renews the timeout on progress", async () => {
		vi.useFakeTimers();
		const { client, server } = await connect();
		server.setHandler("tools/call", async (request) => {
			const token = ((request.params as Record<string, unknown>)._meta as Record<string, unknown>)
				.progressToken as number;
			setTimeout(() => {
				void server.transport.send({
					jsonrpc: "2.0",
					method: "notifications/progress",
					params: { progressToken: token, progress: 1, total: 2 },
				});
			}, 40);
			await new Promise((resolve) => setTimeout(resolve, 80));
			return { resultType: "complete", content: [{ type: "text", text: "done" }] };
		});
		const progress = vi.fn();
		const result = client.callTool("slow", {}, { timeoutMs: 50, onProgress: progress });
		await vi.advanceTimersByTimeAsync(40);
		await vi.advanceTimersByTimeAsync(40);
		expect(await result).toEqual({ resultType: "complete", content: [{ type: "text", text: "done" }] });
		expect(progress).toHaveBeenCalledWith({ progressToken: 2, progress: 1, total: 2 });
		await client.close();
	});

	it("cancels aborted and timed-out requests", async () => {
		const { client, server } = await connect(undefined, { tools: {} });
		server.setHandler("tools/call", () => new Promise(() => {}));
		const controller = new AbortController();
		const aborted = client.callTool("wait", {}, { signal: controller.signal });
		controller.abort("stop");
		await expect(aborted).rejects.toBeInstanceOf(McpAbortError);
		await new Promise((resolve) => setTimeout(resolve, 0));
		expect(server.messages).toContainEqual({
			jsonrpc: "2.0",
			method: "notifications/cancelled",
			params: { requestId: 2, reason: "stop" },
		});

		await expect(client.callTool("wait", {}, { timeoutMs: 5 })).rejects.toBeInstanceOf(McpTimeoutError);
		await client.close();
	});

	it("reports transport errors without failing pending requests", async () => {
		const { clientTransport, server } = await createServer();
		const client = new McpClient({ name: "test-client", version: "1.0.0" });
		await client.connect(clientTransport);
		const errors: Error[] = [];
		client.onError((error) => errors.push(error));
		let respond = () => {};
		server.setHandler(
			"tools/call",
			() =>
				new Promise((resolve) => {
					respond = () => resolve({ resultType: "complete", content: [] });
				}),
		);
		const call = client.callTool("wait");
		await new Promise((resolve) => setTimeout(resolve, 0));
		clientTransport.emitError(new Error("stray log line"));
		respond();
		expect(await call).toEqual({ resultType: "complete", content: [] });
		expect(errors.map((error) => error.message)).toEqual(["stray log line"]);
		await client.close();
	});

	it.each([
		{
			reason: "discover not implemented",
			discover: () => {
				throw new McpError(-32601, "Method not found");
			},
			error: "Method not found",
		},
		{
			reason: "older protocol only",
			discover: () => ({ resultType: "complete", supportedVersions: ["2025-06-18"], capabilities: {} }),
			error: TASKS_PROTOCOL_VERSION,
		},
	])("does not fall back when discovery reports $reason", async ({ discover, error }) => {
		const { clientTransport, server } = await createServer();
		server.setHandler("server/discover", discover);
		const client = new McpClient({ name: "test-client", version: "1.0.0" });
		await expect(client.connect(clientTransport)).rejects.toThrow(error);
		expect(client.connectionState).toBe("closed");
		expect(server.messages.map((message) => ("method" in message ? message.method : undefined))).toEqual([
			"server/discover",
		]);
	});

	it("defaults missing tool result content to an empty list", async () => {
		const { client, server } = await connect();
		server.setHandler("tools/call", () => ({ resultType: "complete", structuredContent: { ok: true } }));
		expect(await client.callTool("structured")).toEqual({
			resultType: "complete",
			content: [],
			structuredContent: { ok: true },
		});
		server.setHandler("tools/call", () => ({ resultType: "complete", content: "not a list" }));
		await expect(client.callTool("broken")).rejects.toThrow("Invalid MCP tools/call result");
		await client.close();
	});

	// #10416: MCP 2026-07-28 allows any JSON value as structured content.
	it("accepts structured content that is not an object", async () => {
		const { client, server } = await connect();
		for (const structuredContent of [[{ id: 1 }, { id: 2 }], null, "text", 3, false]) {
			server.setHandler("tools/call", () => ({ resultType: "complete", content: [], structuredContent }));
			expect(await client.callTool("structured")).toEqual({
				resultType: "complete",
				content: [],
				structuredContent,
			});
		}
		await client.close();
	});

	it("closes the connection when discovery times out", async () => {
		const { clientTransport, server } = await createServer();
		server.setHandler("server/discover", () => new Promise(() => {}));
		const client = new McpClient({ name: "test-client", version: "1.0.0", requestTimeoutMs: 5 });
		await expect(client.connect(clientTransport)).rejects.toBeInstanceOf(McpTimeoutError);
		expect(client.connectionState).toBe("closed");
	});

	it("notifies close listeners once when the transport drops", async () => {
		const { client, server } = await connect();
		const closed = vi.fn();
		client.onClose(closed);
		server.setHandler("tools/call", () => new Promise(() => {}));
		const pending = client.callTool("wait");
		await new Promise((resolve) => setTimeout(resolve, 0));
		await server.transport.close();
		await expect(pending).rejects.toThrow("MCP connection closed");
		expect(client.connectionState).toBe("closed");
		await client.close();
		expect(closed).toHaveBeenCalledTimes(1);
	});

	it("only delivers tool list changes for an acknowledged subscription", async () => {
		const { clientTransport, server } = await createServer({ tools: { listChanged: true } });
		const request = Promise.withResolvers<JsonRpcRequest>();
		server.setHandler("subscriptions/listen", (message) => {
			request.resolve(message);
			return new Promise(() => {});
		});
		const client = new McpClient({ name: "test-client", version: "2.0.0" });
		const changed = vi.fn();
		const alsoChanged = vi.fn();
		client.onNotification("notifications/tools/list_changed", changed);
		await client.connect(clientTransport);
		client.onNotification("notifications/tools/list_changed", alsoChanged);
		try {
			const { id, params } = await request.promise;
			expect(params).toEqual({
				notifications: { toolsListChanged: true },
				_meta: { ...metadata, "io.modelcontextprotocol/clientCapabilities": { extensions: {} } },
			});
			const sendChange = (subscriptionId: JsonRpcRequest["id"]) =>
				server.transport.send({
					jsonrpc: "2.0",
					method: "notifications/tools/list_changed",
					params: { _meta: { "io.modelcontextprotocol/subscriptionId": subscriptionId } },
				});
			await sendChange(id);
			await server.transport.send({
				jsonrpc: "2.0",
				method: "notifications/subscriptions/acknowledged",
				params: {
					_meta: { "io.modelcontextprotocol/subscriptionId": id },
					notifications: { toolsListChanged: true },
				},
			});
			await server.transport.send({ jsonrpc: "2.0", method: "notifications/tools/list_changed" });
			await sendChange("wrong-subscription");
			await sendChange(id);
			const delivered = { _meta: { "io.modelcontextprotocol/subscriptionId": id } };
			expect(changed.mock.calls).toEqual([[delivered]]);
			expect(alsoChanged.mock.calls).toEqual([[delivered]]);
			expect(server.messages.map((message) => ("method" in message ? message.method : undefined))).toEqual([
				"server/discover",
				"subscriptions/listen",
			]);
		} finally {
			await client.close();
		}
	});

	it("does not subscribe when the server does not advertise tool list changes", async () => {
		const { client, server } = await connect();
		try {
			const changed = vi.fn();
			client.onNotification("notifications/tools/list_changed", changed);
			await server.transport.send({ jsonrpc: "2.0", method: "notifications/tools/list_changed" });
			expect(changed).not.toHaveBeenCalled();
			expect(server.messages.map((message) => ("method" in message ? message.method : undefined))).toEqual([
				"server/discover",
			]);
		} finally {
			await client.close();
		}
	});
});
