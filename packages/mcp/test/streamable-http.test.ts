import type { IncomingMessage, ServerResponse } from "node:http";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
	type McpAuthRequiredError,
	McpClient,
	type McpFetch,
	McpHttpError,
	StreamableHttpTransport,
	TASKS_PROTOCOL_VERSION,
} from "../src/index.ts";
import { consumeSseStream, type SseEvent } from "../src/transports/streamable-http.ts";
import { closeServers, listen, readBody } from "./helpers.ts";

interface RecordedRequest {
	method: string;
	headers: IncomingMessage["headers"];
	message?: Record<string, unknown>;
}

async function startServer(
	handler: (
		message: Record<string, unknown>,
		response: ServerResponse,
		request: IncomingMessage,
	) => void | Promise<void>,
): Promise<{ url: string; requests: RecordedRequest[] }> {
	const requests: RecordedRequest[] = [];
	const origin = await listen(async (request, response) => {
		if (request.method !== "POST") {
			requests.push({ method: request.method ?? "", headers: request.headers });
			response.writeHead(405).end();
			return;
		}
		const message = JSON.parse(await readBody(request)) as Record<string, unknown>;
		requests.push({ method: request.method, headers: request.headers, message });
		await handler(message, response, request);
	});
	return { url: `${origin}/mcp`, requests };
}

function protocolHandler(message: Record<string, unknown>, response: ServerResponse): void {
	if (!("id" in message)) {
		response.statusCode = 202;
		response.end();
		return;
	}
	if (message.method === "server/discover") {
		response.writeHead(200, {
			"content-type": "application/json",
			"mcp-session-id": "session-1",
		});
		response.end(
			JSON.stringify({
				jsonrpc: "2.0",
				id: message.id,
				result: {
					resultType: "complete",
					supportedVersions: [TASKS_PROTOCOL_VERSION],
					capabilities: { tools: {} },
					_meta: { "io.modelcontextprotocol/serverInfo": { name: "http-fixture", version: "1.0.0" } },
				},
			}),
		);
		return;
	}
	if (message.method === "tools/list") {
		response.setHeader("content-type", "application/json");
		response.end(
			JSON.stringify({
				jsonrpc: "2.0",
				id: message.id,
				result: { tools: [{ name: "echo", inputSchema: { type: "object" } }] },
			}),
		);
		return;
	}
	if (message.method === "tools/call") {
		const result = { resultType: "complete", content: [{ type: "text", text: "hello" }] };
		response.writeHead(200, { "content-type": "text/event-stream" });
		response.end(`id: tool-result\ndata: ${JSON.stringify({ jsonrpc: "2.0", id: message.id, result })}\n\n`);
		return;
	}
	response.writeHead(400).end("Unknown MCP method");
}

afterEach(closeServers);

describe("consumeSseStream", () => {
	it("parses chunked CRLF events, comments, IDs, and multiline data", async () => {
		const encoder = new TextEncoder();
		const chunks = [': keepalive\r\nid: 7\r\ndata: {"one":\r\n', "data: 1}\r\n\r\n"];
		const stream = new ReadableStream<Uint8Array>({
			start(controller) {
				for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
				controller.close();
			},
		});
		const events: SseEvent[] = [];
		await consumeSseStream(stream, { onEvent: (event) => events.push(event) });
		expect(events).toEqual([{ id: "7", data: '{"one":\n1}' }]);
	});

	it("recognizes CR, LF, and CRLF line endings, including CRLF split across chunks", async () => {
		const encoder = new TextEncoder();
		const parse = async (chunks: string[]) => {
			const stream = new ReadableStream<Uint8Array>({
				start(controller) {
					for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
					controller.close();
				},
			});
			const events: SseEvent[] = [];
			await consumeSseStream(stream, { onEvent: (event) => events.push(event) });
			return events;
		};
		const expected = [{ id: "1", data: "a\nb" }, { data: "c" }];
		for (const separator of ["\n", "\r\n", "\r"]) {
			const text = ["id: 1", "data: a", "data: b", "", "data: c", "", ""].join(separator);
			expect(await parse([text])).toEqual(expected);
			// One character per chunk splits every CRLF between chunks.
			expect(await parse([...text])).toEqual(expected);
		}
		expect(await parse(["id: 1\rdata: a\r\ndata: b\n\r", "\ndata: c\r\r"])).toEqual(expected);
	});

	it("dispatches CR-terminated events while the stream stays open", async () => {
		const encoder = new TextEncoder();
		let controller!: ReadableStreamDefaultController<Uint8Array>;
		const stream = new ReadableStream<Uint8Array>({
			start(c) {
				controller = c;
			},
		});
		const received = new Promise<SseEvent>((resolve) => {
			void consumeSseStream(stream, { onEvent: resolve }).catch(() => {});
		});
		controller.enqueue(encoder.encode("data: first\r\r"));
		expect(await received).toEqual({ data: "first" });
		controller.close();
	});

	it("rejects events whose data lines exceed the limit without a blank line", async () => {
		const encoder = new TextEncoder();
		let sent = 0;
		const stream = new ReadableStream<Uint8Array>({
			pull(controller) {
				// Never sends a blank line, so the event is never dispatched.
				if (sent++ > 1000) controller.close();
				else controller.enqueue(encoder.encode("data: xxxxxxxxxxxxxxxx\n"));
			},
		});
		await expect(consumeSseStream(stream, { maxEventBytes: 256, onEvent: () => {} })).rejects.toThrow(
			"MCP SSE event exceeds 256 bytes",
		);
		expect(sent).toBeLessThan(100);
	});

	// Regression test for #11729: a failed stream must be cancelled, not left open.
	it("cancels the stream when an event exceeds the limit", async () => {
		const encoder = new TextEncoder();
		let cancelled = false;
		const stream = new ReadableStream<Uint8Array>({
			start(controller) {
				// The server keeps the response open after the oversized event.
				controller.enqueue(encoder.encode(`data: ${"x".repeat(512)}\n\n`));
			},
			cancel() {
				cancelled = true;
			},
		});
		await expect(consumeSseStream(stream, { maxEventBytes: 128, onEvent: () => {} })).rejects.toThrow(
			"MCP SSE event exceeds 128 bytes",
		);
		expect(cancelled).toBe(true);
	});
});

describe("StreamableHttpTransport", () => {
	it("uses only stateless POSTs for JSON and SSE responses, ignoring session headers", async () => {
		const { url, requests } = await startServer(protocolHandler);
		const token = vi.fn(async () => "token");
		const transport = new StreamableHttpTransport({
			url,
			headers: { "Mcp-Session-Id": "stale-session", "Last-Event-ID": "stale-event" },
			authProvider: { token },
		});
		const client = new McpClient({ name: "http-test", version: "1.0.0" });
		await client.connect(transport);
		expect(await client.listTools()).toEqual([{ name: "echo", inputSchema: { type: "object" } }]);
		expect(await client.callTool("echo", { text: "hello" })).toEqual({
			resultType: "complete",
			content: [{ type: "text", text: "hello" }],
		});
		await client.close();

		expect(token).toHaveBeenCalledTimes(3);
		expect(
			requests.map(({ method, headers, message }) => [
				method,
				headers["mcp-protocol-version"],
				headers["mcp-method"],
				headers["mcp-name"],
				headers["mcp-session-id"],
				headers["last-event-id"],
				message?.method,
			]),
		).toEqual([
			["POST", TASKS_PROTOCOL_VERSION, "server/discover", undefined, undefined, undefined, "server/discover"],
			["POST", TASKS_PROTOCOL_VERSION, "tools/list", undefined, undefined, undefined, "tools/list"],
			["POST", TASKS_PROTOCOL_VERSION, "tools/call", "echo", undefined, undefined, "tools/call"],
		]);
	});

	it("routes Tasks advertised by discovery by method and task ID", async () => {
		const taskId = "=?base64?literal?=";
		const { url, requests } = await startServer((message, response) => {
			const result =
				message.method === "server/discover"
					? {
							resultType: "complete",
							supportedVersions: [TASKS_PROTOCOL_VERSION],
							capabilities: { tools: {}, extensions: { "io.modelcontextprotocol/tasks": {} } },
						}
					: message.method === "tools/call"
						? { resultType: "task", taskId, status: "working", pollIntervalMs: 1 }
						: { resultType: "complete", taskId, status: "completed", result: { structuredContent: "done" } };
			const reply = JSON.stringify({ jsonrpc: "2.0", id: message.id, result });
			if (message.method === "tasks/get") {
				response.writeHead(200, { "content-type": "text/event-stream" });
				response.end(`data: ${reply}\n\n`);
			} else {
				response.writeHead(200, { "content-type": "application/json" });
				response.end(reply);
			}
		});
		const client = new McpClient({ name: "http-test", version: "1.0.0" });
		try {
			await client.connect(new StreamableHttpTransport({ url }));
			expect(await client.callTool("hello world")).toEqual({ content: [], structuredContent: "done" });
		} finally {
			await client.close();
		}

		expect(
			requests.map(({ method, headers }) => [
				method,
				headers["mcp-protocol-version"],
				headers["mcp-method"],
				headers["mcp-name"],
				headers["mcp-session-id"],
			]),
		).toEqual([
			["POST", TASKS_PROTOCOL_VERSION, "server/discover", undefined, undefined],
			["POST", TASKS_PROTOCOL_VERSION, "tools/call", "hello world", undefined],
			[
				"POST",
				TASKS_PROTOCOL_VERSION,
				"tasks/get",
				`=?base64?${Buffer.from(taskId).toString("base64")}?=`,
				undefined,
			],
		]);
	});

	it("receives tool changes on the subscription POST stream while other requests complete", async () => {
		const closed = Promise.withResolvers<void>();
		const { url, requests } = await startServer((message, response) => {
			if (message.method === "server/discover") {
				response.writeHead(200, { "content-type": "application/json" });
				response.end(
					JSON.stringify({
						jsonrpc: "2.0",
						id: message.id,
						result: {
							resultType: "complete",
							supportedVersions: [TASKS_PROTOCOL_VERSION],
							capabilities: { tools: { listChanged: true } },
						},
					}),
				);
				return;
			}
			if (message.method !== "subscriptions/listen") return protocolHandler(message, response);
			response.once("close", closed.resolve);
			const _meta = { "io.modelcontextprotocol/subscriptionId": message.id };
			const ack = {
				jsonrpc: "2.0",
				method: "notifications/subscriptions/acknowledged",
				params: { _meta, notifications: { toolsListChanged: true } },
			};
			const change = { jsonrpc: "2.0", method: "notifications/tools/list_changed", params: { _meta } };
			response.writeHead(200, { "content-type": "text/event-stream" });
			response.write(`data: ${JSON.stringify(ack)}\n\ndata: ${JSON.stringify(change)}\n\n`);
		});
		const client = new McpClient({ name: "http-test", version: "1.0.0" });
		const changed = vi.fn();
		await client.connect(new StreamableHttpTransport({ url }));
		try {
			client.onNotification("notifications/tools/list_changed", changed);
			await expect.poll(() => changed.mock.calls.length).toBe(1);
			expect(await client.listTools()).toHaveLength(1);
			expect(changed).toHaveBeenCalledOnce();
		} finally {
			await client.close();
		}
		await closed.promise;
		expect(requests.map(({ method, headers }) => [method, headers["mcp-method"]])).toEqual([
			["POST", "server/discover"],
			["POST", "subscriptions/listen"],
			["POST", "tools/list"],
		]);
	});

	it("classifies authentication failures", async () => {
		const { url } = await startServer((_message, response) => {
			response.writeHead(401, { "www-authenticate": 'Bearer resource_metadata="https://example.com/meta"' });
			response.end("login required");
		});
		const client = new McpClient({ name: "http-test", version: "1.0.0" });
		await expect(client.connect(new StreamableHttpTransport({ url }))).rejects.toMatchObject({
			name: "McpAuthRequiredError",
			status: 401,
			body: "login required",
			wwwAuthenticate: 'Bearer resource_metadata="https://example.com/meta"',
		} satisfies Partial<McpAuthRequiredError>);
	});

	it.each([
		{ event: "id: dropped\nretry: 1\ndata:\n\n", errorCount: 0, reason: "resume hint" },
		{ event: "data: not json\n\n", errorCount: 1, reason: "malformed event" },
	])("fails only the affected request without SSE replay ($reason)", async ({ event, errorCount }) => {
		const started = Promise.withResolvers<void>();
		const finish = Promise.withResolvers<void>();
		const { url, requests } = await startServer(async (message, response) => {
			const name = (message.params as { name?: string } | undefined)?.name;
			if (name === "broken") {
				response.writeHead(200, { "content-type": "text/event-stream" }).end(event);
				return;
			}
			if (name === "slow") {
				started.resolve();
				await finish.promise;
			}
			protocolHandler(message, response);
		});
		const client = new McpClient({ name: "http-test", version: "1.0.0" });
		const errors = vi.fn();
		client.onError(errors);
		await client.connect(new StreamableHttpTransport({ url }));
		try {
			const slow = client.callTool("slow");
			await started.promise;
			await expect(client.callTool("broken", {}, { timeoutMs: 2_000 })).rejects.toThrow(
				"MCP response stream failed: stream ended without a response",
			);
			finish.resolve();
			expect(await slow).toEqual({ resultType: "complete", content: [{ type: "text", text: "hello" }] });
			expect(await client.listTools()).toHaveLength(1);
			expect(errors).toHaveBeenCalledTimes(errorCount);
		} finally {
			finish.resolve();
			await client.close();
		}
		expect(requests.map(({ method, headers }) => [method, headers["mcp-method"], headers["mcp-name"]])).toEqual([
			["POST", "server/discover", undefined],
			["POST", "tools/call", "slow"],
			["POST", "tools/call", "broken"],
			["POST", "tools/list", undefined],
		]);
		expect(requests.every(({ headers }) => headers["last-event-id"] === undefined)).toBe(true);
	});

	it("rejects a request the server accepts without a response", async () => {
		const { url } = await startServer((message, response) => {
			if (message.method !== "tools/call") return protocolHandler(message, response);
			response.statusCode = 202;
			response.end();
		});
		const client = new McpClient({ name: "http-test", version: "1.0.0" });
		await client.connect(new StreamableHttpTransport({ url }));
		await expect(client.callTool("echo")).rejects.toThrow("without a response");
		await client.close();
	});

	it("delivers the terminal response from a POST stream only once", async () => {
		const request = { jsonrpc: "2.0", id: 1, method: "tools/list" } as const;
		const reply = { jsonrpc: "2.0", id: request.id, result: { tools: [] } };
		const event = `id: 1\ndata: ${JSON.stringify(reply)}\n\n`;
		const fetch = vi.fn(
			async () => new Response(event + event, { headers: { "content-type": "text/event-stream" } }),
		);
		const transport = new StreamableHttpTransport({ url: "http://127.0.0.1/mcp", fetch });
		const received = vi.fn();
		transport.onMessage(received);
		await transport.start();
		try {
			await transport.send(request);
			await expect.poll(() => received.mock.calls).toEqual([[reply]]);
			expect(fetch).toHaveBeenCalledOnce();
		} finally {
			await transport.close();
		}
	});

	// #11335
	it("applies maxMessageBytes to JSON responses", async () => {
		const send = async (descriptionBytes: number) => {
			const body = JSON.stringify({
				jsonrpc: "2.0",
				id: 1,
				result: { tools: [{ name: "echo", description: "x".repeat(descriptionBytes), inputSchema: {} }] },
			});
			const transport = new StreamableHttpTransport({
				url: "http://127.0.0.1/mcp",
				maxMessageBytes: 512,
				fetch: async () => new Response(body, { headers: { "content-type": "application/json" } }),
			});
			const messages: unknown[] = [];
			transport.onMessage((message) => messages.push(message));
			await transport.start();
			try {
				await transport.send({ jsonrpc: "2.0", id: 1, method: "tools/list" });
				return messages;
			} finally {
				await transport.close();
			}
		};

		expect(await send(32)).toHaveLength(1);
		await expect(send(2048)).rejects.toThrow("MCP JSON response exceeds 512 bytes");
	});

	it("decodes JSON responses split inside a BOM and a multi-byte character", async () => {
		const bytes = new TextEncoder().encode(
			`\uFEFF${JSON.stringify({ jsonrpc: "2.0", id: 1, result: { text: "é€" } })}`,
		);
		const body = new ReadableStream<Uint8Array>({
			start(controller) {
				for (const byte of bytes) controller.enqueue(new Uint8Array([byte]));
				controller.close();
			},
		});
		const transport = new StreamableHttpTransport({
			url: "http://127.0.0.1/mcp",
			fetch: async () => new Response(body, { headers: { "content-type": "application/json" } }),
		});
		const messages: unknown[] = [];
		transport.onMessage((message) => messages.push(message));
		await transport.start();
		await transport.send({ jsonrpc: "2.0", id: 1, method: "tools/list" });
		await transport.close();
		expect(messages).toEqual([{ jsonrpc: "2.0", id: 1, result: { text: "é€" } }]);
	});

	it("reads only an excerpt of HTTP error bodies and cancels the rest", async () => {
		let cancelled = false;
		// An endless error body: reading it whole would never finish.
		const body = new ReadableStream<Uint8Array>({
			pull: (controller) => controller.enqueue(new TextEncoder().encode("x".repeat(1024))),
			cancel: () => {
				cancelled = true;
			},
		});
		const transport = new StreamableHttpTransport({
			url: "http://127.0.0.1/mcp",
			fetch: async () => new Response(body, { status: 500 }),
		});
		await transport.start();
		const error = await transport.send({ jsonrpc: "2.0", id: 1, method: "tools/list" }).catch((e: unknown) => e);
		await transport.close();
		expect(error).toBeInstanceOf(McpHttpError);
		expect((error as McpHttpError).body).toHaveLength(8 * 1024);
		expect(cancelled).toBe(true);
	});

	it("includes the response body in HTTP errors", async () => {
		const { url } = await startServer((_message, response) => {
			response.statusCode = 400;
			response.end("Invalid Accept header");
		});
		const client = new McpClient({ name: "http-test", version: "1.0.0" });
		await expect(client.connect(new StreamableHttpTransport({ url }))).rejects.toThrow(
			"MCP HTTP request failed with status 400: Invalid Accept header",
		);
	});

	it("hands 401 and insufficient-scope 403 responses to the auth provider with the rejected token", async () => {
		const seen: { status: number; token?: string }[] = [];
		let token = "old";
		const { url } = await startServer((message, response, request) => {
			if (request.headers.authorization === "Bearer old") {
				response.writeHead(401, { "www-authenticate": "Bearer" }).end();
				return;
			}
			if (message.method === "tools/call" && request.headers.authorization === "Bearer new") {
				response.writeHead(403, { "www-authenticate": 'Bearer error="insufficient_scope", scope="admin"' }).end();
				return;
			}
			protocolHandler(message, response);
		});
		const client = new McpClient({ name: "http-test", version: "1.0.0" });
		await client.connect(
			new StreamableHttpTransport({
				url,
				authProvider: {
					token: async () => token,
					onUnauthorized: async ({ response, token: rejected }) => {
						seen.push({ status: response.status, token: rejected });
						if (response.status === 401) token = "new";
						else token = "admin";
					},
				},
			}),
		);
		expect(await client.callTool("echo")).toEqual({
			resultType: "complete",
			content: [{ type: "text", text: "hello" }],
		});
		expect(seen).toEqual([
			{ status: 401, token: "old" },
			{ status: 403, token: "new" },
		]);
		await client.close();
	});

	// #10598: a request cancelled or timed out while waiting for a shared refresh must not be retried.
	it("does not retry requests that were cancelled while authorization was recovering", async () => {
		let token = "old";
		const executed: unknown[] = [];
		const { url } = await startServer((message, response, request) => {
			if (message.method === "tools/call" && request.headers.authorization === "Bearer old") {
				response.writeHead(401, { "www-authenticate": "Bearer" }).end();
				return;
			}
			if (message.method === "tools/call") executed.push((message.params as { name: string }).name);
			protocolHandler(message, response);
		});
		let releaseRefresh = () => {};
		const refreshGate = new Promise<void>((resolve) => {
			releaseRefresh = resolve;
		});
		let refresh: Promise<void> | undefined;
		let refreshes = 0;
		let waiting = 0;
		let allWaiting = () => {};
		const waitingGate = new Promise<void>((resolve) => {
			allWaiting = resolve;
		});
		const client = new McpClient({ name: "http-test", version: "1.0.0" });
		await client.connect(
			new StreamableHttpTransport({
				url,
				authProvider: {
					token: async () => token,
					onUnauthorized: async () => {
						if (!refresh) {
							refreshes++;
							refresh = refreshGate.then(() => {
								token = "new";
							});
						}
						if (++waiting === 3) allWaiting();
						await refresh;
					},
				},
			}),
		);
		const controller = new AbortController();
		const aborted = client.callTool("aborted", {}, { signal: controller.signal });
		const timedOut = client.callTool("timed-out", {}, { timeoutMs: 200 });
		const kept = client.callTool("kept");
		await waitingGate;
		controller.abort();
		await expect(aborted).rejects.toMatchObject({ name: "AbortError" });
		await expect(timedOut).rejects.toMatchObject({ name: "McpTimeoutError" });
		releaseRefresh();
		expect(await kept).toEqual({ resultType: "complete", content: [{ type: "text", text: "hello" }] });
		expect(executed).toEqual(["kept"]);
		expect(refreshes).toBe(1);
		await client.close();
	});

	// #10188: Cloudflare Workers reject the platform fetch when called with a receiver other than globalThis.
	it("calls fetch without a receiver", async () => {
		const realFetch = globalThis.fetch;
		const strictFetch = function (this: unknown, input: string | URL, init?: RequestInit) {
			if (this !== undefined && this !== globalThis) throw new TypeError("Illegal invocation");
			return realFetch(input, init);
		} satisfies McpFetch;
		const { url } = await startServer((message, response, request) => {
			if (request.headers.authorization === undefined) {
				response.writeHead(401, { "www-authenticate": "Bearer" }).end();
				return;
			}
			protocolHandler(message, response);
		});
		const connect = async (fetch: McpFetch | undefined) => {
			let token: string | undefined;
			const client = new McpClient({ name: "http-test", version: "1.0.0" });
			await client.connect(
				new StreamableHttpTransport({
					url,
					fetch,
					authProvider: {
						token: async () => token,
						onUnauthorized: async (context) => {
							await (await context.fetch(url)).body?.cancel();
							token = "token";
						},
					},
				}),
			);
			expect(await client.listTools()).toEqual([{ name: "echo", inputSchema: { type: "object" } }]);
			await client.close();
		};

		await connect(strictFetch);
		vi.stubGlobal("fetch", strictFetch);
		try {
			await connect(undefined);
		} finally {
			vi.unstubAllGlobals();
		}
	});

	it("surfaces a JSON-RPC error from a failed HTTP POST without replaying the request", async () => {
		const { url, requests } = await startServer((message, response) => {
			if (message.method !== "tools/call") return protocolHandler(message, response);
			response.writeHead(404, { "content-type": "application/json" });
			response.end(
				JSON.stringify({ jsonrpc: "2.0", id: message.id, error: { code: -32602, message: "unknown tool" } }),
			);
		});
		const client = new McpClient({ name: "http-test", version: "1.0.0" });
		await client.connect(new StreamableHttpTransport({ url }));
		try {
			await expect(client.callTool("missing")).rejects.toMatchObject({ code: -32602, message: "unknown tool" });
			expect(client.connectionState).toBe("connected");
		} finally {
			await client.close();
		}
		expect(requests.map(({ method, message }) => [method, message?.method])).toEqual([
			["POST", "server/discover"],
			["POST", "tools/call"],
		]);
	});
});
