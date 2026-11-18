// A stdio MCP server for p2's MCP tests, in the style of pi-mcp's test/fixtures/stdio-server.mjs. Tools: `echo`
// ({ text }), `whoami` (its pid, its parent's, cwd, the client's roots and its `MCP_FIXTURE_*` variables), `crash`
// (answers, then exits), `grow` (adds the tool `extra` and announces the change). `MCP_FIXTURE_LOG`: a file every start
// appends its pid to; `MCP_FIXTURE_HANG`: never answers `server/discover`.
import { appendFileSync } from "node:fs";
import { createInterface } from "node:readline";

if (process.env.MCP_FIXTURE_LOG) appendFileSync(process.env.MCP_FIXTURE_LOG, `${process.pid}\n`);
const send = (message) => process.stdout.write(`${JSON.stringify({ jsonrpc: "2.0", ...message })}\n`);
const tools = [
	{
		name: "echo",
		description: "Echoes the text back",
		inputSchema: { type: "object", properties: { text: { type: "string" } } },
	},
	{ name: "whoami", description: "Reports the server process", inputSchema: { type: "object" } },
	{ name: "crash", description: "Exits the server", inputSchema: { type: "object" } },
	{ name: "grow", description: "Adds a tool", inputSchema: { type: "object" } },
];
const vars = Object.fromEntries(Object.entries(process.env).filter(([key]) => key.startsWith("MCP_FIXTURE_")));
const whoami = (roots) => ({
	content: [
		{ type: "text", text: JSON.stringify({ pid: process.pid, ppid: process.ppid, cwd: process.cwd(), roots, vars }) },
	],
});
const tasks = new Map();
let subscriptionMeta;
const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
for await (const line of lines) {
	const message = JSON.parse(line);
	if (!("id" in message)) continue;
	const reply = (result) => send({ id: message.id, result: { resultType: "complete", ...result } });
	if (message.method === "server/discover") {
		if (process.env.MCP_FIXTURE_HANG) continue;
		reply({
			supportedVersions: ["2026-07-28"],
			capabilities: { tools: { listChanged: true }, extensions: { "io.modelcontextprotocol/tasks": {} } },
			_meta: { "io.modelcontextprotocol/serverInfo": { name: "p2-fixture", version: "1.0.0" } },
		});
	} else if (message.method === "subscriptions/listen") {
		if (message.params.notifications?.toolsListChanged !== true) {
			send({ id: message.id, error: { code: -32602, message: "expected a tool list subscription" } });
			continue;
		}
		subscriptionMeta = { "io.modelcontextprotocol/subscriptionId": message.id };
		send({
			method: "notifications/subscriptions/acknowledged",
			params: { _meta: subscriptionMeta, notifications: { toolsListChanged: true } },
		});
	} else if (message.method === "tools/list") reply({ tools });
	else if (message.method === "tools/call") {
		const { name, arguments: args } = message.params;
		if (name === "echo") reply({ content: [{ type: "text", text: `echo: ${args?.text}` }] });
		else if (name === "whoami") {
			if (message.params._meta["io.modelcontextprotocol/clientCapabilities"].roots === undefined) {
				reply(whoami(null));
			} else {
				const taskId = String(message.id);
				tasks.set(taskId, { roots: undefined });
				send({ id: message.id, result: { resultType: "task", taskId, status: "input_required" } });
			}
		} else if (name === "crash") {
			reply({ content: [{ type: "text", text: "bye" }] });
			console.error("fixture crashing on purpose");
			setTimeout(() => process.exit(1), 20);
		} else if (name === "grow") {
			if (!tools.some((tool) => tool.name === "extra"))
				tools.push({ name: "extra", description: "Added later", inputSchema: { type: "object" } });
			reply({ content: [{ type: "text", text: "grown" }] });
			if (subscriptionMeta) {
				send({ method: "notifications/tools/list_changed", params: { _meta: subscriptionMeta } });
			}
		} else reply({ content: [{ type: "text", text: `no tool ${name}` }], isError: true });
	} else if (message.method === "tasks/get" || message.method === "tasks/update") {
		const { taskId } = message.params;
		const task = tasks.get(taskId);
		if (!task) {
			send({ id: message.id, error: { code: -32602, message: `no task ${taskId}` } });
		} else if (message.method === "tasks/update") {
			task.roots = message.params.inputResponses.roots.roots;
			reply({});
		} else if (task.roots === undefined) {
			reply({
				taskId,
				status: "input_required",
				inputRequests: { roots: { method: "roots/list" } },
				pollIntervalMs: 1,
			});
		} else {
			tasks.delete(taskId);
			reply({ taskId, status: "completed", result: whoami(task.roots) });
		}
	} else send({ id: message.id, error: { code: -32601, message: "not found" } });
}
