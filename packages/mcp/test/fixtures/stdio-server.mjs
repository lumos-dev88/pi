import { createInterface } from "node:readline";

console.error("stdio fixture ready");
const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });

for await (const line of lines) {
	const message = JSON.parse(line);
	if (!("id" in message)) continue;
	let result;
	if (message.method === "server/discover") {
		result = {
			resultType: "complete",
			supportedVersions: ["2026-07-28"],
			capabilities: { tools: {} },
			_meta: { "io.modelcontextprotocol/serverInfo": { name: "stdio-fixture", version: "1.0.0" } },
		};
	} else if (message.method === "tools/list") {
		result = {
			tools: [{ name: "echo", inputSchema: { type: "object", properties: { text: { type: "string" } }, required: ["text"] } }],
		};
	} else if (message.method === "tools/call") {
		result = { resultType: "complete", content: [{ type: "text", text: String(message.params.arguments.text) }] };
	} else {
		process.stdout.write(`${JSON.stringify({ jsonrpc: "2.0", id: message.id, error: { code: -32601, message: "not found" } })}\n`);
		continue;
	}
	process.stdout.write(`${JSON.stringify({ jsonrpc: "2.0", id: message.id, result })}\n`);
}
