import { isObject, JSON_RPC_ERROR_CODES, McpError } from "./jsonrpc.ts";

const DEFAULT_POLL_INTERVAL_MS = 2_000;
const MAX_TIMER_MS = 2_147_483_647;

type TaskStatus = "working" | "input_required" | "completed" | "failed" | "cancelled";

interface TaskProgress {
	taskId: string;
	status: TaskStatus;
	statusMessage?: string;
	pollIntervalMs: number;
}

export type TaskHandle = TaskProgress & { resultType: "task" };

export type TaskSnapshot = TaskProgress & { resultType: "complete" } & (
		| { status: "working" | "cancelled" }
		| { status: "input_required"; inputRequests: Record<string, unknown> }
		| { status: "completed"; result: Record<string, unknown> }
		| { status: "failed"; error: McpError }
	);

/** Decode either a creation handle or a detailed snapshot, optionally matching the polled task ID. */
export function readTaskSnapshot(value: unknown, resultType: "task"): TaskHandle;
export function readTaskSnapshot(value: unknown, resultType: "complete", taskId: string): TaskSnapshot;
export function readTaskSnapshot(
	value: unknown,
	resultType: "task" | "complete",
	taskId?: string,
): TaskHandle | TaskSnapshot {
	const invalid = () => new McpError(JSON_RPC_ERROR_CODES.invalidRequest, "Invalid MCP task snapshot");
	if (
		!isObject(value) ||
		value.resultType !== resultType ||
		typeof value.taskId !== "string" ||
		!/^[\x21-\x7e]{1,128}$/.test(value.taskId) ||
		(taskId !== undefined && value.taskId !== taskId) ||
		(value.statusMessage !== undefined && typeof value.statusMessage !== "string")
	) {
		throw invalid();
	}
	const { status, inputRequests, result, error, pollIntervalMs } = value;
	const progress = {
		taskId: value.taskId,
		statusMessage: value.statusMessage,
		pollIntervalMs:
			typeof pollIntervalMs === "number" && Number.isInteger(pollIntervalMs) && pollIntervalMs >= 0
				? Math.max(1, Math.min(MAX_TIMER_MS, pollIntervalMs))
				: DEFAULT_POLL_INTERVAL_MS,
	};
	switch (status) {
		case "working":
		case "cancelled":
			return { ...progress, resultType, status };
		case "input_required":
			if (resultType === "task") return { ...progress, resultType, status };
			if (!isObject(inputRequests)) throw invalid();
			return { ...progress, resultType, status, inputRequests };
		case "completed":
			if (resultType === "task") return { ...progress, resultType, status };
			if (!isObject(result)) throw invalid();
			return { ...progress, resultType, status, result };
		case "failed":
			if (resultType === "task") return { ...progress, resultType, status };
			if (!isObject(error) || typeof error.code !== "number" || typeof error.message !== "string") throw invalid();
			return { ...progress, resultType, status, error: new McpError(error.code, error.message, error.data) };
		default:
			throw invalid();
	}
}
