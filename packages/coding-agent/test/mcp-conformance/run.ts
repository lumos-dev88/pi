/**
 * Runs the official MCP client conformance suite against pi (client.ts) and compares every check with
 * the committed baseline (baseline.json). Fails when a check that passed in the baseline no longer
 * passes, or when a check fails that the baseline does not list. Known failures stay visible in the
 * output. See README.md.
 */

import { spawn } from "node:child_process";
import {
	chmodSync,
	existsSync,
	mkdirSync,
	mkdtempSync,
	readdirSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { parseArgs } from "node:util";

/** The pinned upstream suite. Bumping it requires reviewing and regenerating the baseline. */
const CONFORMANCE_PACKAGE = "@modelcontextprotocol/conformance@0.2.0-alpha.11";
const PROTOCOL_VERSION = "2026-07-28";
// Cover the parts of the upstream suite exercised by pi's MCP and OAuth client.
const SCENARIOS: readonly string[] = [
	"tools_call",
	"request-metadata",
	"http-standard-headers",
	"json-schema-2020-12-preservation",
	"auth/metadata-default",
	"auth/metadata-var1",
	"auth/metadata-var2",
	"auth/metadata-var3",
	"auth/basic-cimd",
	"auth/scope-from-www-authenticate",
	"auth/scope-from-scopes-supported",
	"auth/scope-omitted-when-undefined",
	"auth/scope-step-up",
	"auth/scope-retry-limit",
	"auth/token-endpoint-auth-basic",
	"auth/token-endpoint-auth-post",
	"auth/token-endpoint-auth-none",
	"auth/pre-registration",
];
const SCENARIO_TIMEOUT_MS = 30_000;
/** Checks that are part of the suite's HTTP trace, not assertions. */
const IGNORED_STATUSES = new Set(["INFO", "SKIPPED"]);

const here = fileURLToPath(new URL(".", import.meta.url));
const baselinePath = join(here, "baseline.json");
const clientPath = join(here, "client.ts");
const resolverUrl = pathToFileURL(join(here, "../../src/experimental/source-resolver.ts")).href;

type CheckStatus = "pass" | "fail";

interface ScenarioResult {
	scenario: string;
	/** Check id to status. Repeated ids pass only when every instance passes. */
	checks: Map<string, CheckStatus>;
	/** Failure messages by check id, for the report. */
	messages: Map<string, string>;
}

interface BaselineScenario {
	passing: string[];
	failing: string[];
}

interface Baseline {
	conformance: string;
	protocolVersion: string;
	scenarios: Record<string, BaselineScenario>;
}

interface RawCheck {
	id?: unknown;
	name?: unknown;
	status?: unknown;
	description?: unknown;
	errorMessage?: unknown;
}

function runCommand(
	command: string,
	args: string[],
	options: { cwd: string; env?: NodeJS.ProcessEnv; timeoutMs?: number },
): Promise<{ code: number; stdout: string; stderr: string }> {
	return new Promise((resolve, reject) => {
		const child = spawn(command, args, { cwd: options.cwd, env: options.env ?? process.env, detached: true });
		let stdout = "";
		let stderr = "";
		child.stdout.on("data", (data: Buffer) => {
			stdout += data.toString();
		});
		child.stderr.on("data", (data: Buffer) => {
			stderr += data.toString();
		});
		const timer =
			options.timeoutMs === undefined
				? undefined
				: setTimeout(() => {
						// The runner spawns the client and scenario servers; end the whole group.
						if (child.pid !== undefined) {
							try {
								process.kill(-child.pid, "SIGKILL");
							} catch {}
						}
					}, options.timeoutMs);
		child.on("error", reject);
		child.on("close", (code, signal) => {
			if (timer) clearTimeout(timer);
			resolve({ code: code ?? (signal ? 128 : 1), stdout, stderr });
		});
	});
}

function conformanceEnv(): NodeJS.ProcessEnv {
	// The suite comes from npm with prebuilt files; nothing needs install scripts.
	return { ...process.env, npm_config_ignore_scripts: "true", npm_config_yes: "true" };
}

function findFile(dir: string, name: string): string | undefined {
	if (!existsSync(dir)) return undefined;
	for (const entry of readdirSync(dir, { withFileTypes: true })) {
		const path = join(dir, entry.name);
		if (entry.isFile() && entry.name === name) return path;
		if (entry.isDirectory()) {
			const found = findFile(path, name);
			if (found) return found;
		}
	}
	return undefined;
}

function record(result: ScenarioResult, id: string, status: CheckStatus, message?: string): void {
	if (result.checks.get(id) !== "fail") result.checks.set(id, status);
	if (status === "fail" && message && !result.messages.has(id)) result.messages.set(id, message);
}

async function runScenario(
	scenario: string,
	launcher: string,
	workDir: string,
	verbose: boolean,
): Promise<ScenarioResult> {
	const scenarioDir = join(workDir, scenario.replace(/[^A-Za-z0-9_.-]+/g, "-"));
	mkdirSync(scenarioDir, { recursive: true });
	const reportPath = join(scenarioDir, "pi-client.json");
	const result: ScenarioResult = { scenario, checks: new Map(), messages: new Map() };
	const run = await runCommand(
		"npx",
		[
			"--yes",
			CONFORMANCE_PACKAGE,
			"client",
			"--command",
			launcher,
			"--scenario",
			scenario,
			"--spec-version",
			PROTOCOL_VERSION,
			"--timeout",
			String(SCENARIO_TIMEOUT_MS),
			"--output-dir",
			scenarioDir,
		],
		{
			cwd: workDir,
			env: { ...conformanceEnv(), PI_MCP_CONFORMANCE_REPORT: reportPath },
			timeoutMs: SCENARIO_TIMEOUT_MS + 30_000,
		},
	);
	if (verbose) process.stderr.write(run.stderr);

	const checksPath = findFile(scenarioDir, "checks.json");
	if (!checksPath) {
		record(result, "runner", "fail", `No checks.json (runner exit ${run.code}): ${run.stderr.trim().slice(-2000)}`);
		return result;
	}
	const checks = JSON.parse(readFileSync(checksPath, "utf8")) as RawCheck[];
	for (const check of checks) {
		const status = String(check.status ?? "FAILURE").toUpperCase();
		if (IGNORED_STATUSES.has(status)) continue;
		const id = String(check.id ?? check.name ?? "unnamed");
		const message = String(check.errorMessage ?? check.description ?? "");
		record(result, id, status === "SUCCESS" ? "pass" : "fail", message);
	}
	// Whether pi completed the scenario. Some scenarios expect the client to give up, so this is
	// baselined like any other check.
	let client: { success?: unknown; error?: unknown } = { error: "client.ts wrote no report" };
	if (existsSync(reportPath)) client = JSON.parse(readFileSync(reportPath, "utf8")) as typeof client;
	record(result, "pi-client", client.success === true ? "pass" : "fail", String(client.error ?? ""));
	return result;
}

function loadBaseline(): Baseline | undefined {
	if (!existsSync(baselinePath)) return undefined;
	return JSON.parse(readFileSync(baselinePath, "utf8")) as Baseline;
}

function toBaseline(results: ScenarioResult[]): Baseline {
	const baseline: Baseline = { conformance: CONFORMANCE_PACKAGE, protocolVersion: PROTOCOL_VERSION, scenarios: {} };
	for (const result of results) {
		const ids = [...result.checks.keys()].sort();
		baseline.scenarios[result.scenario] = {
			passing: ids.filter((id) => result.checks.get(id) === "pass"),
			failing: ids.filter((id) => result.checks.get(id) === "fail"),
		};
	}
	return baseline;
}

/** Compare with the baseline. Returns whether there is no regression. */
function evaluate(results: ScenarioResult[], baseline: Baseline): boolean {
	const regressions: string[] = [];
	const fixed: string[] = [];
	for (const result of results) {
		const expected = baseline.scenarios[result.scenario];
		for (const [id, status] of result.checks) {
			const message = result.messages.get(id);
			if (status === "fail" && !expected?.failing.includes(id)) {
				regressions.push(`${result.scenario}: ${id} fails${message ? `: ${message}` : ""}`);
			}
			if (status === "pass" && expected?.failing.includes(id)) fixed.push(`${result.scenario}: ${id}`);
		}
		for (const id of expected?.passing ?? []) {
			if (!result.checks.has(id)) regressions.push(`${result.scenario}: ${id} was not reported`);
		}
	}
	for (const scenario of Object.keys(baseline.scenarios)) {
		if (!results.some((result) => result.scenario === scenario))
			regressions.push(`${scenario}: scenario did not run`);
	}
	if (fixed.length > 0) {
		console.log(
			`\nFixed since the baseline (run with --update-baseline):\n${fixed.map((line) => `  ${line}`).join("\n")}`,
		);
	}
	if (regressions.length > 0) {
		console.log(`\nRegressions:\n${regressions.map((line) => `  ${line}`).join("\n")}`);
		return false;
	}
	console.log("\nNo regressions against the baseline.");
	return true;
}

function printResults(results: ScenarioResult[]): void {
	console.log(`\n${PROTOCOL_VERSION}`);
	for (const result of results) {
		const statuses = [...result.checks.values()];
		const failed = statuses.filter((status) => status === "fail").length;
		const passed = statuses.length - failed;
		console.log(`  ${failed === 0 ? "pass" : "FAIL"} ${result.scenario} (${passed} passed, ${failed} failed)`);
		for (const [id, status] of result.checks) {
			if (status !== "fail") continue;
			const message = result.messages.get(id)?.replace(/\s+/g, " ").slice(0, 300);
			console.log(`         ${id}${message ? `: ${message}` : ""}`);
		}
	}
}

async function main(): Promise<number> {
	const { values } = parseArgs({
		options: {
			scenario: { type: "string", multiple: true },
			"update-baseline": { type: "boolean", default: false },
			"keep-results": { type: "boolean", default: false },
			verbose: { type: "boolean", default: false },
			help: { type: "boolean", short: "h", default: false },
		},
	});
	if (values.help) {
		console.log(`Usage: node packages/coding-agent/test/mcp-conformance/run.ts [options]

Options:
  --scenario <name>    Only run this scenario (repeatable)
  --update-baseline    Write the results to baseline.json instead of comparing
  --keep-results       Keep the upstream checks.json and client output
  --verbose            Print the upstream runner output`);
		return 0;
	}
	if (process.platform === "win32") {
		console.error("The conformance runner needs a POSIX shell.");
		return 1;
	}
	const scenarios = values.scenario ?? SCENARIOS;
	for (const scenario of scenarios) {
		if (!SCENARIOS.includes(scenario)) {
			console.error(`Unsupported scenario ${scenario}. Supported: ${SCENARIOS.join(", ")}`);
			return 1;
		}
	}
	if (values["update-baseline"] && values.scenario) {
		console.error("--update-baseline needs a full run, without --scenario.");
		return 1;
	}

	const workDir = mkdtempSync(join(tmpdir(), "pi-mcp-conformance-"));
	try {
		// The upstream runner splits --command on spaces, so use a launcher path without any.
		const launcher = join(workDir, "pi-client");
		writeFileSync(
			launcher,
			`#!/bin/sh\nexec ${JSON.stringify(process.execPath)} --import ${JSON.stringify(resolverUrl)} ${JSON.stringify(clientPath)} "$@"\n`,
		);
		chmodSync(launcher, 0o755);

		const results: ScenarioResult[] = [];
		for (const scenario of scenarios) {
			process.stderr.write(`${PROTOCOL_VERSION} ${scenario}\n`);
			results.push(await runScenario(scenario, launcher, workDir, values.verbose));
		}
		printResults(results);

		if (values["update-baseline"]) {
			if (results.some((result) => result.checks.has("runner"))) {
				console.error("\nCannot update the baseline: the runner did not produce checks for every scenario.");
				return 1;
			}
			writeFileSync(baselinePath, `${JSON.stringify(toBaseline(results), null, "\t")}\n`);
			console.log(`\nWrote ${baselinePath}`);
			return 0;
		}
		const baseline = loadBaseline();
		if (!baseline) {
			console.error("\nNo baseline.json. Run with --update-baseline and review it.");
			return 1;
		}
		if (baseline.conformance !== CONFORMANCE_PACKAGE || baseline.protocolVersion !== PROTOCOL_VERSION) {
			console.error(`\nbaseline.json does not match ${CONFORMANCE_PACKAGE} on ${PROTOCOL_VERSION}. Regenerate it.`);
			return 1;
		}
		const scoped: Baseline = values.scenario
			? {
					...baseline,
					scenarios: Object.fromEntries(
						Object.entries(baseline.scenarios).filter(([scenario]) => scenarios.includes(scenario)),
					),
				}
			: baseline;
		return evaluate(results, scoped) ? 0 : 1;
	} finally {
		if (values["keep-results"]) console.log(`\nResults kept in ${workDir}`);
		else rmSync(workDir, { recursive: true, force: true });
	}
}

process.exit(await main());
