// P0.2 validation harness: the interlude OpenAI provider INSIDE wasm, hitting a
// local mock OpenAI SSE server (mock-sse.cjs spawned as a child), streaming the
// resulting events out to this Node harness.
//
// Runs three bursts (mock-plain / mock-tool / mock-slow) and asserts:
//   plain : >=6 token events, concatenated text == mock content, completion_ended
//   tool  : a tool_call event named get_weather appears, then completion_ended
//           (ToolPolicyExitAfter ends the burst on a tool_call)
//   slow  : tokens spread over >=500ms, content matches the mock
//
// Usage: node harness2.mjs [port]   (default 9101)
import { createRequire } from "node:module";
import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";

const require = createRequire(import.meta.url);
const dir = path.dirname(new URL(import.meta.url).pathname);

// Globals required by wasm_exec.js (Node already provides most).
globalThis.fs = fs;
globalThis.path = path;
globalThis.process = process;
globalThis.crypto ??= require("node:crypto").webcrypto;
globalThis.performance ??= require("node:perf_hooks").performance;

// Load the Go exec shim -> sets globalThis.Go.
require(path.join(dir, "wasm_exec.js"));
const Go = globalThis.Go;

// Scenario content must match mock-sse.cjs (imported, not started: it only
// starts a server when it is the main module).
const { SCENARIOS } = require(path.join(dir, "mock-sse.cjs"));
const PLAIN_EXPECTED = SCENARIOS["mock-plain"].tokens.join("");
const SLOW_EXPECTED = SCENARIOS["mock-slow"].tokens.join("");
const TOOL_EXPECTED_ARGS = SCENARIOS["mock-tool"].argsFirst + SCENARIOS["mock-tool"].argsSecond;
const TOOL_EXPECTED_NAME = SCENARIOS["mock-tool"].toolName;

const PORT = parseInt(process.argv[2], 10) || 9101;
const ENDPOINT = `http://localhost:${PORT}/v1`;
const BURST_TIMEOUT_MS = 15000;

// ---------------------------------------------------------------------------
// 1. Spawn the mock SSE server and wait for readiness
// ---------------------------------------------------------------------------
const mock = spawn(process.execPath, [path.join(dir, "mock-sse.cjs"), String(PORT)], {
	stdio: ["ignore", "pipe", "pipe"],
});
let mockReady = false;
mock.stdout.on("data", (d) => {
	process.stdout.write(`  [mock] ${d}`);
	if (!mockReady && String(d).includes("listening on")) mockReady = true;
});
mock.stderr.on("data", (d) => process.stderr.write(`  [mock:err] ${d}`));
const mockReadyPromise = new Promise((resolve) => {
	const t0 = Date.now();
	const iv = setInterval(() => {
		if (mockReady) {
			clearInterval(iv);
			resolve(true);
		} else if (Date.now() - t0 > 5000) {
			clearInterval(iv);
			resolve(false);
		}
	}, 20);
});
if (!(await mockReadyPromise)) {
	console.log("FAIL: mock server did not become ready within 5000ms");
	mock.kill();
	process.exit(1);
}
console.log(`\n>> mock SSE server ready on :${PORT}\n`);

// ---------------------------------------------------------------------------
// 2. Instantiate the owner wasm
// ---------------------------------------------------------------------------
const wasmPath = path.join(dir, "owner.wasm");
const go = new Go();
const bytes = fs.readFileSync(wasmPath);
const { instance } = await WebAssembly.instantiate(bytes, go.importObject);

// KEY (P0.2 finding): Go's js/wasm net/http disables the Fetch API when it
// detects Node — net/http/roundtrip_js.go sets
//   jsFetchDisabled = process.argv0 starts with "node"
// and then routes ALL HTTP through an in-memory fake network (net/net_fake.go)
// that cannot reach host sockets: dials to localhost fail with
// "Connection refused" (or hang). That flag is a package var evaluated during
// Go startup, so BEFORE go.run() we shadow globalThis.process with a proxy
// that reports a non-node argv0. Everything else still delegates to the real
// process. Node 22's global fetch then serves the owner, including streaming
// response bodies (SSE) via ReadableStream.getReader().
if (String(process.argv0).startsWith("node")) {
	// A Proxy can't override argv0 (non-configurable target invariant), so use
	// a plain shadow object carrying only what the harness needs from the real
	// process. Go's runtime references globalThis.process in exactly one place
	// (the detection above), so this is safe.
	const real = process;
	const shadow = {
		argv0: "wasm-owner",
		execPath: real.execPath,
		argv: real.argv,
		env: real.env,
		stdout: real.stdout,
		stderr: real.stderr,
		exit: (code) => real.exit(code),
	};
	Object.defineProperty(globalThis, "process", { value: shadow, configurable: true });
}
go.run(instance); // start Go runtime + main (blocks; not awaited)

// Single stable dispatcher: Go's stored js.Value points at THIS function, so
// we can swap `sink` per burst without re-calling setCallback.
let sink = null;
globalThis.__cyrCallback = (evJSON) => {
	const ev = JSON.parse(evJSON);
	sink?.({ t: Date.now(), ev });
};
instance.exports.setCallback();

// ---------------------------------------------------------------------------
// 3. Burst runner: set opts, start, collect until completion_ended or timeout
// ---------------------------------------------------------------------------
function runBurst(model, userText = "hello") {
	return new Promise((resolve) => {
		const events = [];
		let done = false;
		sink = ({ t, ev }) => {
			const txt =
				ev.payload && "text" in ev.payload
					? ` ${JSON.stringify(ev.payload.text)}`
					: ev.type === "error"
						? ` ${JSON.stringify(ev.payload)}`
						: "";
			console.log(`[t=${t}] ${ev.type}${txt}`);
			events.push({ t, ev });
			if (!done && ev.type === "completion_ended") {
				done = true;
				// Defer so the synchronous Go->JS stack unwinds first.
				setImmediate(() => resolve(events));
			}
		};
		globalThis.__cyrOpts = JSON.stringify({
			userText,
			endpoint: ENDPOINT,
			apiKey: "mock-key",
			model,
			useOpenAI: true,
		});
		const watchdog = setTimeout(() => {
			if (!done) {
				done = true;
				events.push({ t: Date.now(), ev: { type: "__timeout__", payload: {} } });
				resolve(events);
			}
		}, BURST_TIMEOUT_MS);
		try {
			instance.exports.startBurst();
			// KEY (P0.2 finding): startBurst spawns the session/consumer
			// goroutines and returns normally, so the wasmexport wrapper does NOT
			// pump them. The runtime's beforeIdle only schedules an idle timeout
			// when a host timer is already pending, so a freshly-spawned burst is
			// never scheduled on the 2nd+ burst (0 events, no HTTP request). Kick
			// the event loop explicitly: resume() runs the PC_F loop, which starts
			// the burst's goroutines (the OpenAI HTTP request / echo timers). From
			// there the burst is driven by its own host events (SSE response,
			// timers). One kick per burst is sufficient.
			instance.exports.resume();
		} catch (e) {
			clearTimeout(watchdog);
			done = true;
			sink = null;
			resolve([{ t: Date.now(), ev: { type: "__start_error__", payload: { error: String(e) } } }]);
			return;
		}
		// the watchdog above stays armed until completion_ended resolves us
	});
}

// ---------------------------------------------------------------------------
// 4. Run the three bursts and validate
// ---------------------------------------------------------------------------
const results = {};

// Common stats for token-stream bursts (plain & slow).
function collectStats(events) {
	const types = events.map((e) => e.ev.type);
	const tokenTexts = events.filter((e) => e.ev.type === "token").map((e) => e.ev.payload.text);
	const text = tokenTexts.join("");
	const firstEnded = types.indexOf("completion_ended");
	const lastToken = types.lastIndexOf("token");
	const minT = events[0]?.t ?? 0;
	const maxT = events[events.length - 1]?.t ?? 0;
	const spread = maxT - minT;
	const nonDecreasing = events.every((e, i) => i === 0 || e.t >= events[i - 1].t);
	console.log("  event types:", types.join(" -> "));
	console.log(`  token count=${tokenTexts.length} text=${JSON.stringify(text)} spread=${spread}ms`);
	return { types, tokenTexts, text, firstEnded, lastToken, spread, nonDecreasing };
}

function commonStreamReasons(s, expectedText) {
	const reasons = [];
	if (s.tokenTexts.length < 6) reasons.push(`only ${s.tokenTexts.length} token events (<6)`);
	if (s.text !== expectedText) reasons.push(`text mismatch: ${JSON.stringify(s.text)} != ${JSON.stringify(expectedText)}`);
	if (s.firstEnded === -1) reasons.push("no completion_ended");
	else if (s.lastToken >= 0 && s.firstEnded < s.lastToken) reasons.push("completion_ended before last token");
	if (!s.nonDecreasing) reasons.push("timestamps not monotonic non-decreasing");
	return reasons;
}

function validatePlain(events) {
	const s = collectStats(events);
	const reasons = commonStreamReasons(s, PLAIN_EXPECTED);
	return { pass: reasons.length === 0, reasons, text: s.text, spread: s.spread };
}

function validateTool(events) {
	const reasons = [];
	const types = events.map((e) => e.ev.type);
	const toolCalls = events.filter((e) => e.ev.type === "tool_call");
	const byName = toolCalls.filter((e) => e.ev.payload.name === TOOL_EXPECTED_NAME);
	const firstEnded = types.indexOf("completion_ended");
	const lastToolCall = toolCalls.length
		? types.lastIndexOf("tool_call")
		: -1;
	if (byName.length === 0) reasons.push(`no tool_call with name ${TOOL_EXPECTED_NAME} (got ${JSON.stringify(toolCalls.map((e) => e.ev.payload))})`);
	else {
		const args = byName.map((e) => e.ev.payload.text);
		const hasFullArgs = args.includes(TOOL_EXPECTED_ARGS);
		if (!hasFullArgs) reasons.push(`no tool_call with full args ${JSON.stringify(TOOL_EXPECTED_ARGS)} (got ${JSON.stringify(args)})`);
	}
	if (firstEnded === -1) reasons.push("no completion_ended (burst did not end after tool_call)");
	else if (lastToolCall >= 0 && firstEnded < lastToolCall) reasons.push("completion_ended before last tool_call");
	console.log("  event types:", types.join(" -> "));
	console.log(`  tool_calls=${JSON.stringify(toolCalls.map((e) => e.ev.payload))}`);
	return { pass: reasons.length === 0, reasons };
}

function validateSlow(events) {
	const s = collectStats(events);
	const reasons = [...commonStreamReasons(s, SLOW_EXPECTED)];
	if (s.spread < 500) reasons.push(`tokens not spread out (spread ${s.spread}ms < 500ms)`);
	return { pass: reasons.length === 0, reasons, text: s.text, spread: s.spread };
}

const MODES = [
	{ model: "mock-plain", validate: validatePlain },
	{ model: "mock-tool", validate: validateTool },
	{ model: "mock-slow", validate: validateSlow },
];

let allPass = true;
for (const { model, validate } of MODES) {
	console.log(`\n================ BURST: ${model} ================`);
	const events = await runBurst(model);
	const r = validate(events);
	results[model] = r.pass;
	if (!r.pass) {
		allPass = false;
		console.log(`  FAIL (${model}): ${r.reasons.join("; ")}`);
	} else {
		console.log(`  PASS (${model})`);
	}
}

// ---------------------------------------------------------------------------
// 5. Report + cleanup
// ---------------------------------------------------------------------------
console.log("\n================ SUMMARY ================");
for (const { model } of MODES) {
	console.log(`${model}: ${results[model] ? "PASS" : "FAIL"}`);
}
console.log(allPass ? "\nPASS: openai provider in wasm vs mock SSE (plain/tool/slow)" : "\nFAIL");

mock.kill();
process.exit(allPass ? 0 : 1);
