// P1 (C1) validation harness: per-dialogue id-keyed parse-cache + ordered
// id-list walk + batched lazy pull via the owner-local JS getMessages bridge.
//
// BURST CLIENT: the interlude OpenAI provider against a local mock SSE server
// (mock-sse.cjs spawned as a child — the P0.2 setup). This is deliberate: the
// Go 1.26 js/wasm f-loop traps (RuntimeError "null function or function
// signature mismatch" at wasm_pc_f_loop) on the ECHO (host-timer-driven) burst
// path once a dialogue alternates timer bursts with exports — a runtime bug
// also reproduced with a bare `nop` export, independent of the C1 code. The
// fetch/SSE-driven burst path (this harness + harness2) is stable: 8+
// sequential bursts and per-burst stop/resume all verified clean. The C1
// mechanism under test (cache / pull / walk) is client-independent.
//
// getMessages stub: logs EVERY call (dialogue + ids) so the harness can assert
// batching + parse-once, and returns canned id -> rawEventJSON (distinct,
// recognizable content per id — the id is embedded in the text).
//
// Scenarios (ONE export per phase — two exports before a single kick is the
// other observed trap shape on this runtime, so the busy-guard scenario from
// the echo harness is not replayed here):
//   S1:  goStart(d1, [a,b,c], "turn1") — ONE batched getMessages([a,b,c]);
//        dump shows a,b,c in order + turn1 + the assistant response.
//   S2:  goSend(d1, [a,b,c,d], "turn2") — ONE batched getMessages([d]) ONLY
//        (a,b,c served from cache — THE key C1 property: parse-once, no re-pull).
//   S3 (edge, missing-id): goSend(d1, [a..e], "turn3") where id "e" has NO
//        content — getMessages([e]) returns {} -> wasm skips + warns, burst runs.
//   S4:  goStart(d2, [a], "t1") — per-dialogue cache isolation: id "a" is
//        pulled AGAIN for dialogue d2 (the cache is keyed by dialogue id).
//   S4b: goSend(d2, [a], "t2") — no new pull (a cached for d2), history grows.
//   S5:  goStart(d3, [b], "s1") on the slow mock model, goStop mid-burst —
//        a stopped burst yields exactly 1 completion_ended + partial tokens.
//   S5b: goSend(d3, [b], "s2") — the handle SURVIVES the cancel (per-burst
//        ctx) and runs a FULL follow-up burst.
//
// Burst completion marker: the OpenAI client yields exactly ONE completion_ended
// per burst (the stream's own; verified in P0.2).
//
// Usage: node harness3.mjs [port]   (default 9111)
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

// Mock SSE scenario content (must match mock-sse.cjs).
const { SCENARIOS } = require(path.join(dir, "mock-sse.cjs"));
const MOCK_PLAIN = SCENARIOS["mock-plain"].tokens.join("");
const MOCK_SLOW = SCENARIOS["mock-slow"].tokens.join("");
const SLOW_TOKENS = SCENARIOS["mock-slow"].tokens.length;

const PORT = parseInt(process.argv[2], 10) || 9111;
const ENDPOINT = `http://localhost:${PORT}/v1`;

// ---------------------------------------------------------------------------
// 1. Spawn the mock SSE server and wait for readiness (P0.2 setup)
// ---------------------------------------------------------------------------
const mock = spawn(process.execPath, [path.join(dir, "mock-sse.cjs"), String(PORT)], {
	stdio: ["ignore", "pipe", "pipe"],
});
let mockReady = false;
mock.stdout.on("data", (d) => {
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
console.log(`>> mock SSE server ready on :${PORT}`);

// KEY (P0.2 finding): Go's js/wasm net/http disables the Fetch API when it
// detects Node (process.argv0 starts with "node") and routes all HTTP through
// an in-memory fake network that cannot reach host sockets. Shadow
// globalThis.process with a non-node argv0 BEFORE go.run() so the owner's
// fetch-backed net/http reaches the mock server.
if (String(process.argv0).startsWith("node")) {
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

// ---------------------------------------------------------------------------
// 2. Instantiate the owner wasm
// ---------------------------------------------------------------------------
const go = new Go();
const bytes = fs.readFileSync(path.join(dir, "owner.wasm"));
const { instance } = await WebAssembly.instantiate(bytes, go.importObject);

// ---------------------------------------------------------------------------
// 3. Canned message store + the getMessages stub (owner-local JS bridge)
// ---------------------------------------------------------------------------
const msg = (type, text) => JSON.stringify({ type, payload: { text } });

// Distinct, recognizable content per id (the id is embedded in the text).
// NOTE: id "e" is deliberately NOT in STORE (missing-id edge case, S3).
const STORE = {
	a: msg("user_message", "msg a: user opens the dialogue"),
	b: msg("assistant_message", "msg b: assistant responds"),
	c: msg("user_message", "msg c: user follows up"),
	d: msg("assistant_message", "msg d: assistant responds again"),
};

const pullCalls = []; // every getMessages call: {dialogue, ids[]}
const pullCount = {}; // "dialogue:id" -> number of times pulled
globalThis.__cyrGetMessages = (dialogue, ids) => {
	const arr = [...ids];
	pullCalls.push({ dialogue, ids: arr });
	for (const id of arr) {
		const k = dialogue + ":" + id;
		pullCount[k] = (pullCount[k] || 0) + 1;
	}
	console.log(`  [getMessages] dialogue=${dialogue} ids=${JSON.stringify(arr)}`);
	const out = {};
	for (const id of arr) if (id in STORE) out[id] = STORE[id];
	return out; // ids without content are OMITTED (missing-id contract)
};

// ---------------------------------------------------------------------------
// Event sink: (handleID, envelopeJSON) — the C1 callback shape
// ---------------------------------------------------------------------------
let phase = null; // {target, ended, events, tokens, resolve}
globalThis.__cyrCallback = (hid, envelopeJSON) => {
	const ev = JSON.parse(envelopeJSON);
	const t = Date.now();
	const txt = ev.payload && "text" in ev.payload ? ` ${JSON.stringify(ev.payload.text)}` : "";
	console.log(`[t=${t}] h=${hid} ${ev.type}${txt}`);
	if (!phase) return;
	phase.events.push({ t, hid, ev });
	if (ev.type === "token") phase.tokens++;
	if (ev.type === "completion_ended") {
		phase.ended++;
		if (phase.ended >= phase.target) {
			const p = phase;
			phase = null;
			// Defer so the synchronous Go->JS stack unwinds first.
			setImmediate(() => p.resolve(p));
		}
	}
};

// waitBurst(target) resolves when `target` completion_ended events have been
// seen. The OpenAI-client burst yields exactly ONE completion_ended (a full OR
// a stopped burst — the session appends its own on stream end).
function waitBurst(target = 1) {
	const p = { target, ended: 0, events: [], tokens: 0, resolve: null };
	const done = new Promise((res) => (p.resolve = res));
	if (phase) console.error("WARN: waitBurst called while a burst is already tracked");
	phase = p;
	setTimeout(() => {
		if (phase === p) {
			console.log("  !! burst wait timed out");
			phase = null;
			p.resolve(p);
		}
	}, 15000);
	return done;
}

// ---------------------------------------------------------------------------
// Check collector + dumpMessages helper (string result via __cyrDump global)
// ---------------------------------------------------------------------------
const okCount = { pass: 0, fail: 0 };
function check(name, cond, detail) {
	const line = `${cond ? "PASS" : "FAIL"}: ${name}${cond ? "" : " — " + detail}`;
	console.log(line);
	if (cond) okCount.pass++;
	else okCount.fail++;
}
function dump(h) {
	instance.exports.dumpMessages(h);
	instance.exports.resume(); // kick after the dump export (avoids two-exports-before-a-kick)
	const raw = globalThis.__cyrDump;
	if (raw === undefined || raw === null) return null;
	return JSON.parse(raw);
}
const payload = (dialogue, ids, userText, model) =>
	JSON.stringify({
		dialogue_id: dialogue,
		ids,
		user_text: userText,
		config: { useOpenAI: true, endpoint: ENDPOINT, apiKey: "mock-key", model },
	});

// ---------------------------------------------------------------------------
// Drive the owner
// ---------------------------------------------------------------------------
go.run(instance); // start Go runtime + main (blocks; not awaited)
instance.exports.setCallback(); // AFTER go.run: wasmexport calls fail before runtime init

// ============ S1: goStart(d1, [a,b,c], "turn1") =============================
console.log("\n================ S1: goStart(d1, [a,b,c], turn1) ================");
globalThis.__cyrPayload = payload("d1", ["a", "b", "c"], "turn1", "mock-plain");
const h1 = instance.exports.goStart();
console.log("goStart returned handle:", h1);
instance.exports.resume(); // one kick per burst
await waitBurst(1);

check(
	"S1 getMessages called EXACTLY ONCE with the full id-list",
	pullCalls.length === 1,
	`got ${pullCalls.length} calls`
);
check(
	"S1 batch = [a,b,c] in order (NOT 3 separate calls)",
	pullCalls.length === 1 && JSON.stringify(pullCalls[0].ids) === JSON.stringify(["a", "b", "c"]),
	JSON.stringify(pullCalls[0]?.ids ?? null)
);
check(
	"S1 routed to dialogue d1",
	pullCalls.length === 1 && pullCalls[0].dialogue === "d1",
	JSON.stringify(pullCalls[0]?.dialogue ?? null)
);

const d1a = dump(h1);
const x1 = (d1a ?? []).map((e) => e.payload?.text);
const t1 = (d1a ?? []).map((e) => e.type);
check(
	"S1 dump: a,b,c pulled+parsed, in order, + the turn1 user message",
	t1[0] === "user_message" && x1[0] === "msg a: user opens the dialogue" &&
		t1[1] === "assistant_message" && x1[1] === "msg b: assistant responds" &&
		t1[2] === "user_message" && x1[2] === "msg c: user follows up" &&
		t1[3] === "user_message" && x1[3] === "turn1",
	JSON.stringify(x1)
);
check(
	"S1 burst appended the assistant response",
	t1[4] === "assistant_message" && x1[4] === MOCK_PLAIN,
	JSON.stringify(x1.slice(4))
);

// ============ S2: goSend(d1, [a,b,c,d], "turn2") — THE key C1 property =====
console.log("\n================ S2: goSend(d1, [a,b,c,d], turn2) ================");
globalThis.__cyrPayload = payload("d1", ["a", "b", "c", "d"], "turn2", "mock-plain");
instance.exports.goSend(h1);
instance.exports.resume();
await waitBurst(1);

check(
	"S2 getMessages called EXACTLY ONCE more, with [d] ONLY (a,b,c served from cache — no re-pull)",
	pullCalls.length === 2 && JSON.stringify(pullCalls[1].ids) === JSON.stringify(["d"]),
	`calls=${JSON.stringify(pullCalls.map((c) => c.ids))}`
);
check(
	"S2 routed to dialogue d1",
	pullCalls.length === 2 && pullCalls[1].dialogue === "d1",
	JSON.stringify(pullCalls[1]?.dialogue ?? null)
);

const d1b = dump(h1);
const x2 = (d1b ?? []).map((e) => e.payload?.text);
const t2 = (d1b ?? []).map((e) => e.type);
check(
	"S2 dump: a,b,c,turn1 history intact (NOT re-added from cache) + d + turn2 + new response",
	d1b.length === 8 &&
		x2[0] === "msg a: user opens the dialogue" &&
		x2[1] === "msg b: assistant responds" &&
		x2[2] === "msg c: user follows up" &&
		x2[3] === "turn1" &&
		t2[4] === "assistant_message" && x2[4] === MOCK_PLAIN &&
		t2[5] === "assistant_message" && x2[5] === "msg d: assistant responds again" &&
		t2[6] === "user_message" && x2[6] === "turn2" &&
		t2[7] === "assistant_message" && x2[7] === MOCK_PLAIN,
	JSON.stringify(x2)
);

// ============ S3 (edge, missing-id): id "e" has NO content =================
console.log("\n================ S3 (edge): goSend(d1, [a..e], turn3), e missing ================");
globalThis.__cyrPayload = payload("d1", ["a", "b", "c", "d", "e"], "turn3", "mock-plain");
instance.exports.goSend(h1);
instance.exports.resume();
await waitBurst(1);

check(
	"S3 getMessages called with [e] only (a-d served from cache)",
	pullCalls.length === 3 && JSON.stringify(pullCalls[2].ids) === JSON.stringify(["e"]),
	`calls=${JSON.stringify(pullCalls.map((c) => c.ids))}`
);
const d1c = dump(h1);
const x3 = (d1c ?? []).map((e) => e.payload?.text);
const t3 = (d1c ?? []).map((e) => e.type);
check(
	"S3 dump: e SKIPPED (no such event), turn3 user msg + full response appended",
	!x3.some((x) => String(x).startsWith("msg e")) &&
		x3.includes("turn3") &&
		t3.filter((tt) => tt === "user_message").length === 5 && // a,c + turn1..turn3
		d1c.length === 10 && // a,b,c,turn1,resp,d,turn2,resp,turn3,resp (e skipped)
		t3[9] === "assistant_message" && x3[9] === MOCK_PLAIN,
	JSON.stringify(x3)
);

// ============ S4 (per-dialogue isolation): fresh dialogue d2, same id "a" ===
console.log("\n================ S4: goStart(d2, [a], t1) — per-dialogue cache ================");
globalThis.__cyrPayload = payload("d2", ["a"], "t1", "mock-plain");
const h2 = instance.exports.goStart();
console.log("goStart(d2) returned handle:", h2);
instance.exports.resume();
await waitBurst(1);

check(
	"S4 getMessages called with [a] AGAIN for dialogue d2 (cache is per-dialogue)",
	pullCalls.length === 4 &&
		pullCalls[3].dialogue === "d2" &&
		JSON.stringify(pullCalls[3].ids) === JSON.stringify(["a"]),
	`calls=${JSON.stringify(pullCalls.map((c) => [c.dialogue, c.ids]))}`
);
const d2d = dump(h2);
const x4 = (d2d ?? []).map((e) => e.payload?.text);
check(
	"S4 dump (d2): a + t1 + response — independent of d1's history",
	x4.length === 3 && x4[0] === "msg a: user opens the dialogue" && x4[1] === "t1" && x4[2] === MOCK_PLAIN,
	JSON.stringify(x4)
);

// ============ S4b: follow-up on d2 — no new pull (a cached for d2) ==========
console.log("\n================ S4b: goSend(d2, [a], t2) — no re-pull ================");
globalThis.__cyrPayload = payload("d2", ["a"], "t2", "mock-plain");
instance.exports.goSend(h2);
instance.exports.resume();
await waitBurst(1);

check(
	"S4b no new getMessages call (a already cached for d2)",
	pullCalls.length === 4,
	`got ${pullCalls.length} calls`
);
const d2e = dump(h2);
const x4b = (d2e ?? []).map((e) => e.payload?.text);
check(
	"S4b dump (d2): a + t1 + response + t2 + response",
	x4b.length === 5 &&
		x4b[0] === "msg a: user opens the dialogue" &&
		x4b[1] === "t1" &&
		x4b[2] === MOCK_PLAIN &&
		x4b[3] === "t2" &&
		x4b[4] === MOCK_PLAIN,
	JSON.stringify(x4b)
);

// ============ S5 (per-burst ctx): stop mid-burst, handle survives ===========
console.log("\n================ S5: goStart(d3, [b], s1) slow + goStop mid-burst ================");
globalThis.__cyrPayload = payload("d3", ["b"], "s1", "mock-slow");
const h3 = instance.exports.goStart();
console.log("goStart(d3) returned handle:", h3);
instance.exports.resume(); // burst begins (~150ms per slow token)
const stopBurstP = waitBurst(1); // track from NOW (tokens stream during the sleep)
await new Promise((r) => setTimeout(r, 800)); // land mid-burst (before the last tokens)
instance.exports.goStop(h3);
const stopBurst = await stopBurstP; // a stopped burst yields exactly 1 completion_ended
check(
	`S5a stopped burst: 1 completion_ended, partial tokens (< ${SLOW_TOKENS})`,
	stopBurst.ended === 1 && stopBurst.tokens > 0 && stopBurst.tokens < SLOW_TOKENS,
	JSON.stringify({ ended: stopBurst.ended, tokens: stopBurst.tokens })
);

// Now the SAME handle must still run a FULL burst (per-burst ctx, not per-handle).
console.log("\n================ S5b: goSend(d3, [b], s2) — full follow-up ================");
globalThis.__cyrPayload = payload("d3", ["b"], "s2", "mock-plain");
instance.exports.goSend(h3);
instance.exports.resume();
const fullBurst = await waitBurst(1);
check(
	"S5b handle SURVIVES the cancel: full follow-up burst (all tokens, 1 completion_ended)",
	// NOTE: the handle's client is fixed at goStart (mock-slow) — goSend's
	// config is ignored by design, so the follow-up streams the SLOW model.
	fullBurst.ended === 1 && fullBurst.tokens === SCENARIOS["mock-slow"].tokens.length,
	JSON.stringify({ ended: fullBurst.ended, tokens: fullBurst.tokens })
);
const d3d = dump(h3);
const x5 = (d3d ?? []).map((e) => e.payload?.text);
check(
	"S5c dump (d3): b + s1 + [partial response] + s2 + full response in order",
	x5[0] === "msg b: assistant responds" &&
		x5[1] === "s1" &&
		// the stopped burst leaves a PARTIAL assistant message in history
		// (interlude appends whatever streamed before the cancel):
		x5[2] !== "" && x5[2] !== MOCK_SLOW && MOCK_SLOW.startsWith(x5[2]) &&
		x5[3] === "s2" &&
		x5[x5.length - 1] === MOCK_SLOW, // full response from the surviving handle
	JSON.stringify(x5)
);

// ============ whole-run consistency: no (dialogue,id) pulled twice ==========
console.log("\n================ whole-run pull consistency ================");
const d1counts = { a: 0, b: 0, c: 0, d: 0, e: 0 };
for (const [k, n] of Object.entries(pullCount)) {
	if (k.startsWith("d1:")) {
		const id = k.slice(3);
		if (id in d1counts) d1counts[id] = n;
	}
}
check(
	"no dialogue-d1 id pulled more than once across the run (parse-once)",
	d1counts.a === 1 && d1counts.b === 1 && d1counts.c === 1 && d1counts.d === 1 && d1counts.e === 1,
	JSON.stringify(d1counts)
);
check(
	"cross-dialogue isolation: a and b pulled once for d2/d3 too (cache is per-dialogue)",
	pullCount["d2:a"] === 1 && pullCount["d3:b"] === 1,
	JSON.stringify(pullCount)
);

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------
console.log("\n================ getMessages call log ================");
pullCalls.forEach((c, i) => console.log(`  call#${i}: dialogue=${c.dialogue} ids=${JSON.stringify(c.ids)}`));
console.log(`  per-id pull counts: ${JSON.stringify(pullCount)}`);
console.log("\n================ SUMMARY ================");
console.log(`${okCount.pass} passed, ${okCount.fail} failed`);
mock.kill();
process.exitCode = okCount.fail === 0 ? 0 : 1;
console.log(
	okCount.fail === 0 ? "\nPASS: C1 id-keyed parse-cache + batched getMessages pull (mock SSE bursts)" : "\nFAIL"
);
process.exit(okCount.fail === 0 ? 0 : 1);
