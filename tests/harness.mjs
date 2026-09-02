// P1/P2 root-cause verification harness (the startBurst path).
//
// Scenarios:
//   S1 (regression): echo burst (no provider → mock echo) streams
//       completion_start → thinking×3 → token×5 (in order) → completion_ended,
//       incrementally over >100ms.
//   S2 (P2): a burst with an UNREGISTERED provider → startBurst()/resume() do
//       NOT throw (the "'5' loadValue" return-frame corruption is gone:
//       startBurst is void and does no JS crossing in the export body),
//       EXACTLY ONE `error` event is emitted (naming the provider), NO
//       completion_ended, and a FOLLOW-UP echo burst still streams normally
//       (the module is alive, the return frame is not corrupted).
//   S3 (P1): a burst whose registered "openai" provider config carries a
//       NON-LATIN-1 api_key (Cyrillic) → no throw, no module exit, EXACTLY
//       ONE `error` event whose message mentions non-Latin-1, the burst still
//       terminates (completion_ended), and a FOLLOW-UP echo burst works.
//   S4 (stop regression): an echo burst with a slow tokenDelay is stop()ped
//       mid-stream → partial tokens (< full) + completion_ended; a follow-up
//       echo burst then streams fully.
//
// Usage: node harness.mjs [wasmPath]
import { createRequire } from "node:module";
import fs from "node:fs";
import path from "node:path";

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

const wasmPath = process.argv[2] || path.join(dir, "owner.wasm");
const go = new Go();
const bytes = fs.readFileSync(wasmPath);
const { instance } = await WebAssembly.instantiate(bytes, go.importObject);

// ---- Check collector ------------------------------------------------------
const okCount = { pass: 0, fail: 0 };
function check(name, cond, detail) {
	const line = `${cond ? "PASS" : "FAIL"}: ${name}${cond ? "" : " — " + detail}`;
	console.log(line);
	if (cond) okCount.pass++;
	else okCount.fail++;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---- Event sink: (evJSON) — the startBurst callback shape -----------------
let sink = null;
globalThis.__cyrCallback = (evJSON) => {
	const ev = JSON.parse(evJSON);
	sink?.({ t: Date.now(), ev });
};

// ---- Burst runner ---------------------------------------------------------
// runBurst(opts, {until, timeoutMs}): sets globalThis.__cyrOpts, calls
// startBurst() + the resume() kick, collects events until the first event of
// type `until` (or an `error` event), or timeout. The events array keeps
// collecting AFTER the resolve (same array reference), so late events (e.g. a
// completion_ended that must NOT come) are observable after the await.
// Returns { events, thrown } — thrown is non-null if startBurst()/resume()
// threw (the P2 "'5' loadValue" symptom).
function runBurst(opts, { until = "completion_ended", timeoutMs = 8000 } = {}) {
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
			if (!done && (ev.type === until || ev.type === "error")) {
				done = true;
				// Defer so the synchronous Go->JS stack unwinds first.
				setImmediate(() => resolve({ events, thrown: null }));
			}
		};
		globalThis.__cyrOpts = JSON.stringify(opts);
		const watchdog = setTimeout(() => {
			if (!done) {
				done = true;
				sink = null;
				resolve({ events, thrown: new Error(`timeout after ${timeoutMs}ms waiting for ${until}`) });
			}
		}, timeoutMs);
		try {
			instance.exports.startBurst(); // void — must not throw ('5' corruption)
			// One kick per burst: the export spawns the burst goroutine and
			// returns normally, so the caller must pump it explicitly.
			instance.exports.resume();
			if (done) clearTimeout(watchdog);
		} catch (e) {
			clearTimeout(watchdog);
			done = true;
			sink = null;
			resolve({ events, thrown: e });
			return;
		}
	});
}

// A clean echo burst (no provider → the HTTP-free echo client). The full
// content is asserted by S1; follow-ups only need "streams normally".
const ECHO_OPTS = { userText: "hello" };

function echoFullStats(events) {
	const types = events.map((e) => e.ev.type);
	const firstTokenIdx = types.indexOf("token");
	const thinkingBefore = types.slice(0, firstTokenIdx).filter((t) => t === "thinking").length;
	const tokenIdxs = types.map((t, i) => (t === "token" ? i : -1)).filter((i) => i >= 0);
	const firstEndedIdx = types.indexOf("completion_ended");
	const tokensConsecutive = tokenIdxs.length > 0 && tokenIdxs.every((i, k) => i === tokenIdxs[0] + k);
	const allThinkingBeforeTokens = types.slice(0, firstTokenIdx).every((t) => t === "thinking" || t === "completion_start");
	const minT = events[0]?.t ?? 0;
	const maxT = events[events.length - 1]?.t ?? 0;
	const spread = maxT - minT;
	return { types, thinkingBefore, tokenIdxs, firstTokenIdx, firstEndedIdx, tokensConsecutive, allThinkingBeforeTokens, spread, tokens: tokenIdxs.length };
}

function assertEchoBurst(name, r) {
	const events = r.events;
	check(`${name}: startBurst/resume did not throw`, r.thrown === null, r.thrown ? String(r.thrown) : "");
	if (!r.thrown && events[0]?.ev.type !== undefined) {
		const s = echoFullStats(events);
		check(
			`${name}: thinking×3 → token×5 in order → completion_ended`,
			s.thinkingBefore === 3 &&
				s.tokens === 5 &&
				s.tokensConsecutive &&
				s.allThinkingBeforeTokens &&
				s.firstEndedIdx > -1 &&
				s.firstEndedIdx > s.tokenIdxs[s.tokenIdxs.length - 1],
			JSON.stringify({ types: s.types, firstEndedIdx: s.firstEndedIdx })
		);
		check(`${name}: incremental (spread > 100ms)`, s.spread > 100, `${s.spread}ms`);
		const texts = events.filter((e) => e.ev.type === "token").map((e) => e.ev.payload.text);
		check(`${name}: echo text intact`, texts.join("") === "Hello world! This is the echo.", JSON.stringify(texts));
	}
	return events;
}

// ---- Drive the owner ------------------------------------------------------
go.run(instance); // start Go runtime + main (blocks; not awaited)
instance.exports.setCallback(); // AFTER go.run: wasmexport calls fail before runtime init

// ============ S1: echo regression (the original stage-1 validation) ========
console.log("\n================ S1: echo burst (regression) ================");
{
	const r = await runBurst(ECHO_OPTS);
	assertEchoBurst("S1", r);
}

// ============ S2 (P2): UNREGISTERED provider → ONE error, no '5' ===========
console.log("\n================ S2 (P2): unregistered provider ================");
{
	const BAD_PROVIDER = "definitely-not-registered";
	const r = await runBurst({ userText: "hello", provider: BAD_PROVIDER, config: {} }, { until: "error", timeoutMs: 6000 });
	check("S2: startBurst/resume did NOT throw (no '5' loadValue corruption)", r.thrown === null, r.thrown ? String(r.thrown) : "");
	await sleep(500); // let any (wrong) late events through
	const errs = r.events.filter((e) => e.ev.type === "error");
	const ended = r.events.filter((e) => e.ev.type === "completion_ended");
	check("S2: EXACTLY ONE error event", errs.length === 1, JSON.stringify(r.events.map((e) => e.ev.type)));
	check(
		"S2: error names the provider",
		errs.length === 1 && String(errs[0].ev.payload?.error ?? "").includes(BAD_PROVIDER),
		JSON.stringify(errs[0]?.ev.payload ?? null)
	);
	check("S2: NO completion_ended (session never ran)", ended.length === 0, JSON.stringify(r.events.map((e) => e.ev.type)));

	// Liveness: a FOLLOW-UP echo burst must stream normally (proves the
	// wrapper's return frame / module state is intact after the error path).
	console.log("\n---------------- S2b: follow-up echo (liveness) ----------------");
	const r2 = await runBurst(ECHO_OPTS);
	assertEchoBurst("S2b", r2);
}

// ============ S3 (P1): non-Latin-1 api_key → ONE error, clean exit =========
console.log("\n================ S3 (P1): non-Latin-1 api_key ================");
{
	// endpoint points at a dead localhost port: if (contrary to the fix) a
	// request were attempted, it would fail locally — never a live endpoint.
	const r = await runBurst(
		{
			userText: "hello",
			provider: "openai",
			config: {
				conn: {
					endpoint: "http://127.0.0.1:9/v1",
					api_key: "секретный-ключ-123",
					model: "gpt-4o",
				},
			},
		},
		{ until: "completion_ended", timeoutMs: 8000 }
	);
	check("S3: startBurst/resume did NOT throw and the module did not exit", r.thrown === null, r.thrown ? String(r.thrown) : "");
	await sleep(500);
	const errs = r.events.filter((e) => e.ev.type === "error");
	const ended = r.events.filter((e) => e.ev.type === "completion_ended");
	check("S3: EXACTLY ONE error event", errs.length === 1, JSON.stringify(r.events.map((e) => e.ev.type)));
	check(
		"S3: error message mentions the key/encoding (non-Latin-1)",
		errs.length === 1 && /API key.*non-Latin-1/i.test(String(errs[0].ev.payload?.error ?? "")),
		JSON.stringify(errs[0]?.ev.payload ?? null)
	);
	check("S3: burst still terminates (completion_ended)", ended.length === 1, JSON.stringify(r.events.map((e) => e.ev.type)));

	// Liveness: a FOLLOW-UP echo burst must stream normally.
	console.log("\n---------------- S3b: follow-up echo (liveness) ----------------");
	const r2 = await runBurst(ECHO_OPTS);
	assertEchoBurst("S3b", r2);
}

// ============ S4: stop() still cancels an in-flight burst ===================
console.log("\n================ S4: stop() mid-burst (regression) ================");
{
	const SLOW = { userText: "hello", tokenDelayMs: 60 }; // 8 events × 60ms ≈ 480ms
	const rP = runBurst(SLOW, { until: "completion_ended", timeoutMs: 8000 });
	await sleep(250); // land mid-burst (≈4 events in: 3 thinking + ~1 token)
	instance.exports.stop(); // cancels the global per-burst cancel
	const r = await rP; // resolves when the (post-stop) completion_ended arrives
	await sleep(300); // settle window for any late events
	const events = r.events;
	const types = events.map((e) => e.ev.type);
	const tokens = events.filter((e) => e.ev.type === "token").length;
	const errs = events.filter((e) => e.ev.type === "error").length;
	check("S4: stop did not throw and no error events", errs === 0, JSON.stringify(types));
	check(
		"S4: stopped burst → partial tokens (<5) + completion_ended",
		types.includes("completion_ended") && tokens > 0 && tokens < 5,
		JSON.stringify({ types, tokens })
	);

	// Liveness: the module (and a fresh per-burst cancel) still works.
	console.log("\n---------------- S4b: follow-up echo (liveness) ----------------");
	const r2 = await runBurst(ECHO_OPTS);
	assertEchoBurst("S4b", r2);
}

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------
console.log("\n================ SUMMARY ================");
console.log(`${okCount.pass} passed, ${okCount.fail} failed`);
console.log(
	okCount.fail === 0
		? "\nPASS: P2 (void startBurst, no export-body JS crossing) + P1 (non-Latin-1 key → EventError) + echo/stop regressions"
		: "\nFAIL"
);
process.exit(okCount.fail === 0 ? 0 : 1);
