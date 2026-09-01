#!/usr/bin/env node
/*
 * mock-sse.cjs — standalone mock OpenAI chat-completions SSE server.
 *
 * Endpoints (POST, JSON body):
 *   /v1/chat/completions
 *   /chat/completions
 * (the openai-go client appends "chat/completions" to the base URL, so the
 * wasm owner sets Endpoint = http://localhost:PORT/v1)
 *
 * The scenario is selected by the `model` field of the request body:
 *   mock-plain : ~10 short content chunks, finish_reason "stop"
 *   mock-tool  : tool_calls deltas split across 2 chunks (name get_weather,
 *                arguments split), finish_reason "tool_calls"
 *   mock-slow  : same shape as mock-plain but 150ms between chunks (~2s total)
 *
 * Every response is valid OpenAI chat-completion SSE: one
 * `data: <json>\n\n` per chunk, terminated by `data: [DONE]\n\n`.
 *
 * Usage:  node mock-sse.cjs [port]        (or PORT env var; default 9100)
 * Node core only — no dependencies. Reused later for Playwright E2E.
 */
"use strict";

const http = require("node:http");

// ---------------------------------------------------------------------------
// Scenario definitions (exported so harness2.mjs can assert exact content)
// ---------------------------------------------------------------------------

const SCENARIOS = {
	"mock-plain": {
		kind: "plain",
		delayMs: 0,
		tokens: ["Hello", " from", " the", " mock", " LLM", " running", " in", " Go", " wasm", "."],
	},
	"mock-tool": {
		kind: "tool",
		delayMs: 0,
		callId: "call_1",
		toolName: "get_weather",
		argsFirst: '{"ci', // arguments split across two chunks
		argsSecond: 'ty":"Moscow"}',
	},
	"mock-slow": {
		kind: "plain",
		delayMs: 150,
		tokens: ["Slow", " stream", " one", " chunk", " at", " a", " time", " ...", " over", " two", " seconds", "."],
	},
};

// ---------------------------------------------------------------------------
// SSE chunk builders
// ---------------------------------------------------------------------------

function baseChunk(model, ts) {
	return {
		id: "chatcmpl-mock",
		object: "chat.completion.chunk",
		created: ts,
		model,
	};
}

/**
 * Build the ordered list of SSE lines for a scenario.
 * Each item: { data: <json string of the chunk object | "[DONE]">, delayMs }
 */
function buildSse(model) {
	const scen = SCENARIOS[model];
	const ts = Math.floor(Date.now() / 1000);
	const lines = [];
	if (!scen) return null;

	const push = (choices, delayMs) => {
		lines.push({ data: JSON.stringify({ ...baseChunk(model, ts), choices }), delayMs });
	};

	if (scen.kind === "plain") {
		scen.tokens.forEach((tok, i) => {
			const delta = i === 0 ? { role: "assistant", content: tok } : { content: tok };
			push([{ index: 0, delta, logprobs: null, finish_reason: null }], scen.delayMs);
		});
		push([{ index: 0, delta: {}, logprobs: null, finish_reason: "stop" }], scen.delayMs);
	} else if (scen.kind === "tool") {
		// chunk 1: tool call begins (id + name + first args fragment)
		push(
			[
				{
					index: 0,
					delta: {
						role: "assistant",
						tool_calls: [
							{
								index: 0,
								id: scen.callId,
								type: "function",
								function: { name: scen.toolName, arguments: scen.argsFirst },
							},
						],
					},
					logprobs: null,
					finish_reason: null,
				},
			],
			scen.delayMs,
		);
		// chunk 2: arguments continuation (no id)
		push(
			[
				{
					index: 0,
					delta: {
						tool_calls: [{ index: 0, function: { arguments: scen.argsSecond } }],
					},
					logprobs: null,
					finish_reason: null,
				},
			],
			scen.delayMs,
		);
		// final chunk
		push([{ index: 0, delta: {}, logprobs: null, finish_reason: "tool_calls" }], scen.delayMs);
	}

	lines.push({ data: "[DONE]", delayMs: scen ? scen.delayMs : 0 });
	return lines;
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

function startServer(port) {
	const server = http.createServer((req, res) => {
		let body = "";
		req.on("data", (c) => (body += c));
		req.on("end", () => {
			let model, stream;
			try {
				const j = JSON.parse(body || "{}");
				model = j.model;
				stream = j.stream;
			} catch {}
			console.log(`[mock-sse] ${req.method} ${req.url} model=${model} stream=${stream}`);

			if (req.method !== "POST") {
				res.writeHead(405, { "Content-Type": "application/json" });
				res.end(JSON.stringify({ error: { message: "method not allowed" } }));
				return;
			}

			const lines = buildSse(model);
			if (!lines) {
				res.writeHead(400, { "Content-Type": "application/json" });
				res.end(
					JSON.stringify({
						error: { message: `unknown model "${model}" (want mock-plain | mock-tool | mock-slow)` },
					}),
				);
				return;
			}

			res.writeHead(200, {
				"Content-Type": "text/event-stream",
				"Cache-Control": "no-cache",
				Connection: "keep-alive",
			});

			let i = 0;
			const next = () => {
				if (i >= lines.length) {
					res.end();
					return;
				}
				const line = lines[i++];
				res.write(`data: ${line.data}\n\n`);
				setTimeout(next, line.delayMs);
			};
			next();
		});
	});

	return new Promise((resolve) => {
		server.listen(port, () => {
			console.log(`[mock-sse] listening on http://localhost:${port}`);
			resolve(server);
		});
	});
}

if (require.main === module) {
	const port = parseInt(process.env.PORT || process.argv[2], 10) || 9100;
	startServer(port);
}

module.exports = { SCENARIOS, buildSse, startServer };
