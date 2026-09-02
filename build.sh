#!/usr/bin/env sh
# Build the owner wasm and stage the Go exec shim next to the test harnesses.
set -eu

GOROOT="$(go env GOROOT)"

echo ">> GOOS=js GOARCH=wasm go build -o tests/owner.wasm ."
GOOS=js GOARCH=wasm go build -o tests/owner.wasm .

# The harness needs INTERACTIVE access to the exported Go functions
# (setCallback / startBurst / stop), so we use wasm_exec.js — the library that
# exposes the `Go` class on globalThis. wasm_exec_node.js is a fire-and-forget
# CLI runner (it reads the wasm path from argv and calls go.run, then exits)
# and does not give an interactive handle, so it is not used by the harness.
cp "$GOROOT/lib/wasm/wasm_exec.js" ./tests/wasm_exec.js

echo ">> done. validate with:"
echo "   node tests/harness.mjs"
echo "   node tests/harness2.mjs"
echo "   node tests/harness3.mjs"
