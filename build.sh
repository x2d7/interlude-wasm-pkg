#!/usr/bin/env sh
# Build the owner wasm and stage the Go exec shim next to it.
set -eu

GOROOT="$(go env GOROOT)"

echo ">> GOOS=js GOARCH=wasm go build -o owner.wasm ."
GOOS=js GOARCH=wasm go build -o owner.wasm .

# The harness needs INTERACTIVE access to the exported Go functions
# (setCallback / startBurst / stop), so we use wasm_exec.js — the library that
# exposes the `Go` class on globalThis. wasm_exec_node.js is a fire-and-forget
# CLI runner (it reads the wasm path from argv and calls go.run, then exits)
# and does not give an interactive handle, so it is not used by the harness.
cp "$GOROOT/lib/wasm/wasm_exec.js" ./wasm_exec.js

echo ">> done. validate with: node harness.mjs"
