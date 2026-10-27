#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
# Explicit development-only compiler; no WASM compiler is shipped to the console.
npm exec --yes --package=wabt@1.0.39 -- wat2wasm type-url-preview.wat -o ../public/examples/type-url-preview.wasm
