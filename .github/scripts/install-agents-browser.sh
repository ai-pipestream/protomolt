#!/usr/bin/env bash
# Install the same pinned Chromium build for both native and clean-bundle tests.
set -euo pipefail
test -n "${RUNNER_TEMP:-}" && test -n "${GITHUB_ENV:-}"
INSTALL_DIR="$RUNNER_TEMP/protomolt-agents-playwright"
npm install --prefix "$INSTALL_DIR" --no-save --package-lock=false playwright@1.63.0
"$INSTALL_DIR/node_modules/.bin/playwright" install --with-deps chromium
CHROME_BIN=$(node -e 'console.log(require(process.argv[1]).chromium.executablePath())' \
  "$INSTALL_DIR/node_modules/playwright")
test -x "$CHROME_BIN"
printf 'CHROME_BIN=%s\n' "$CHROME_BIN" >> "$GITHUB_ENV"
"$CHROME_BIN" --version
