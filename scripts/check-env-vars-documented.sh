#!/usr/bin/env bash
# Fails when main source reads an environment variable that is missing from
# docs/operations/environment-variables.md, naming each missing variable.
#
# Three extractions over */src/main/java, united:
#   1. ENV_* string constants:            String ENV_X = "NAME"
#   2. direct reads:                      System.getenv("NAME")
#   3. any quoted name in a known family: "DOCUMENT_PLATFORM_*", "PROTOMOLT_*",
#      "CONFLUENCE_*", "PARSE_GRPARSE_*", "PARSE_PLAYGROUND_*", "OPENAI_*"
#      (catches reads through helpers such as requiredEnv(...), envInt(...),
#      environment(...), env.get(...))
#
# The doc writes every variable name in backticks, so the presence check is
# exact, never a substring match.
set -euo pipefail
cd "$(dirname "$0")/.."

doc=docs/operations/environment-variables.md
if [ ! -f "$doc" ]; then
  echo "missing $doc" >&2
  exit 1
fi

main_dirs=$(find . -type d -path '*/src/main/java' -not -path './.git/*' -not -path '*/build/*')

names=$(
  {
    # shellcheck disable=SC2086
    grep -rhoE 'String ENV_[A-Z0-9_]+ *= *"[A-Z0-9_]+"' --include='*.java' $main_dirs \
      | grep -oE '"[A-Z0-9_]+"'
    # shellcheck disable=SC2086
    grep -rhoE 'System\.getenv\("[A-Z][A-Z0-9_]*"\)' --include='*.java' $main_dirs \
      | grep -oE '"[A-Z0-9_]+"'
    # shellcheck disable=SC2086
    grep -rhoE '"(DOCUMENT_PLATFORM|PROTOMOLT|CONFLUENCE|PARSE_GRPARSE|PARSE_PLAYGROUND|OPENAI)_[A-Z0-9_]*[A-Z0-9]"' \
      --include='*.java' $main_dirs
  } | tr -d '"' | sort -u
)

missing=0
total=0
for name in $names; do
  total=$((total + 1))
  if ! grep -qF "\`$name\`" "$doc"; then
    echo "undocumented environment variable: $name (add it to $doc)"
    missing=$((missing + 1))
  fi
done

if [ "$missing" -gt 0 ]; then
  echo "== FAIL: $missing of $total environment variables are missing from $doc"
  exit 1
fi
echo "== all $total environment variables read by main source are documented in $doc"
