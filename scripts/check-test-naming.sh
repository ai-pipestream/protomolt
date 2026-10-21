#!/usr/bin/env bash
# Keep Testcontainers-backed suites out of the fast test task. Inspect tracked
# and untracked Java sources, not ignored build outputs or container helpers.
set -euo pipefail
cd "$(dirname "$0")/.."

status=0
while IFS= read -r -d '' file; do
  [[ -f "$file" ]] || continue
  case "$file" in
    *IT.java|*IntegrationTest.java) continue ;;
  esac
  if grep -Eq '^[[:space:]]*import[[:space:]]+(static[[:space:]]+)?org[.]testcontainers[.]' "$file" &&
     grep -Eq '(^|[[:space:]])@(Test|ParameterizedTest|org[.]junit[.]jupiter[.]api[.]Test|org[.]junit[.]jupiter[.]params[.]ParameterizedTest)([[:space:](]|$)' "$file"; then
    printf 'FAIL: %s imports Testcontainers and declares a test; name it *IT.java or *IntegrationTest.java\n' "$file" >&2
    status=1
  fi
done < <(git ls-files --cached --others --exclude-standard -z -- ':(glob)**/src/test/**/*.java')

if (( status == 0 )); then
  printf 'Testcontainers test naming OK\n'
fi
exit "$status"
