#!/usr/bin/env bash
# Self-test for the main-forward check gate. No network and no git writes.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
# shellcheck disable=SC1091
source "$here/forward-main-checks.sh"

failures=0
assert_eq() {
  local label=$1 expected=$2 actual=$3
  if [ "$expected" != "$actual" ]; then
    echo "FAIL ${label}" >&2
    echo "  expected: ${expected}" >&2
    echo "  actual:   ${actual}" >&2
    failures=$((failures + 1))
  else
    echo "ok ${label}"
  fi
}

assert_fail() {
  local label=$1
  shift
  local err
  if err=$("$@" 2>&1); then
    echo "FAIL ${label}: expected a failed policy read" >&2
    failures=$((failures + 1))
  else
    echo "ok ${label}"
    printf '%s\n' "$err" | grep -q 'GitHub main was not moved' || {
      echo "FAIL ${label}: missing explicit stop" >&2
      failures=$((failures + 1))
    }
  fi
}

documented=$(documented_ci_contexts)
absent=$(required_contexts 404 '{"message":"Required status checks not enabled","status":"404"}' 200 '[]')
assert_eq "absent protection keeps the documented contexts" "$documented" "$absent"

empty_policy=$(required_contexts 200 '{"strict":false,"contexts":[],"checks":[]}' 200 '[]')
assert_eq "empty protection is not an empty policy" "$documented" "$empty_policy"

extra=$(required_contexts 200 '{"contexts":["lint"],"checks":[{"context":"build (25)"}]}' 200 '[
  {"type":"required_status_checks","parameters":{"required_status_checks":[{"context":"ruleset-check"}],"contexts":["legacy-check"]}}
]')
assert_eq "configured contexts are added" "$(printf '%s\n' "$documented" lint ruleset-check legacy-check)" "$extra"

assert_fail "auth failure on protection" required_contexts 401 '{"message":"Bad credentials"}' 200 '[]'
assert_fail "auth failure on rules" required_contexts 404 '{"message":"Required status checks not enabled"}' 403 '{"message":"Resource not accessible"}'
assert_fail "transport failure" required_contexts 000 '' 200 '[]'
assert_fail "protection server error" required_contexts 500 '{"message":"oops"}' 200 '[]'
assert_fail "rules server error" required_contexts 404 '{"message":"Required status checks not enabled"}' 502 '<html>'
assert_fail "malformed protection" required_contexts 200 '{"message":"not a policy"}' 200 '[]'

success_runs='{"check_runs":[
  {"name":"build (25)","status":"completed","conclusion":"success","started_at":"2026-10-23T00:00:00Z"},
  {"name":"console","status":"completed","conclusion":"success","started_at":"2026-10-23T00:00:00Z"},
  {"name":"conformance","status":"completed","conclusion":"neutral","started_at":"2026-10-23T00:00:00Z"},
  {"name":"integration","status":"completed","conclusion":"skipped","started_at":"2026-10-23T00:00:00Z"}
]}'
evaluate_required_checks "$success_runs" "$documented"
assert_eq "successful checks" "0|" "${pending_checks}|${failed_checks}"

pending_runs='{"check_runs":[
  {"name":"build (25)","status":"completed","conclusion":"success","started_at":"2026-10-23T00:00:00Z"},
  {"name":"console","status":"in_progress","conclusion":null,"started_at":"2026-10-23T00:00:01Z"},
  {"name":"conformance","status":"queued","conclusion":null,"started_at":"2026-10-23T00:00:01Z"}
]}'
evaluate_required_checks "$pending_runs" "$documented"
assert_eq "pending and missing checks" "3|" "${pending_checks}|${failed_checks}"

failed_runs='{"check_runs":[
  {"name":"build (25)","status":"completed","conclusion":"failure","started_at":"2026-10-23T00:00:00Z"},
  {"name":"build (25)","status":"completed","conclusion":"success","started_at":"2026-10-23T00:00:02Z"},
  {"name":"console","status":"completed","conclusion":"cancelled","started_at":"2026-10-23T00:00:00Z"},
  {"name":"conformance","status":"completed","conclusion":"success","started_at":"2026-10-23T00:00:00Z"},
  {"name":"integration","status":"completed","conclusion":"success","started_at":"2026-10-23T00:00:00Z"}
]}'
evaluate_required_checks "$failed_runs" "$documented"
assert_eq "latest failure is the one that counts" "0| console(cancelled)" "${pending_checks}|${failed_checks}"

script="$here/forward-main-to-github.sh"
helper="$here/forward-main-checks.sh"
grep -q 'require_github_main_ancestor' "$script"
grep -q 'refs/heads/main' "$script"
grep -q 'required_contexts' "$script"
grep -q 'evaluate_required_checks' "$script"
if grep -n '|| true' "$script" "$helper"; then
  echo "FAIL a forward script ignores a command failure" >&2
  failures=$((failures + 1))
else
  echo "ok forward scripts do not ignore a command failure"
fi
if grep -q -- '--force' "$script"; then
  echo "FAIL forward script force-pushes" >&2
  failures=$((failures + 1))
else
  echo "ok forward script does not force-push"
fi

if [ "$failures" -ne 0 ]; then
  echo "${failures} failed" >&2
  exit 1
fi
echo "all forward check tests passed"
