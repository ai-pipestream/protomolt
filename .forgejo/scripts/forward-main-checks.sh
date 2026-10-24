#!/usr/bin/env bash
# Check-policy helpers for the Forgejo-to-GitHub main forward.
#
# The documented CI contexts are always required. Contexts configured on GitHub
# branch protection or branch rules are required in addition. An absent
# protection configuration is not an empty policy, and an API or auth failure
# is not a successful read.
set -euo pipefail

# Job names from .github/workflows/ci.yml. The build job's matrix value is the
# check-run name GitHub records.
documented_ci_contexts() {
  printf '%s\n' 'build (25)' 'console' 'conformance' 'integration'
}

# Prints the contexts the forward must see succeed, one per line.
# Arguments: protection HTTP status, protection body, rules HTTP status, rules body.
# A status other than 200, or 404 for the protection endpoint only, is a failed read.
required_contexts() {
  local protection_status=$1 protection_body=$2 rules_status=$3 rules_body=$4
  local configured
  case "$protection_status" in
    200|404) ;;
    *)
      echo "Cannot read GitHub branch protection required status checks (HTTP ${protection_status}). GitHub main was not moved." >&2
      return 1
      ;;
  esac
  if [ "$rules_status" != 200 ]; then
    echo "Cannot read GitHub branch rules (HTTP ${rules_status}). GitHub main was not moved." >&2
    return 1
  fi
  if ! printf '%s' "$rules_body" | jq -e 'type == "array"' >/dev/null; then
    echo "GitHub branch rules response is not a rules array. GitHub main was not moved." >&2
    return 1
  fi
  if ! printf '%s' "$rules_body" | jq -e '
    all(.[]; type == "object")
    and all(.[] | select(.type == "required_status_checks");
      (.parameters | type == "object")
      and ((.parameters.required_status_checks // []) | type == "array"
        and all(.[]; type == "object" and (.context | type == "string")))
      and ((.parameters.contexts // []) | type == "array" and all(.[]; type == "string")))
  ' >/dev/null; then
    echo "GitHub branch rules status-check list is malformed. GitHub main was not moved." >&2
    return 1
  fi
  if [ "$protection_status" = 200 ]; then
    if ! printf '%s' "$protection_body" | jq -e '
      type == "object" and (has("contexts") or has("checks"))
      and ((.contexts // []) | type == "array" and all(.[]; type == "string"))
      and ((.checks // []) | type == "array"
        and all(.[]; type == "object" and (.context | type == "string")))
    ' >/dev/null; then
      echo "GitHub branch protection status-check list is malformed. GitHub main was not moved." >&2
      return 1
    fi
  fi
  if ! configured=$(
    {
      if [ "$protection_status" = 200 ]; then
        printf '%s' "$protection_body" | jq -r '.contexts // [] | .[]'
        printf '%s' "$protection_body" | jq -r '.checks // [] | .[].context'
      fi
      printf '%s' "$rules_body" | jq -r '
        .[] | select(.type == "required_status_checks") | .parameters
        | (.required_status_checks // [] | .[].context), (.contexts // [] | .[])'
    } | awk 'NF && !seen[$0]++'
  ); then
    echo "GitHub check policy could not be parsed. GitHub main was not moved." >&2
    return 1
  fi
  {
    documented_ci_contexts
    printf '%s\n' "$configured"
  } | awk 'NF && !seen[$0]++'
}

# Sets pending_checks and failed_checks from a check-runs JSON body and the
# context list on stdin (one name per line).
evaluate_required_checks() {
  local runs_body=$1 contexts=$2 context state
  pending_checks=0
  failed_checks=""
  if ! printf '%s' "$runs_body" | jq -e '.check_runs | type == "array"' >/dev/null; then
    echo "GitHub check-runs response has no check_runs array. GitHub main was not moved." >&2
    return 1
  fi
  while IFS= read -r context; do
    [ -n "$context" ] || continue
    state=$(printf '%s' "$runs_body" | jq -r --arg n "$context" '
      [.check_runs[] | select(.name == $n)] | sort_by(.started_at) | last
      | "\(.status // "missing")/\(.conclusion // "")"')
    case "$state" in
      completed/success|completed/neutral|completed/skipped) ;;
      completed/*) failed_checks="${failed_checks} ${context}(${state#completed/})" ;;
      *) pending_checks=$((pending_checks + 1)) ;;
    esac
  done <<< "$contexts"
}
