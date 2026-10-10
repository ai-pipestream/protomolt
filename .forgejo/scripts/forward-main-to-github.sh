#!/usr/bin/env bash
# Forwards the current Forgejo main head to GitHub main as a fast-forward.
#
# Steps, each failing loudly:
#   1. GitHub main must already be an ancestor of the head (no clobbering).
#   2. The head is pushed to a GitHub branch and a pull request is opened so the
#      required status checks run on this exact commit. Nothing is merged on
#      GitHub; the pull request exists only to attach check runs to the commit.
#   3. Once every required context has a successful check run, GitHub main is
#      pushed to the head without force. GitHub accepts the push because the
#      commit's required checks passed, so both remotes end on the same commit.
#   4. The pull request is closed and the forward branch deleted.
#
# Environment: see github-main.sh. Optional:
#   FORWARD_WAIT_MINUTES   how long to wait for the checks (default 90)
#   FORWARD_POLL_SECONDS   polling interval (default 60)
set -euo pipefail
source "$(dirname "$0")/github-main.sh"

head=$(git rev-parse HEAD)
short=${head:0:12}
wait_minutes=${FORWARD_WAIT_MINUTES:-90}
poll_seconds=${FORWARD_POLL_SECONDS:-60}

require_github_main_ancestor "$head"
github_main=$(git rev-parse refs/remotes/github-main/main)
if [ "$github_main" = "$head" ]; then
  echo "GitHub main already at ${short}; nothing to forward"
  exit 0
fi

branch="forward/${short}"
echo "Pushing ${short} to GitHub branch ${branch}"
git "${GIT_AUTH[@]}" push --quiet "$GITHUB_URL" "${head}:refs/heads/${branch}"

body_file=$(mktemp)
jq -n --arg title "Forward Forgejo main ${short} to GitHub main" --arg head "$branch" \
  --arg body "Automated forward from Forgejo main. This pull request only attaches the required check runs to commit ${head}; it is not merged. When the checks pass, GitHub main is fast-forwarded to the same commit and this pull request is closed." \
  '{title:$title, head:$head, base:"main", body:$body}' > "$body_file"
existing=$(gh_api GET "/pulls?state=open&head=${GITHUB_REPOSITORY_SLUG%%/*}:${branch}" | jq -r '.[0].number // empty')
if [ -n "$existing" ]; then
  pr_number=$existing
else
  pr_number=$(gh_api POST /pulls "$body_file" | jq -r '.number // empty')
fi
rm -f "$body_file"
if [ -z "$pr_number" ]; then
  echo "::error::could not open the forward pull request on GitHub" >&2
  exit 1
fi
echo "Forward pull request #${pr_number} open; waiting for required checks on ${short}"

required=$(gh_api GET /branches/main/protection/required_status_checks | jq -r '.contexts // [] | .[]' 2>/dev/null || true)
if [ -z "$required" ]; then
  echo "GitHub main requires no status checks"
else
  deadline=$(( $(date +%s) + wait_minutes * 60 ))
  while :; do
    runs=$(gh_api GET "/commits/${head}/check-runs?per_page=100")
    pending=0; failed=""
    while IFS= read -r context; do
      [ -n "$context" ] || continue
      state=$(printf '%s' "$runs" | jq -r --arg n "$context" '[.check_runs[] | select(.name==$n)] | sort_by(.started_at) | last | "\(.status // "missing")/\(.conclusion // "")"')
      case "$state" in
        completed/success) ;;
        completed/neutral|completed/skipped) ;;
        completed/*) failed="${failed} ${context}(${state#completed/})" ;;
        *) pending=$((pending + 1)) ;;
      esac
    done <<< "$required"
    if [ -n "$failed" ]; then
      echo "::error::required checks failed on ${short}:${failed}. GitHub main was not moved; pull request #${pr_number} stays open for inspection." >&2
      exit 1
    fi
    if [ "$pending" -eq 0 ]; then
      echo "All required checks passed on ${short}"
      break
    fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo "::error::required checks did not finish within ${wait_minutes} minutes on ${short}; pull request #${pr_number} stays open." >&2
      exit 1
    fi
    echo "${pending} required check(s) still running; next poll in ${poll_seconds}s"
    sleep "$poll_seconds"
  done
fi

echo "Fast-forwarding GitHub main to ${short}"
if ! git "${GIT_AUTH[@]}" push --quiet "$GITHUB_URL" "${head}:refs/heads/main"; then
  echo "::error::GitHub refused the fast-forward of main to ${short}. Nothing was forced. Inspect GitHub main and pull request #${pr_number}." >&2
  exit 1
fi

close_file=$(mktemp)
jq -n '{state:"closed"}' > "$close_file"
gh_api PATCH "/pulls/${pr_number}" "$close_file" > /dev/null
rm -f "$close_file"
git "${GIT_AUTH[@]}" push --quiet "$GITHUB_URL" ":refs/heads/${branch}" || echo "forward branch ${branch} was already gone"
echo "GitHub main is at ${short}; forward pull request #${pr_number} closed"
