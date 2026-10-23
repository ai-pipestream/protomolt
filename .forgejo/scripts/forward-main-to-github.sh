#!/usr/bin/env bash
# Forwards the current Forgejo main head to GitHub main as a fast-forward.
#
# Steps, each failing loudly:
#   1. GitHub main must already be an ancestor of the head (no clobbering).
#   2. The head is pushed to a GitHub branch and a pull request is opened so the
#      required status checks run on this exact commit. Nothing is merged on
#      GitHub; the pull request exists only to attach check runs to the commit.
#   3. The documented CI contexts, plus any contexts configured on branch
#      protection or branch rules, must have a successful check run. A missing
#      protection configuration does not waive them. An API or auth failure
#      stops the forward. GitHub main is then pushed to the head without force.
#   4. The pull request is closed and the forward branch deleted.
#
# Environment: see github-main.sh. Optional:
#   FORWARD_WAIT_MINUTES   how long to wait for the checks (default 90)
#   FORWARD_POLL_SECONDS   polling interval (default 60)
set -euo pipefail
source "$(dirname "$0")/github-main.sh"
source "$(dirname "$0")/forward-main-checks.sh"

# Writes the response body to the given file and prints the HTTP status.
# A transport failure stops the forward; an HTTP error status is returned.
gh_http() { # outfile method path
  local out=$1 method=$2 path=$3 status
  status=$(curl -sS -o "$out" -w '%{http_code}' -X "$method" \
    -H "Authorization: token $GITHUB_FORWARD_TOKEN" \
    -H 'Accept: application/vnd.github+json' \
    "${GITHUB_API}${path}") || {
    echo "::error::GitHub API request failed (${method} ${path}). GitHub main was not moved." >&2
    exit 1
  }
  printf '%s' "$status"
}

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

protection_file=$(mktemp)
rules_file=$(mktemp)
protection_status=$(gh_http "$protection_file" GET /branches/main/protection/required_status_checks)
rules_status=$(gh_http "$rules_file" GET /rules/branches/main)
if ! contexts=$(required_contexts "$protection_status" "$(cat "$protection_file")" "$rules_status" "$(cat "$rules_file")"); then
  rm -f "$protection_file" "$rules_file"
  exit 1
fi
rm -f "$protection_file" "$rules_file"
if [ "$protection_status" = 404 ]; then
  echo "GitHub branch protection has no required status checks (HTTP 404). The documented CI contexts are still required."
fi
echo "Required checks:"
printf '%s\n' "$contexts" | sed 's/^/  /'
deadline=$(( $(date +%s) + wait_minutes * 60 ))
while :; do
  runs_file=$(mktemp)
  runs_status=$(gh_http "$runs_file" GET "/commits/${head}/check-runs?per_page=100")
  if [ "$runs_status" != 200 ]; then
    echo "::error::Cannot read check runs for ${short} (HTTP ${runs_status}). GitHub main was not moved; pull request #${pr_number} stays open." >&2
    rm -f "$runs_file"
    exit 1
  fi
  if ! evaluate_required_checks "$(cat "$runs_file")" "$contexts"; then
    rm -f "$runs_file"
    exit 1
  fi
  rm -f "$runs_file"
  if [ -n "$failed_checks" ]; then
    echo "::error::required checks failed on ${short}:${failed_checks}. GitHub main was not moved; pull request #${pr_number} stays open for inspection." >&2
    exit 1
  fi
  if [ "$pending_checks" -eq 0 ]; then
    echo "All required checks passed on ${short}"
    break
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "::error::required checks did not finish within ${wait_minutes} minutes on ${short}; pull request #${pr_number} stays open." >&2
    exit 1
  fi
  echo "${pending_checks} required check(s) still running; next poll in ${poll_seconds}s"
  sleep "$poll_seconds"
done

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
