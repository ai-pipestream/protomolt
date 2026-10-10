#!/usr/bin/env bash
# Shared helpers for the GitHub forward and guard workflows.
#
# Forgejo is canonical: main moves here first and is forwarded to GitHub as a
# fast-forward, never a force push and never a merge commit made on GitHub.
# Forwarding must not clobber work that landed on GitHub first, so every caller
# checks that GitHub main is already an ancestor of the commit it is about to
# forward and fails loudly otherwise.
#
# Required environment:
#   GITHUB_FORWARD_TOKEN  token with contents:write on the GitHub repository
#   GITHUB_REPOSITORY_SLUG  owner/name on GitHub, for example ai-pipestream/protomolt
set -euo pipefail

: "${GITHUB_REPOSITORY_SLUG:?GITHUB_REPOSITORY_SLUG is required}"
if [ -z "${GITHUB_FORWARD_TOKEN:-}" ]; then
  echo "::error::secret GITHUB_FORWARD_TOKEN is not set; add it under the repository's Actions secrets" >&2
  exit 2
fi

GITHUB_URL="https://github.com/${GITHUB_REPOSITORY_SLUG}.git"
GITHUB_API="https://api.github.com/repos/${GITHUB_REPOSITORY_SLUG}"
# The token travels in a header git never echoes, not in the remote URL.
GIT_AUTH=(-c "http.https://github.com/.extraheader=AUTHORIZATION: basic $(printf 'x-access-token:%s' "$GITHUB_FORWARD_TOKEN" | base64 -w0)")

gh_api() { # method path [json-body-file]
  local method=$1 path=$2 body=${3:-}
  if [ -n "$body" ]; then
    curl -sS -X "$method" -H "Authorization: token $GITHUB_FORWARD_TOKEN" -H 'Accept: application/vnd.github+json' \
      -H 'Content-Type: application/json' --data @"$body" "${GITHUB_API}${path}"
  else
    curl -sS -X "$method" -H "Authorization: token $GITHUB_FORWARD_TOKEN" -H 'Accept: application/vnd.github+json' "${GITHUB_API}${path}"
  fi
}

# Fetches GitHub main into refs/remotes/github-main and prints its hash.
fetch_github_main() {
  git "${GIT_AUTH[@]}" fetch --quiet "$GITHUB_URL" '+refs/heads/main:refs/remotes/github-main/main'
  git rev-parse refs/remotes/github-main/main
}

# Succeeds when GitHub main is an ancestor of (or equal to) the given commit.
# Otherwise lists the GitHub commits the candidate lacks and fails.
require_github_main_ancestor() { # candidate-commit
  local candidate=$1 github_main
  github_main=$(fetch_github_main)
  if git merge-base --is-ancestor "$github_main" "$candidate"; then
    echo "GitHub main ${github_main:0:12} is contained in ${candidate:0:12}"
    return 0
  fi
  echo "::error::GitHub main ${github_main:0:12} has commits that ${candidate:0:12} does not contain. Forwarding would clobber them. Merge GitHub main into the branch first." >&2
  git log --oneline "${candidate}..${github_main}" >&2
  return 1
}
