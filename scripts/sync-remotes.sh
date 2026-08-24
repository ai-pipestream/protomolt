#!/usr/bin/env bash
# Sync the forgejo and github mains of this repository without ever
# rewriting either one.
#
# The relationship between the two remotes at any moment is one of four
# states, and each state has exactly one safe move:
#
#   identical      -> nothing to do
#   github ahead   -> forgejo is an ancestor; fast-forward forgejo main
#   forgejo ahead  -> github main is protected (checks required), so the
#                     commits arrive through a sync branch plus PR; the run
#                     after that PR merges fast-forwards forgejo
#   diverged       -> merge both tips on a merge branch and open a PR; a
#                     conflict fails loudly instead of picking a side
#
# Invariants this script keeps:
#   - main on either remote is never force-pushed. Forgejo main only ever
#     receives fast-forwards; github main only ever changes through a PR.
#   - a divergence is always preserved as a merge commit with both parents,
#     so no commit on either remote is ever dropped.
#
# Required environment:
#   FORGEJO_REMOTE     the forgejo repository: a remote name, or a URL with
#                      credentials embedded by the caller (the workflow passes
#                      a URL; a working clone can pass the remote name)
#   GITHUB_REMOTE      name of the github remote, default "origin". The CI job
#                      clones from github so origin is correct there. A local
#                      worktree of this project has it the other way round,
#                      with origin pointing at canonical forgejo, and must say
#                      so: GITHUB_REMOTE=github FORGEJO_REMOTE=origin.
#   GH_TOKEN           token for gh (must be a PAT: PRs opened with the
#                      ambient GITHUB_TOKEN do not trigger CI)

set -euo pipefail

SYNC_BRANCH="sync/forgejo-main"
MERGE_BRANCH="sync/merge-remotes"
GITHUB_REMOTE="${GITHUB_REMOTE:-origin}"

git fetch "$GITHUB_REMOTE" main
GITHUB_MAIN=$(git rev-parse FETCH_HEAD)
git fetch "$FORGEJO_REMOTE" main
FORGEJO_MAIN=$(git rev-parse FETCH_HEAD)

if [ "$GITHUB_MAIN" = "$FORGEJO_MAIN" ]; then
  echo "remotes already in sync at $GITHUB_MAIN"
  exit 0
fi

if git merge-base --is-ancestor "$FORGEJO_MAIN" "$GITHUB_MAIN"; then
  echo "github is ahead; fast-forwarding forgejo main to $GITHUB_MAIN"
  git push "$FORGEJO_REMOTE" "$GITHUB_MAIN:refs/heads/main"
  exit 0
fi

if git merge-base --is-ancestor "$GITHUB_MAIN" "$FORGEJO_MAIN"; then
  echo "forgejo is ahead; routing $FORGEJO_MAIN to github through a sync PR"
  BRANCH="$SYNC_BRANCH"
  TIP="$FORGEJO_MAIN"
else
  echo "mains have diverged; building a merge branch"
  BRANCH="$MERGE_BRANCH"
  TIP="$GITHUB_MAIN"
fi

# The job owns the clone, so switching branches is safe.
git switch -C "$BRANCH" "$TIP"
if [ "$BRANCH" = "$MERGE_BRANCH" ]; then
  if ! git merge --no-ff --no-edit "$FORGEJO_MAIN"; then
    echo "CONFLICT: forgejo and github mains diverged and do not merge cleanly." >&2
    echo "Resolve by hand; this script never picks a side." >&2
    exit 1
  fi
fi

# Sync branches are scratch space owned by this automation and are reborn on
# every run, so a plain force push is correct here. The lease form is not
# usable because the run only fetches main. Main itself is never pushed.
git push --force "$GITHUB_REMOTE" "$BRANCH:refs/heads/$BRANCH"

if gh pr list --head "$BRANCH" --state open --json number --jq '.[0].number' | grep -q .; then
  echo "sync PR for $BRANCH already open"
else
  gh pr create \
    --base main \
    --head "$BRANCH" \
    --title "Sync remotes: $BRANCH" \
    --body "Automated remote sync. Mains are never force-pushed; see scripts/sync-remotes.sh. Merge when checks are green." \
    || echo "PR creation failed; the branch is pushed, open the PR by hand"
fi
