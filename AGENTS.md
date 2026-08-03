# AGENTS.md

## Remotes and push policy

- **Push Forgejo first, GitHub second.** Forgejo
  (`git.rokkon.com/ai-pipestream/protomolt`, remote `origin`) is the master
  build; GitHub (remote `github`) is the public copy. Nothing auto-syncs
  between them — push both, in that order.
- This repo was GitHub-canonical until 2026-08-01 (the forgejo repo used to
  be a read-only pull mirror of GitHub). The mirror was deleted and the repo
  recreated as a normal repo; forgejo is now the source of truth. If a
  checkout still has the old remote layout (`origin` = github,
  `forgejo` = forgejo), fix it with:
  `git remote rename origin github && git remote rename forgejo origin`.

## CI layout (deliberate, 2026-08-02)

- **GitHub Actions is the build of record** (`.github/workflows/ci.yml`:
  build 21/25, conformance, console, integration — branch protection
  requires all five, so pushes to GitHub main go through a PR).
- **Forgejo runs no build CI.** Its two workflows are
  `publish-registry.yml` (snapshot publish to the instance's Maven
  registry — consumers like `knn-node` resolve `ai.pipestream.*` from it)
  and `tei-integration.yml`. Do not add a forgejo ci.yml; the user decided
  GitHub covers build verification.
- `publish-registry` uses the `REGISTRY_PUBLISH_TOKEN` repo secret: the
  ambient actions token cannot publish packages on this instance even with
  `permissions: packages: write` (that grant is in the workflow anyway as
  defense in depth). If the job starts failing on auth again, the token
  needs rotation, not a workflow change.
