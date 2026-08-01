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
