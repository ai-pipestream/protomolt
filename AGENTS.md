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

## Vocabulary (ADR-001)

| Domain | Word |
|---|---|
| Orchestration definition, authored and compiled | workflow |
| Durable execution of one | run |
| In-process transform sequence, and its stages | pipeline, processor |
| LLM field and message guidance | instruction |
| Index field and analyzer definitions | mapping |
| Chunking configuration on a shape | chunking policy |

One word per domain: recipe, chain, directive, and index plan are retired.
A new noun comes from its domain's anchor vocabulary or is a genuinely new
word, never a synonym of a term above.

## Agent collaboration and coding workers

Read these before changing the multi-agent runtime:

- `docs/apps/agent-host.md` defines provider processes, roles, cursor commits,
  and restart behavior.
- `docs/apps/coding-workers.md` defines the worker images, state ownership,
  credential boundaries, and current transport.
- `docs/apps/task-console.md` defines the browser guidance surface.
- `docs/transform/delegation.md` defines the durable task protocol.
- `deploy/portainer/README.md` owns the NAS coordinator topology.
- `deploy/krick-1/README.md` owns the Kimi and Muse Glimmer workstation stack.
- `deploy/workers/README.md` owns Java and C++ worker build and run commands.
- `deploy/nano1/README.md` owns the Jetson ARM64 build and inference node.

Current implementation and deployment snapshot, verified 2026-08-14:

- The always-on coordinator belongs on the NAS under Portainer. Its delegation
  transcript is encrypted before repository-service and RustFS storage.
- krick-1 runs the GPU-only Muse Glimmer inference sidecar plus Kimi and
  Glimmer workers. Do not enable CPU inference or CPU model offload. CPU-only
  build and test containers are allowed.
- `https://protomolt.rokkon.com/console/tasks` is the bounded task console.
  Its login credential is separate from the ProtoMolt API token. Do not print,
  commit, or copy either token into task messages.
- Durable Codex-to-Kimi delegation, worker reconnect, provider-session resume,
  task messages, checkpoints, review, and acceptance have live proof.
- A persistent authenticated Codex worker and the reverse Kimi-to-Codex live
  path are not yet deployed and proven. Do not report symmetric collaboration
  until both exist and pass a live task.
- The Java and C++ coding worker images are published for amd64 and arm64. The
  krick-1 Compose file still uses `protomolt-agent-host:local`; migrating it to
  a language image is separate deployment work.
- Nano1 is a trusted, manual-only native ARM64 GitHub runner. Its Docker group
  authority belongs only to the runner service. Never select its labels from a
  pull-request workflow or expose its Docker socket to a coding worker.
- Nano1 also runs a loopback-only, GPU-gated DJL Serving processor backed by
  TensorRT. Its startup and live checks execute a real CUDA engine on the Orin
  GPU. CPU inference and CPU model offload are prohibited. Build capacity and
  inference capacity must be advertised separately before either joins mesh
  scheduling.
- Nano1's TEI service uses a CUDA-only native ARM64 build and a pinned
  `BAAI/bge-small-en-v1.5` revision for 384-dimensional embeddings. It binds to
  loopback by default and may bind to Nano1's Tailscale address for tailnet
  access. Do not replace it with the published CPU ARM64 image, enable a CPU
  fallback, or expose its unauthenticated port to the Internet.

The durable control plane currently uses MCP streamable HTTP over TLS. gRPC is
the application plane for reflection, invocation, and generated clients. A
typed local gRPC sidecar and bounded workspace-execution API are planned but
not implemented. Never substitute a Docker socket or cluster-admin credential
for that API.

Before operating a live deployment, verify this dated snapshot against the
running containers and the checked-in Compose files. Preserve these ownership
boundaries: Portainer owns NAS services, `deploy/krick-1` owns workstation
inference and workers, and provider credentials enter only through runtime
mounts or secret injection.
