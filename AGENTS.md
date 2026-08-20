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

## Dependency updates (renovate)

Renovate runs on the Forgejo instance and opens its PRs there, but Forgejo
runs no build CI for this repo, so a renovate PR is never merged in place.
The flow: cherry-pick the renovate branches onto one rollup branch cut from
current main, verify locally (full build plus the console lockfile check),
open a GitHub PR (the build of record), merge on green, sync Forgejo main,
then close the Forgejo renovate PRs with a comment pointing at the rollup.
Renovate detects the updated dependencies on its next run either way.

## Vocabulary (ADR-001)

| Domain | Word |
|---|---|
| Orchestration definition, authored and compiled | workflow |
| Durable execution of one | run |
| In-process transform sequence, and its stages | pipeline, processor |
| LLM field and message guidance | instruction |
| Index field and analyzer definitions | mapping |
| Chunking configuration on a shape | chunking policy |
| Externally served component over a store or pipeline | service |
| A node's mounted capability family (PROTOMOLT_ROLES) | role |
| A check that admits or refuses at a boundary | gate |

One word per domain: recipe, chain, directive, index plan, and door are
retired.
A new noun comes from its domain's anchor vocabulary or is a genuinely new
word, never a synonym of a term above.

## Naming and structure (ADR-002)

A module's name is `protomolt-` plus its directory path segments joined with
dashes. Its Java package is `ai.pipestream.proto.` plus those same segments,
dots for dashes. The published Maven artifactId is the module name. One
module, one package; a package is never split across two modules.

The top-level tree word is kept or dropped by the tree's kind. Every
remaining segment is kept, in order.

| Kind | Top word | Trees |
|---|---|---|
| Domain | kept | protobuf, mesh, repo, account, jobs, inference, intake, parse, search, schema, acquire, metric |
| Capability | dropped | core, transform, sink, surface, host, apps |

| Path | Module | Package |
|---|---|---|
| intake/service | protomolt-intake-service | ai.pipestream.proto.intake.service |
| host/config-registry | protomolt-config-registry | ai.pipestream.proto.config.registry |

Proto packages are `ai.pipestream.proto.<domain>[.<sub>].v1` and always carry
the `proto` segment.

A directory matches its module and package stem exactly; where singular and
plural forms of a stem have both been in use, the singular wins
(`metric`, `host/server`). A stem that is plural everywhere it
appears (`formats`, `sources`, `types`) is one stem, not a violation.

### Layer words

| Word | Names |
|---|---|
| service | the served component (ADR-001) |
| server | a framework host adapter under `host/server`, and nothing else |
| proto | the wire-contract module |
| spi | the extension seam |
| core | the shared-implementation leaf inside a capability subtree |
| console | a browser-facing page server, deliberately a variant of service |

`server` never names a served component's directory or module.

A subtree's `core` directory holds its namesake module, named and packaged
without the core segment: `acquire/gather/core` is `protomolt-acquire-gather`
at `ai.pipestream.proto.acquire.gather`. `transform/mapper/core` predates
this and keeps `protomolt-mapper-core` in the grandfather table.

### Technical terms

| Term | Meaning |
|---|---|
| module | a Gradle subproject; capitalized in code, Composer's `ServiceModule`, the unit a role mounts |
| mount | the act, and the holder classes, of following a config-lane document into a live catalog |

Neither is a synonym for service or role.

### Named exceptions

These are correct as they stand. Do not "fix" them.

| Site | Exception | Reason |
|---|---|---|
| apps/record-verifier | package `ai.pipestream.receipt.verify` | zero runtime coupling to the platform is the point |
| protobuf/validation-protovalidate | `buf.validate` proto package | vendored upstream vocabulary |
| protobuf/seo | schema.org words such as Recipe | external vocabulary, exempt from ADR-001 retirement |
| bom, samples, system-tests | excluded from the published BOM | build-only projects, never published |
| host/integration/quarkus | runtime module named protomolt-integration-quarkus | the Quarkus extension convention pairs artifact with artifact-deployment |
| search/index/spi | proto package ai.pipestream.proto.index.hints.v1 | wire-frozen dialect; schemas in the wild reference the extension |

### Grandfathered names

The names below predate this rule. They are grandfathered, not license, and
carry their targets into the single pre-1.0 breaking batch.

| Current | Path | Target |
|---|---|---|
| protomolt-mapper-core | transform/mapper/core | protomolt-mapper, the namesake-core rule |

`protomolt-search-service` conforms; search is a domain tree, so its name is
not a candidate for renaming.

Packages drift separately from module names:

| Module | Current | Target |
|---|---|---|
| protomolt-parse-document | ai.pipestream.document.v1 | ai.pipestream.proto.parse.document.v1 |
| protomolt-parse-grparse | ai.pipestream.parse.v1 | ai.pipestream.proto.parse.grparse.v1 |

`protomolt_service.proto` under surface/grpc/service declares
`ai.pipestream.protomolt.v1`: no `proto` segment, and `protomolt` is not a
domain. Its domain is chosen in the breaking batch.

A new module conforms on creation. This table only shrinks.

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
