# AGENTS.md

## Start here: reuse and scope control

- Removed repository design docs and evidence (tag docs-archive/2026-10-14) are
  superseded. Do not restore or cite them;
  docs/design/parts-based-repository-requirements.md is the only repository design source.
- Before implementation, read the current user authorization and these three
  documents: [repository requirements](docs/design/parts-based-repository-requirements.md),
  [toolkit audit](docs/design/protobuf-toolkit-audit.md), and the relevant entries
  in the [source index](docs/design/protobuf-toolkit-source-index.md).
  Verify their findings against the checkout; they are not proof that fixes landed.
- Identify the branch, worktree, starting commit and existing local changes.
  During this reconciliation, work only in `reconcile/parts-based-repository`.
  Leave `main` and `ai-slop-archive-by-astra` untouched. Do not resume old goals,
  automations or repair branches as implied authorization.
- For each implementation slice, record the required behavior, existing classes
  reused, entry-to-storage/read call path, and acceptance assertions before edits.
  Search callers and tests before inventing a mapper, validator, schema resolver,
  codec, receipt format, persistence layer or service. If an existing component
  is insufficient, name the specific missing behavior; do not silently bypass it.
- Use descriptor/Message/Builder APIs, the existing CEL mapper/projection tools,
  ProtoValidator, parts layout and retained schema/receipt primitives. Internal
  protobuf -> JSON/Struct -> protobuf is not a convenience implementation of
  those capabilities. An intentional Struct field or external JSON boundary is
  not a license to convert an unrelated typed message into it.
- Stop the affected slice and explain the concrete tradeoff before changing an
  approved architecture: another public document API, new authoritative format,
  document size ceiling, weaker validation, changed part semantics, or loss of
  retained identity. Obtain explicit user agreement for such a design change.
  Ordinary implementation choices within the approved design do not need approval.
- Existing broken behavior is not precedent. Do not copy a JSON persistence path,
  swallowed exception, implicit schema/provider fallback or buffered size limit
  merely because another module uses it. Preserve causes and distinguish missing,
  unavailable, invalid and opaque data. Never turn one into successful validation.
- Evidence must exercise the actual public path being changed. Assert fidelity,
  invalid-input refusal and failure/recovery outcomes, not just successful returns.
  Use real providers for claims about provider identity, transactions and durability;
  mocks cannot establish those claims. Do not weaken tests, skip failures or substitute
  a narrower path to obtain green. Report pre-existing failures separately.
- At each completed slice, report behavior changed, reused components, meaningful
  checks and remaining limitations. Distinguish local edits, commit, push, merge
  and deployment. Keep the scope small enough to review before starting another.
- Every delegated handoff or restart must carry these rules, exact branch/base,
  owned files, exclusions and exit criteria. A worker report is evidence to review,
  not approval to merge. Do not hand the user an unbounded review burden.
- The current audit/planning authorization does not authorize implementing R1-R9.
  Wait for a subsequent implementation instruction; this file does not grant it.

## Repository architecture and data integrity

- `DocumentService` is the canonical public document API. Preserve selected-part
  saves, copy-forward of untouched parts, selected chunk sets and selective reads.
  Do not replace these behaviors with a complete-replacement publication API.
- The separate `DocumentPublicationService` transport and its remote client are
  removed in this reconciliation. Internal publication, admission, receipts,
  revision history and recovery machinery remain distinct from that public API.
  Removing a transport does not authorize deleting those protections.
- Retained `DocumentPublicationRepository` is an internal integration and
  qualification boundary, not an alternative application-facing document API.
  Do not expose it again or recommend it to callers because it exists in code.
- Structured content remains protobuf through descriptor-based mapping,
  projection, CEL validation and repository storage. Keep original source files
  as bytes and large source content behind claim checks. SQL owns ledger state,
  references and simple metadata lookups; richer search belongs to search.
- JSON is permitted at actual external boundaries and for metadata lookup
  projections in SQL. A projection must not replace authoritative protobuf
  content, erase unknown fields, or become a JSON round trip for validation.
- JSON front ends (REST gateway, MCP, ACP, A2A) are edge adapters generated
  from proto descriptors, never transports between our own services. A2A streams
  well but is JSON-RPC over HTTP with JSON payloads by design; its gRPC binding
  is newer and less widely used. Internal calls stay protobuf over gRPC. Any
  internal doc for a JSON-facing surface repeats this boundary.
- Preserve parts, field presence, unknown fields, exact numeric values, retained
  schema identities, Any bytes and validation annotations. Opaque storage does
  not establish typed validation. Do not silently use a newer schema for history.
- Reuse existing streaming and storage-provider capabilities. Document size,
  transport message size and control-record budgets are separate. Do not invent
  a document ceiling from a buffered adapter's limit or claim large-object
  support by increasing a constant alone.
- Propagate failures with their causes; never drop failed records, swallow errors,
  return fake success or change credentials/providers as an implicit fallback.
  Record unverified behavior explicitly instead of claiming completion.
- Trace entry, storage, read and recovery paths before claiming fidelity. Tests
  must assert the required behavior; a green suite alone proves no architecture.
  Do not claim the canonical parts API gained typed admission unless its actual
  call path and tests establish it. Historical evidence is not a current API guide.

## Remotes and push policy

- **Push Forgejo first, GitHub second.** Forgejo
  (`git.rokkon.com/ai-pipestream/protomolt`, remote `origin`) is the master
  build; GitHub (remote `github`) is the public copy. Push branches to both,
  in that order. Main is different: it moves on Forgejo through a pull
  request and is forwarded to GitHub by `.forgejo/workflows/forward-main-to-github.yml`.
- **Main is forwarded, not merged on GitHub.** The forward opens a GitHub
  pull request only so the required checks run on the exact Forgejo commit,
  then fast-forwards GitHub main to it and closes that pull request. It does
  not force. Do not merge pull requests into GitHub main; a merge commit made
  on GitHub puts the mains on different commits.
- **Work that landed on GitHub first is kept.**
  `.forgejo/workflows/github-ancestor-guard.yml` fails a Forgejo pull request to
  main when the head does not already contain GitHub main, listing the missing
  commits; the forward halts the same way. Merge GitHub main into the branch
  and the guard accepts it. Both workflows read the `GH_FORWARD_TOKEN`
  repository secret (Forgejo does not allow the `GITHUB_` prefix), a fine-grained
  GitHub token with Contents and Pull requests write, Checks and Administration read.
- This repo was GitHub-canonical until 2026-08-01 (the forgejo repo used to
  be a read-only pull mirror of GitHub). The mirror was deleted and the repo
  recreated as a normal repo; forgejo is now the source of truth. If a
  checkout still has the old remote layout (`origin` = github,
  `forgejo` = forgejo), fix it with:
  `git remote rename origin github && git remote rename forgejo origin`.

## CI layout (deliberate, 2026-08-02)

- **GitHub Actions is the build of record** (`.github/workflows/ci.yml`:
  build 21/25, conformance, console, integration — branch protection
  requires all of them; the forward workflow attaches them to each Forgejo
  main commit before GitHub main moves).
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
dashes. Its Java package is `ai.protomolt.proto.` plus those same segments,
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
| intake/service | protomolt-intake-service | ai.protomolt.proto.intake.service |
| host/config-registry | protomolt-config-registry | ai.protomolt.proto.config.registry |

Proto packages are `ai.protomolt.proto.<domain>[.<sub>].v1` and always carry
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
at `ai.protomolt.proto.acquire.gather`, and `transform/mapper/core` is
`protomolt-mapper` at `ai.protomolt.proto.mapper`.

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
| apps/record-verifier | package `ai.protomolt.receipt.verify` | zero runtime coupling to the platform is the point |
| protobuf/validation-protovalidate | `buf.validate` proto package | vendored upstream vocabulary |
| protobuf/seo | schema.org words such as Recipe | external vocabulary, exempt from ADR-001 retirement |
| bom, samples, system-tests | excluded from the published BOM | build-only projects, never published |
| host/integration/quarkus | runtime module named protomolt-integration-quarkus | the Quarkus extension convention pairs artifact with artifact-deployment |
| search/index/spi | proto package ai.protomolt.proto.index.hints.v1 | wire-frozen dialect; schemas in the wild reference the extension |
| transform/workflow/authoring | generated proto/Java package ai.protomolt.proto.samples.starter.v1 for workflow_deliverable.proto and workflow_launch.proto | extracted starter contracts retain descriptor names, import paths and stored Any type URLs; handwritten Java uses ai.protomolt.proto.workflow.authoring |

The parse/document and parse/grparse protos are canonical here and conform;
fleet repos re-vendor them from this repo, byte-identical.

### Grandfathered names

The table is empty: the pre-1.0 breaking batch is done. A name that does
not conform is a defect, not a candidate for a new grandfather row.

`protomolt-search-service` conforms; search is a domain tree, so its name is
not a candidate for renaming.

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
- krick-1 also runs an OpenVINO Model Server embeddings sidecar
  (`deploy/krick-1/compose.embeddings.yml`, loopback port 8091) whose
  embedding models share the same B70 with the Muse Glimmer sidecar.
- `https://protomolt.rokkon.com/console/tasks` is the bounded task console.
  Its login credential is separate from the ProtoMolt API token. Do not print,
  commit, or copy either token into task messages.
- Durable Codex-to-Kimi delegation, worker reconnect, provider-session resume,
  task messages, checkpoints, review, and acceptance have live proof.
- A persistent authenticated Codex worker and the reverse Kimi-to-Codex live
  path are not yet deployed and proven. Do not report symmetric collaboration
  until both exist and pass a live task.
- The Java and C++ coding worker images are published for amd64 and arm64. The
 krick and krick-1 Compose files pull
 `ghcr.io/ai-pipestream/protomolt-worker-java:edge` (overridable through
 `PROTOMOLT_WORKER_IMAGE`); neither host builds `protomolt-agent-host:local`
 anymore.
- Nano1 is a trusted, manual-only native ARM64 GitHub runner. Its Docker group
  authority belongs only to the runner service. Never select its labels from a
  pull-request workflow or expose its Docker socket to a coding worker.
- krick runs a TEI embeddings sidecar on its NVIDIA GPU
  (`deploy/krick/compose.embeddings.yml`, loopback port 8096) serving pinned
  `BAAI/bge-m3` for 1024-dimensional embeddings. It replaces the retired DJL
  Serving embedder, whose Rust engine has no working CUDA path. It binds to
  loopback only; do not expose its unauthenticated port to the Internet.
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
