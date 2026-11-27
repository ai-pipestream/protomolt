# Repository integration with Rust search

Plan, 2026-11-01. Investigated Rust `ai-pipestream/protomolt-search` at
`6968885a3ce92974bfa73886a1573ed49cdae3f3`; Java baseline
`7aa5f25266fa216fff1f8552d732ab4d67e654f1` plus this branch's Turboembed changes.
The network repository → gRParse → Turboembed → Rust-search path is **not yet
complete or qualified**. This plan preserves the canonical
[parts-based DocumentService](../design/parts-based-repository-requirements.md).
It does not introduce another repository document API or authoritative format.

## Existing seams to reuse

- [GrparseParserAdapter](../../parse/grparse/src/main/java/ai/protomolt/proto/parse/grparse/GrparseParserAdapter.java)
  bridges `ParserPluginService` to `ParseStreamingService.StreamProcessDocument`.
  It forwards page results, assembles typed parser output and preserves collector
  warnings and stream failures. Its input is buffered under an explicit adapter
  budget; this is not arbitrary-size streaming support. Existing fixture tests
  establish the adapter contract, not qualification of a live parser fleet.
- [Turboembed](../../search/embedding/turboembed/README.md) implements the existing
  embedding SPI using KServe protobuf RPCs. Document/query purpose, model revision,
  manifest digest, finite FP32 output and dimensions are explicit. The provider
  identity participates in the chunk policy digest. Provider live qualification
  is separate from repository/Rust-search qualification.
- [SearchWorkflows and the Java search service](service.md) already compose
  parse/index, delete/unindex and replay for the Java Lucene service. That service
  and Rust's `ai.protomolt.search.v1` APIs are different contracts. Existing Java
  workflow behavior does not establish revision-bound Rust publication.
- Reuse repository `DocumentEvent`, exact `RevisionSelector`, `ReadDocument`,
  `GetSchemaDescriptor`, retained occurrence admission, descriptor mapping and
  the existing durable jobs/outbox machinery. Do not add a JSON intermediary,
  schema-by-latest fallback or a second generic workflow engine.

## What Rust exposes at the investigated revision

The source links below are pinned to the investigated revision. Source takes
precedence over older status prose in `docs/network-ingest-authorization.md`.

| Boundary | Actual capability and limit |
|---|---|
| [Hosted service](https://git.rokkon.com/ai-pipestream/protomolt-search/src/commit/6968885a3ce92974bfa73886a1573ed49cdae3f3/src/main.rs) | `hosted_write_service` mounts `HostedDocumentWriteService` with `raft` + `tls`, bearer principals and explicitly configured, already-activated managed catalogs. It recovers catalogs; it does not provision ownership. |
| [DocumentWriteService](https://git.rokkon.com/ai-pipestream/protomolt-search/src/commit/6968885a3ce92974bfa73886a1573ed49cdae3f3/proto/ai/protomolt/search/v1/document_write.proto) | `GetWriteTarget` and `AcceptDocument` are ready RPCs. Network requests require contract version 2 and the exact history ID. Exact actor-scoped retries survive lost replies. `searchable=false`; `durable=true` certifies a synced local source/history transaction only. |
| [Local projection](https://git.rokkon.com/ai-pipestream/protomolt-search/src/commit/6968885a3ce92974bfa73886a1573ed49cdae3f3/src/embedded.rs) | Prepare/stage/publish/recover consume an exact accepted source version. These are trusted Rust methods taking `DocumentCatalog`, not public RPCs. |
| [Publication](https://git.rokkon.com/ai-pipestream/protomolt-search/src/commit/6968885a3ce92974bfa73886a1573ed49cdae3f3/src/node/publication.rs) | One persistent segmented shard joins artifact publication, prior-row retirement, serving activation and a durable source journal decision. It refuses WAL, unsealed tails and incompatible state. The activation identifies a particular read version, not perpetual visibility. |
| [Managed catalog](https://git.rokkon.com/ai-pipestream/protomolt-search/src/commit/6968885a3ce92974bfa73886a1573ed49cdae3f3/src/document_catalog/managed.rs) | `ActiveManagedCatalog` exposes acceptance, settlement, admitted history reads and metadata inspection. Its inner catalog is private; there is no managed publication adapter or network publication-status method. |

`IngestMapped`, `RoutedIngestMapped`, `DeleteDocuments`, `CommitReplacements` and
`Flush` remain useful lower-level APIs. Row counts, successful flushes or individual
shard acknowledgments are not source-certified, collection-wide publication proof.

## Required integration contracts

**Authority and revision.** Repository events trigger work; the event's exact
revision selects every part and retained schema read. Wait for complete, verified
reads. Persist parser/chunk/embedding results through selected-part saves with
appropriate revision preconditions, then index the resulting committed revision.
A concurrent update must retry or produce an explicit stale-work outcome, never
silently index current content as the requested historical revision. Typed `Any`
uses that revision's retained occurrence evidence; opaque content stays opaque.

Search's accepted source is rebuildable index input, not a replacement source of
truth. Define the indexed protobuf message and its mapping explicitly. Where it
is derived from several parts, record their revision, digests and policy identity;
do not describe assembled bytes as byte-identical stored fragments. Preserve exact
payload/descriptor bytes at boundaries that promise fidelity. The unary search
acceptance budget is an adapter limit, not a new repository document-size ceiling;
oversized input needs an explicit unsupported/deferred outcome until a suitable
transport exists.

**Identity and progress.** Use a collision-free encoding of the complete account
and repository address as the logical search key. Persist the association between
repository revision, mapping/policy generation and search history ID, allocated
version and accepted sequence (see
[versioned external-system bindings and ExternalLink publication receipts](../repo/external-links.md)).
Repository and search version numbers are different
domains. Pin history before writes; use stable operation IDs and a stable authorized
actor for retries. Serialize each logical document's revisions, prevent delayed old
work replacing newer content, and fence competing consumers. Keep accepted progress
separate from published progress; advance each only on its corresponding evidence.

**Authorization.** Map repository accounts to explicitly configured search
resources. Rust independently grants SEARCH/INGEST/ADMIN; SEARCH can carry mandatory
document visibility and field permissions, but INGEST is collection-scoped.
Implement and test repository ACL translation and revocation before exposing search
to end users. Current document reads must still authorize. Reauthorizing a clicked
document alone does not protect search snippets, counts, aggregates or suggestions.
Never grant unrestricted shared-collection SEARCH as a substitute for this work.

**Deletion.** An accepted search tombstone is not yet unsearchable. Its ordered
publication must retire every prior parent/chunk row for the key and survive restart.
Empty projections are distinct from deletion. Tombstone, ACL revocation and purge
handling must have explicit retry/reconciliation behavior; purge events carry
revision zero and cannot be processed as ordinary accession revisions. Prevent a
late indexing retry from resurrecting a deleted document.

**Checkpoint and recovery.** Distinguish workflow progress, source acceptance,
publication decisions, serving activation and backup completion. Rust's
`CatalogCheckpoint` pins source history and journal metadata and refuses pending
decisions; matching index artifacts are still required for a coherent backup.
An index snapshot, row count or accepted receipt alone proves none of the other
states. Replay may rebuild search from repository authority; it must not restore
repository state from the search copy.

## Next prerequisite: design before the managed publication slice

This is more than a Java client addition. Define a short Rust protocol design first:

1. Admit managed publication under current owner/epoch and current permission,
   preserving storage fences and cancellation/unknown-outcome behavior.
2. Specify durable handling of UNCONFIRMED and FENCED versions. Fenced history must
   never publish; unresolved outcomes cannot be assumed accepted. The existing
   journal requires consecutive accepted sequences, so skipping fenced entries
   requires a defined durable transition, not advancing a client cursor.
3. Define bounded publication/status access keyed by exact history, document,
   accepted version and index generation. Separate historical durable decisions
   from current serving observations and authorization to disclose either.
4. Start with one explicitly configured persistent segmented shard and complete
   document placement there. Do not claim multi-shard atomic visibility. Define
   model/mapping changes as a separately identified index generation.

After that design is reviewed, the local admitted publisher is a bounded first
implementation slice; network status and the Java adapter follow separately.
Do not expose `ActiveManagedCatalog.inner` or bypass owner admission to make the
existing local APIs fit. Rust `AGENTS.md` adds remote/push rules; it supplies no
missing publication contract. No Rust implementation is included in this plan.

## Acceptance matrix

Run the existing regression commands below from the named checkout. They are
executable baselines, **not evidence that the missing integration is complete**.
No new tests were run while writing this plan.

| Gate | Required assertion | Existing regression entry point / remaining test |
|---|---|---|
| Parser seam | Ordered typed output, warnings, remote failures, cancellation and explicit input budget | Java command A, `GrparseParserAdapterTest`; live parser qualification remains separate. |
| Embedding seam | Document/query purpose, pinned bundle, dimensions and remote failures; symmetric providers unchanged | Java A and B. Run live C with explicit deployment configuration. |
| Source acceptance | History pin, exact retry, current grant, lease expiry, unknown outcome settlement and restart | Rust D, `control_raft_hosted_writes`. |
| Local publication | Exact original source/identity, zero rows versus deletion, ordered replacement and crash reconciliation | Rust D, `document_projection`, plus E. |
| Query isolation | Grants, mandatory membership, revoked access and scoped statistics | Rust D, authorization and visibility targets. Repository-account translation still needs a real integration test. |
| Managed publication prerequisite | Revoke during publication; unresolved/fenced sequence never serves or blocks later valid history indefinitely; crash recovery yields the same decision | New Rust owner-publication tests required after protocol design; existing local tests cannot establish this. |
| Complete workflow | Upload → live parser → committed result revision → live Turboembed → managed Rust publication → authorized query; verify exact revision and model/policy identity | New Java `:integrationTest` against actual services required. Assert replacement, tombstone, duplicate/out-of-order events, lost replies, restart and cross-account refusal on this same path. No fixture-only substitute. |

```bash
# A — Java checkout; serialize every Gradle invocation.
flock /work/.gradle-krick.lock ./gradlew \
  :protomolt-parse-grparse:test --tests '*GrparseParserAdapterTest*' \
  :protomolt-search-embedding:test \
  :protomolt-search-chunk:test --tests '*PolicyDerivationTest*' \
  :protomolt-search-service:test --tests '*SearchEmbeddingPurposeTest*' \
  --max-workers=2

# B — Java protocol fixture (IT task, not the unit-test task).
flock /work/.gradle-krick.lock ./gradlew \
  :protomolt-search-embedding-turboembed:integrationTest \
  --tests '*TurboembedEmbeddingProviderIT*' --max-workers=2

# C — Java live provider: first set all six PROTOMOLT_TURBOEMBED_* variables
# documented in the provider README. Requires a compatible query-prefix bundle.
flock /work/.gradle-krick.lock ./gradlew \
  :protomolt-search-embedding-turboembed:integrationTest --rerun \
  --tests '*TurboembedLiveIT*' --max-workers=2

# D — Rust checkout at the pinned SHA. fault-injection is test-only;
# these features are necessary to execute the hosted-write test bodies.
cargo test --locked -j 2 --features raft,tls,fault-injection \
  --test control_raft_hosted_writes --test document_projection \
  --test authorization --test membership_visibility --test visibility_stats

# E — Rust source/index decision and recovery regression tests.
cargo test --locked -j 2 --features raft,tls,fault-injection \
  --lib document_catalog::publication::tests
```

Record exact service revisions/configuration, executed test counts, refusals and
crash outcomes when qualifying each gate. A disabled feature producing zero tests,
an absent external service or a skipped scenario is not a passing gate.
