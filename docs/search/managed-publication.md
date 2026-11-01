# Managed search publication and status

Protocol proposal for [#753](https://git.rokkon.com/ai-pipestream/protomolt/issues/753),
2026-11-01. **Design only; none of the proposed managed publication operations or
status guarantees is implemented by this change.** Start with one explicitly
configured persistent segmented shard containing every parent/chunk row for its
collection. No multi-shard atomic visibility, new service, repository API, or
replacement repository format is proposed.

The [parts-based requirements](../design/parts-based-repository-requirements.md)
remain authoritative for the repository. Owner epochs below belong to Rust search,
not repository saves. This develops the integration plan in
[PR #745](https://git.rokkon.com/ai-pipestream/protomolt/pulls/745); its reference
`docs/search/repository-rust-integration.md` was read in the processing worktree,
without editing it. It is not present at this PR's Java base.

## Evidence and scope recorded before edits

Assigned worktree: `/work/worktrees/protomolt/astra-753-search-contract`;
branch `docs/753-managed-search-publication`, clean starting commit
`7aa5f25266fa216fff1f8552d732ab4d67e654f1`. Scope: this design and its indexing-guide
link only. No Rust, Java, public/generated proto, dependency or deployment edits.
Read AGENTS.md, repository requirements, toolkit audit and source-index entries
for admission, codec, schema resolution, protobuf indexing, mapping and projection.
The audit's old repair findings are not treated as current implementation facts.

Rust research checkout was clean at `6968885a3ce92974bfa73886a1573ed49cdae3f3`.
`git ls-remote` against both Forgejo and GitHub returned that same SHA for HEAD and
`refs/heads/main` on 2026-11-01: **no movement**. Read its AGENTS.md and inspected
source read-only; no checkout, fetch, build output or edits there. Rust links below
are pinned to that commit; symbols, not stale prose, establish the baseline.

| Required behavior | Reused components and observed entry/read/storage path | Acceptance assertion for implementation |
|---|---|---|
| Exact revision input | `DocumentEvent` → `DocumentService.ReadDocument` → `AccessionGrpcService` → `DocumentReads`/`DocumentStreams` → catalog manifest and stacks; `GetSchemaDescriptor` verifies retained digest | Final stream completion, part digests and retained schema match requested revision; missing/unavailable/opaque never become typed success |
| Existing network acceptance | `main::hosted_write_service` → `HostedDocumentWriteService` → `RaftHost::lease` → `ActiveManagedCatalog::accept`/`settle` → source redb transaction | Exact actor/operation retry, pinned history, owner epoch and permission; acceptance is not publication |
| New managed publication | Proposed admitted methods on `ActiveManagedCatalog` → existing projection/staging → node publication → segment catalog and source journal → serving view | No bare `DocumentCatalog` escape; only final accepted sources publish; every sequence has a durable decision |
| New status | Managed admission → exact source operation/outcome and journal lookup; separate shard snapshot | Historical decision survives restart/revocation; disclosure and current serving are checked anew |

Java source anchors:
[DocumentReads](../../repo/accession/src/main/java/ai/protomolt/proto/repo/accession/DocumentReads.java),
[DocumentStreams](../../repo/accession/src/main/java/ai/protomolt/proto/repo/accession/DocumentStreams.java),
[SearchWorkflows](../../search/service/src/main/java/ai/protomolt/proto/search/service/SearchWorkflows.java),
[MessageProjection](../../transform/projection/src/main/java/ai/protomolt/proto/projection/MessageProjection.java).
`DocumentReads.resolve` selects an exact revision after current-access checking;
`getSchemaDescriptor` checks retained reachability, length and digest.
`MessageProjection` uses `DynamicMessage`/`ProtoFieldMapper`; incomplete source
masks cannot justify pruning reads. `SearchWorkflows` currently targets the Java
search API and builds JSON workflow definitions: reuse jobs execution/checkpoints,
not those definitions as a typed Rust integration or payload conversion precedent.

### Rust baseline and missing behavior

| Source | Verified behavior and required change |
|---|---|
| [Hosted adapter](https://git.rokkon.com/ai-pipestream/protomolt-search/src/commit/6968885a3ce92974bfa73886a1573ed49cdae3f3/src/document_write_service/hosted.rs), [main](https://git.rokkon.com/ai-pipestream/protomolt-search/src/commit/6968885a3ce92974bfa73886a1573ed49cdae3f3/src/main.rs) | `GetWriteTarget`/`AcceptDocument` are wired with raft+tls. Reuse bearer identity, leased admissions, blocking workers and capacity permits. Add publication/status to this existing hosted boundary later; no new service. |
| [Managed catalog](https://git.rokkon.com/ai-pipestream/protomolt-search/src/commit/6968885a3ce92974bfa73886a1573ed49cdae3f3/src/document_catalog/managed.rs) | `inner` is private; admitted acceptance, settlement, history and inspection exist. No managed stage/publish/status methods. `settle` currently requires the original actor; administrative reconciliation below is a new bounded capability. |
| [Source authority](https://git.rokkon.com/ai-pipestream/protomolt-search/src/commit/6968885a3ce92974bfa73886a1573ed49cdae3f3/src/source_authority.rs) | `SourceAdmission::admit_write` checks ACTIVE owner, exact write epoch and action. Shared guard prevents local policy/owner application during the operation; lease freshness bounds isolated-host authority. Recovery admission only opens a handle. |
| [Projection](https://git.rokkon.com/ai-pipestream/protomolt-search/src/commit/6968885a3ce92974bfa73886a1573ed49cdae3f3/src/document_catalog/projection.rs), [outcomes](https://git.rokkon.com/ai-pipestream/protomolt-search/src/commit/6968885a3ce92974bfa73886a1573ed49cdae3f3/src/document_catalog/outcome.rs) | Preparation verifies history/version/sequence/source and mapping; it does not establish managed outcome finality. `mark_unconfirmed` can move ACCEPTED to UNCONFIRMED after the write returns from storage. Publication must exclude that intermediate window. |
| [Embedded API](https://git.rokkon.com/ai-pipestream/protomolt-search/src/commit/6968885a3ce92974bfa73886a1573ed49cdae3f3/src/embedded.rs), [node publication](https://git.rokkon.com/ai-pipestream/protomolt-search/src/commit/6968885a3ce92974bfa73886a1573ed49cdae3f3/src/node/publication.rs) | Trusted methods take ordinary `DocumentCatalog`. `publish_rows_inner` prepares serving state, holds shard write guard through manifest commit/activation, then resolves source journal. This is not a managed admission boundary. |
| [Journal](https://git.rokkon.com/ai-pipestream/protomolt-search/src/commit/6968885a3ce92974bfa73886a1573ed49cdae3f3/src/document_catalog/publication.rs), [storage messages](https://git.rokkon.com/ai-pipestream/protomolt-search/src/commit/6968885a3ce92974bfa73886a1573ed49cdae3f3/proto/ai/protomolt/search/storage/v1/document_catalog.proto) | `prepare_index_publication` requires `committed_sequence + 1`. Decisions currently hold `ProjectionIntent`; recovery compares durable before/after manifests, preserving pending on a third manifest. Extend this journal, including checkpoint validation, for explicit skips and managed attempts. |

## Identities and invariants

1. Repository identity is full `NodeAddress` (including account), positive exact
   revision, retained schema artifact identities and selected part/segmentation/
   embedding-set identities and digests. Encode the logical search key as a
   versioned, length-delimited binary tuple of **all** address fields; no delimiter
   concatenation, hash-only key or revision in the logical key. Cross-language
   fixtures must prove injectivity for empty fields, Unicode and separators.
2. Search source identity is `(workspace, collection, history_id, document_key,
   version, accepted_sequence)`. `history_id` is exactly 16 bytes. `version` is
   search allocated per key; `accepted_sequence` orders the whole catalog, including
   unresolved/fenced allocations. Neither equals repository revision. Persist the
   verified association in the workflow's protobuf checkpoint; do not infer it from
   coincidentally equal integers. Pin `DocumentWriteTarget` before acceptance.
3. Index identity is `(history_id, index_key, generation_digest, shard_binding)`.
   A provisioned index key immutably binds a generation and a persistent shard;
   no caller-selected paths. Digest covers versioned binary mapping definition,
   reviewed plan fingerprint, retained descriptor identities, text analysis,
   chunking policy, model revision/manifest, dimensions, similarity and normalization.
   A plan fingerprint alone does not identify an embedding model.
4. Mutation identity is `(history_id, principal, PUBLICATION, operation_id)` plus
   exact intent bytes, including
   source identity, index identity and action. Different intent under the same ID
   is `ALREADY_EXISTS`. Preserve stable actor and ID after lost replies. Credentials,
   retry deadline and trace context are not intent. Expected *current owner epoch*
   is an execution precondition, not part of immutable source identity or retry hash.
   Source write epoch remains evidence; a new current owner may publish a historically
   final accepted source from an older epoch, but may never un-fence a source.
5. At most one pending publication per index; a source sequence is resolved exactly
   once per index. Every terminal decision binds source identity and prior decision,
   including no-op decisions. `decided_through` is the contiguous resolved prefix;
   it is not "last searchable sequence". Arbitrary client cursor advancement is forbidden.
6. Publication changes all rows for one logical document together in this shard,
   including old parent/chunk retirements and both search legs. Ordinary ingest,
   flush and compaction cannot mutate a source-owned index outside its journal.
   Compaction must preserve the generation and source decisions.

Repository authority is unchanged: selected-part writes and copy-forward remain
on `DocumentService`. Read verified completion before deriving input; decode with
retained schemas/occurrences, using descriptor APIs, mapper/CEL, `ProtoValidator`
and parts codec as applicable. Opaque bytes stay opaque. Search input is an explicit
protobuf projection, not a byte-identical claim about reassembled parts; retain
exact original source/descriptor bytes where promised. No JSON/Struct round trip.
Do not share projection environments across different retained schemas by type name.

## State machine and sequence holes

Source finality, publication progress and serving are separate dimensions:

| State | Durable meaning | Permitted next step |
|---|---|---|
| No recorded source outcome | Lookup found none, not proof that an in-flight write cannot commit | Same acceptance retry under fresh authority |
| UNRESOLVED | Allocated source exists, but acceptance is UNCONFIRMED or completion is still pending | Settle; no projection or cursor advance |
| ACCEPTED_FINAL | Admission completion durably finalized for exact source version | Stage under current publisher admission |
| FENCED | Terminal source refusal with control revision; bytes/history retained | Append SKIPPED_FENCED for each index when it reaches this sequence |
| STAGED | Private derived files, exact source/generation/target read version | Revalidate or restage; not a durable publication promise |
| PENDING | Journal intent persists; before/after artifact identities fixed, outcome unknown | Reconcile actual durable manifest under publication fence |
| PUBLISHED | Immutable artifact decision for exact source/generation | Observe current serving separately; never rewrite original acceptance receipt |
| SKIPPED_FENCED | Immutable no-op decision consuming a terminal fenced sequence | Next consecutive sequence; no rows added or retired |
| Current serving | Authorized observation of a read version, not a durable lifecycle state | Can change, be unavailable, be superseded or be hidden immediately |

**Close the acceptance-completion race first.** Add an internal durable completion
marker for managed allocations: write them pending, then finalize ACCEPTED or
UNCONFIRMED after the durability/freshness check. Retry, finalization, settlement
and publication eligibility use the same source transaction conflict point.
Final ACCEPTED is monotonic; a late original writer cannot demote a settled result.
The completion transaction checks fresh authority immediately before commit, under
the shared guard, and records that admission control revision. This is its terminal
authorization boundary; it does not recursively require another completion marker.
A slow completion sync can delay disclosure/serving but cannot demote that terminal
decision. An uncertain storage result is resolved by rereading the marker.
A crash before finalization leaves pending/UNRESOLVED, never publishable. Existing
managed records without this marker require explicit recovery reconciliation
before first managed publication; do not infer finality from old ACCEPTED bytes.
This is a proposed storage migration, not a claim about today's `WriteOutcome`.

**Settlement without impersonation.** Keep existing actor retry settlement. Add a
local reconciler authorized by current ADMIN *and* INGEST on the ACTIVE owner to
resolve one exact pending source operation if its actor is absent. It evaluates
the recorded actor's current INGEST and original owner/write epoch against the
same fresh authority snapshot. Still valid → final ACCEPTED; definite revocation
or epoch move → FENCED at that control revision. Outage, expired lease, missing
control history or ambiguous identity → remain unresolved. Record reconciler and
original actor separately; never accept a caller-supplied principal as credential.
This capability does not rewrite already-final accepted history. It needs tests
and review before use; absent it, a departed actor can block the catalog forever.

**Skip transaction.** Extend the existing journal decision value to a versioned
oneof: published `ProjectionIntent` or `SkippedFenced`. The latter contains full
source identity, source outcome evidence/digest and fence control revision, prior
terminal decision digest, current owner admission evidence, and unchanged manifest
digest/epoch. Under the segment publication fence and source write transaction:

1. Require current admitted owner, exact history/generation, no pending publication
   or maintenance, and sequence exactly `decided_through + 1`.
2. Read the terminal FENCED version/operation association from storage (not a client
   boolean). Verify the live durable manifest matches the journal's committed tip.
3. Check admission freshness immediately before commit, with the same terminal
   boundary as source completion. Atomically insert immutable skip and advance the
   prefix; leave manifest and
   serving view unchanged. An exact retry reads the same skip. A conflicting skip,
   missing allocation, ACCEPTED or UNRESOLVED source refuses.

Example: sequence 40 published, 41 unresolved, 42 accepted. 42 cannot publish.
After 41 becomes fenced, commit skip 41, then publish 42. Crash before skip commit
leaves prefix 40; crash after commit replays skip 41. No synthetic empty document,
renumbering, unrecorded gap, or treating a timeout as a fence. An unresolved source
blocks the entire initial single-index prefix; this deliberate throughput cost
avoids an unbounded sparse journal. No general "skip invalid source" operation is
proposed: deterministic mapping errors remain reported blockers for operator repair.

## Admission, revocation and publication commit

The publisher is a configured machine principal, with current INGEST for the
resource; provisioning/generation changes and reconciliation require ADMIN too.
INGEST does not grant SEARCH or arbitrary source/status disclosure to end users.
All managed mutation entry points obtain `RaftHost::lease` and call the managed
owner check. Never expose `inner`, reopen its path as an unbound catalog, use a
recovery admission for writes, or fall back to local admission on a hosted member.

Stage expensive work privately after admission and final-source checking; release
that admission before CPU/I/O work. Retain no authorization promise in the stage.
At publish, acquire a new lease, verify owner epoch, permission, final source,
generation, exact stage identity and target read version. The lock order must be
uniform: authority admission → node/shard publication guard → segment publication
fence → source transaction; never acquire authority while holding a storage lock.
Existing node/catalog nesting must be audited to implement this order without
recursive acquisition. A stale target is `ABORTED`; rebuild/rebind privately under
the same logical operation, not an unconditional publish against a changed index.

Extend `publish_rows_inner` with admitted callbacks at intent persistence, immediately
before durable manifest switch, and after sync **before** installing serving state.
Keep the shared authority guard and shard exclusion through this short commit.
Lease elapsed before irreversible work → refuse, resolve before-manifest abort,
no decision/prefix advance. Revocation applied before admission → refuse. A valid
admission guard prevents local application of revocation during commit; leased
freshness still matters if leadership/control moves elsewhere during filesystem I/O.

**Late durability cannot authorize serving.** If manifest sync finishes after the
lease expires, or its result is uncertain, persist/retain PENDING and quarantine
this shard from query and ingest. Do not run today's unconditional activation step.
Status may disclose a historical pending record to an authorized operator but
must report serving UNAVAILABLE. Cancellation likewise releases neither permits
nor ownership until the worker ends. A transport timeout is never a rollback receipt.

Under a new lease, reconcile the original publishing actor and original publication
owner epoch, independently of source acceptance finality:

- Durable before-manifest: abort this attempt, preserve operation identity for
  retry, no sequence decision. No serving mutation occurred.
- Durable after-manifest and original publication authority still valid: finalize
  immutable PUBLISHED and activate under the current admitted owner/shard guard.
- Durable after-manifest but original publisher revoked/epoch moved before an
  uncertain attempt was confirmed: do **not** call it published or skip its accepted
  source. Keep shard quarantined and restore the exact prior logical rows/binding
  using a journaled compensation manifest at a new catalog epoch. Retain both
  artifact digests and the fenced attempt. Only after compensation is durable may
  the same sequence be retried by a currently authorized publisher. This is a
  fenced *attempt*, distinct from terminal FENCED *source*; it consumes no sequence.
- Third manifest, lost prior artifacts or mismatched history: retain intent and
  fail closed with corruption/reconciliation details; no guessed success or reset.

The compensation record extends existing before/after recovery, with its own
pending before/after identities; it advances the journal manifest tip and artifact
epoch while retaining the same source prefix, like journaled maintenance. Extend
`validate_tip` to verify this chain rather than comparing only the last source
intent. A crash during compensation is reconciled the same way. Pin prior artifacts until attempt resolution; backup and maintenance
refuse unresolved attempts. Compensation support and query quarantine are required
before the managed publication path is exposed, not optional cleanup. A revoked
actor cannot trigger compensation itself; the current admitted ADMIN+INGEST owner
reconciles. If there is no active owner, remain unavailable until one is provisioned.

Successful commit order is: final freshness check → durable manifest → post-sync
freshness check → terminal source-journal decision transaction → serving install.
The terminal transaction checks admission immediately before commit and records
its control revision while retaining the authority guard. This precommit check is
the terminal decision's authorization boundary, as for source completion above;
there is no infinite chain of confirmation writes. If the result is uncertain,
quarantine and reread: a terminal record means historical PUBLISHED, its absence
means the pending attempt still needs reconciliation. If the lease expires during
terminal sync, do not install serving state until a new current admission succeeds.
A terminal PUBLISHED record is never compensated because the old publisher was
subsequently revoked. A merely pending after-manifest remains subject to settlement
and compensation. Fault tests must distinguish these two crash windows.

If terminal PUBLISHED was durably decided but reply/activation is lost, retain
historical success: recovery can activate its artifacts under current owner
admission. It is permissible to retain PUBLISHED while serving is unavailable.
Serving installation always checks current owner authority; time-bounded
admissions cannot be serialized as reusable grants.

This deliberately adds a completion phase and compensation to the trusted local
path. Merely adding an entry authorization check to it is insufficient. Query
exposure after owner fencing/lease expiry must also fail closed, including after
restart; an ingest-only fence is not sufficient. Current SEARCH permission and
repository ACL translation remain independent query gates.

## Retry, status and bounded operation shapes

Use protobuf operations on the existing Rust `DocumentWriteService` in a later
contract slice. The sketch below is **not a compilable or allocated public proto**;
names/tags require review then. Reuse existing
[write messages](https://git.rokkon.com/ai-pipestream/protomolt-search/src/commit/6968885a3ce92974bfa73886a1573ed49cdae3f3/proto/ai/protomolt/search/v1/document_write.proto)
and [projection messages](https://git.rokkon.com/ai-pipestream/protomolt-search/src/commit/6968885a3ce92974bfa73886a1573ed49cdae3f3/proto/ai/protomolt/search/v1/document_projection.proto).
No payload, descriptor set, segment files, physical paths or bearer token in status.

```proto
// Proposed field inventory, not field-number assignments.
PublicationKey {
  DocumentWriteTarget target; // required; exact configured resource/history
  bytes document_key; uint64 version; uint64 accepted_sequence;
  bytes index_key; bytes generation_digest;
}
PublishAcceptedRequest {
  uint32 contract_version; // new supported version required; reject zero/unknown
  PublicationKey key;
  bytes operation_id;
  uint64 expected_owner_epoch; // compare to current managed handle, not source epoch
  StageDocumentProjectionRequest stage; // cross-check nested history/key/version
}
GetPublicationStatusRequest {
  PublicationKey key;
  optional bytes operation_id; // own actor scope; never accept an actor selector
  bool include_serving;
}
PublicationStatus {
  PublicationKey key;
  SourceState source; // NO_RECORDED_OUTCOME, UNRESOLVED, ACCEPTED_FINAL, FENCED
  PublicationState progress; // NONE, BLOCKED, STAGED, PENDING, PUBLISHED, SKIPPED_FENCED
  optional DocumentProjectionStage stage; // sanitized, no paths; advisory only
  optional HistoricalDecision decision; // published identity or explicit skip
  optional ServingObservation serving;
  optional uint64 blocking_sequence; // operator only
  AttemptState attempt; // NONE, RUNNING, UNRESOLVED, FENCED, COMPENSATING, RETRYABLE
  optional FailureDetail failure; // bounded typed reason, no source/path/credential
  bool replayed;
}
HistoricalDecision {
  bytes decision_id; uint64 decision_control_revision; uint64 publisher_epoch;
  oneof result { PublishedIdentity published; SkippedFencedIdentity skipped; }
}
PublishedIdentity {
  bytes intent_id; uint64 catalog_epoch; uint64 rows; bool deleted;
  bytes before_manifest_sha256; bytes after_manifest_sha256;
  optional DocumentProjectionActivation historical_activation;
  // Optional recorded observation; absent if no activation was observed.
  // Never relabel its historical stats epoch as a current-serving assertion.
}
SkippedFencedIdentity {
  uint64 fenced_at_control_revision; bytes source_outcome_sha256;
  bytes prior_decision_sha256; bytes unchanged_manifest_sha256;
  // Full source/index identity comes from the enclosing PublicationKey.
}
ServingObservation {
  enum State { UNKNOWN; UNAVAILABLE; NOT_CURRENT; CURRENT_ROWS; CURRENT_EMPTY;
               CURRENT_TOMBSTONE; }
  State state;
  optional DocumentProjectionActivation activation;
  uint64 observed_control_revision;
  // Exact stats incarnation/epoch and catalog epoch in activation; no TTL promise.
}
```

`PublishAccepted` performs bounded stage + one publication attempt; no unbounded
"wait until searchable" call or range drain. If an earlier sequence blocks it,
return BLOCKED with no staging work. An owner worker performs internal skip and
settlement before retrying. Final result is status, not a rewritten
`DocumentWriteReceipt.searchable=true`. Exact operation replay resolves recorded
intent first after disclosure authorization, before stale execution preconditions.
A pending record returns pending; no record does not prove an old request stopped.
Changed intent conflicts even after failure; authorization/resource rejection before
reservation consumes no operation ID. Store operation intent digest and result
atomically with journal finality; concurrent duplicate operations converge. Staging
may be rebuilt after restart and does not get a permanent operation ID of its own.
Use versioned deterministic binary intent encoding, reject unknown command fields,
retain unknown source fields; budgets are execution controls, not semantic intent.

`GetPublicationStatus` is read-only and must not settle/repair as a side effect.
Authenticate then authorize before existence lookup. For the first slice it is
machine-operator-only: current INGEST on the exact resource for own operations;
ADMIN plus INGEST for collection-wide historical decisions. Include-serving also
requires SEARCH and mandatory document/field visibility. Do not let a status call
bypass repository-account scope. Later end-user status needs the same current
repository document policy as queries; until that translation is qualified, expose
neither. A removed permission prevents disclosure even if a historical decision
still exists. Return a uniform NOT_FOUND for hidden/missing document keys; global
permission failures expose no document/operation detail. Backend unavailability
is UNAVAILABLE, not "not found" or `searchable=false`.

Read historical decision and serving snapshot separately. An authorized response
can contain PUBLISHED plus NOT_CURRENT (replacement or generation switch),
PUBLISHED plus UNAVAILABLE (restart), or SKIPPED_FENCED plus no serving observation.
Current observation requires matching history/index/generation and per-document
version membership in a pinned shard read snapshot. The journal's most recent
collection decision is not enough: another document may have published since.
Maintain/reconstruct per-document active identity from certified live rows plus
zero-row/tombstone decisions, and preserve it through compaction. Recheck transport
and document access at response emission; withhold a reply revoked during lookup.
Do not disclose a superseding version's identity unless separately authorized.

Proposed bounds: one key/request, document key 1..16 KiB, index key 1..1024 bytes,
operation ID 1..1024 bytes, generation digest exactly SHA-256, all positive versions/
sequences/epochs, target strings matching existing configured-resource validation.
Request envelope cap 1 MiB, status cap 64 KiB, <=64 admitted operations and <=64 MiB
pending encoded requests; reject excess, do not truncate. Stage retains existing
limits (1..65536 rows, 1..64 MiB encoded projection, 1..1 GiB staged files).
Receiver may configure lower caps with explicit RESOURCE_EXHAUSTED; no silent
fallback. These proposed defaults need capacity qualification, not a new document
size ceiling. Existing acceptance unary limit is configured within 1..64 MiB and
pending limit within 1..256 MiB. A repository document exceeding the adapter budget
is deferred with a named reason until a suitable transport/projection exists;
never split it into independently published chunks or drop parts to fit. Process
heap, decoding, disk and transport limits must be measured separately.

## Repository ordering, deletion, generations and disclosure

The Java consumer deduplicates `DocumentEvent` by stable event ID and exact address/
revision, pins exact selected part reads and records the accepted association before
advancing its source-acceptance checkpoint. Serialize work per logical key using
existing jobs ownership/CAS; use `expected_version` on search acceptance. Compare
repository revision to the durable per-key workflow high-water mark before submitting
work. Concurrent consumer losers reread their checkpoint, not just search's head.
A lost acceptance reply retries identical bytes and actor/operation ID; it must not
allocate a second version. Do not publish delayed revision N after N+1. An explicit
history replay uses a separate non-serving generation and an ordered replay policy.
Mapping generation alone must not make an old repository revision current again.

An accepted tombstone is not deletion from search. Its publication retires **every**
prior parent/chunk row for the key in both search legs and records CURRENT_TOMBSTONE
with zero rows. A normal zero-row projection records CURRENT_EMPTY, retains source,
and also retires prior rows. A fenced tombstone changes nothing; a later valid
repository deletion requires new accepted work. A durable repository tombstone or
purge blocks stale indexing retries at the workflow boundary. Purge events have
revision zero: use event kind, event ID and purge ID as control identity, never
pretend zero is an accession. Purge immediately invalidates query disclosure and
queues search removal. This design does not claim that tombstoning Rust removes
retained source bytes, old segments or backups; physical search erasure needs its
own reviewed retention contract. Search can be rebuilt from repository authority;
repository cannot be restored from the search copy.

Account translation must explicitly bind account → workspace/collection, credential
→ principal, document ACL → mandatory membership filter, and field permissions →
query/result field policy. INGEST is collection-scoped, not a repository writer
grant. Restrict this adapter to a trusted service authorized to read the source and
write that account's configured resource; reject unconfigured/cross-account targets.
Never grant blanket shared-collection SEARCH. Gate snippets, counts, aggregates,
suggestions and ranking statistics before disclosure, not merely document clicks.
ACL changes/revocation must invalidate the current query policy independently of
asynchronous reindexing. Until a synchronized policy revision or synchronous current
ACL check can establish freshness, fail closed for that account/document, including
cached responses and status. End-user search is a separate qualification gate.

A mapping/model change creates a new immutable generation/index key and empty
persistent shard; never alter a live generation in place. Replay the same source
history from sequence 1 with explicit fenced skips, through a pinned fence; tail
later sequences until caught up. Each generation has its own contiguous decisions.
For initial implementation use an explicit maintenance window: stop serving, quiesce
publication at a fixed fence, finish the new generation, then durably CAS the
configured serving binding from old to new under ADMIN+INGEST and current owner
admission, recording both generations and fence. Reopen/check that binding before
queries resume. A crash either retains the old binding or selects the fully verified
new one; ambiguous state stays unavailable. There is still only one serving shard,
no online multi-shard atomic switch claim. If old sources cannot project under the
new mapping/model, the rebuild blocks; a new repository-derived input/history and
its migration plan must be explicitly reviewed, not substituted by latest-schema
lookup. Retired generation decisions remain historical; rolling back requires
catch-up including tombstones/ACL state, never restoring stale visibility.

## Restart and checkpoints

Startup recovers the configured managed owner and exact catalog binding; it does
not provision an owner or permit writes from recovery admission. Open the exact
persistent shard, verify generation and source owner, keep queries closed, acquire
fresh admission and reconcile pending acceptance/attempts/compensation. Verify
contiguous published/skip decisions, their source outcomes and manifests before
serving. A stale stage is discarded/rebuilt; a published decision is never discarded
because its reply was lost. Corrupt decision/manifest associations halt recovery.

Keep these checkpoints distinct in protobuf job state:

| Checkpoint | What it proves | What it does not prove |
|---|---|---|
| Workflow | Exact input accession/event, derived part saves, pending operation IDs | Search acceptance or serving |
| Acceptance | Search history/version/sequence and final accepted association | Projection or backup |
| Publication | Per-generation contiguous decision prefix/digest; per-key published version | Current serving, freshness of ACLs, replica durability |
| Serving observation | Exact node read incarnation/epoch, generation and current visibility policy observation | Perpetual visibility or a durable backup |
| Backup | Source `CatalogCheckpoint`, decision chain including skips, matching segment artifacts and generation/serving binding | Repository backup or a completed workflow |

Extend `checkpoint_states`/`capture_checkpoint` and backup validation to include
skip evidence and completion/compensation state. Refuse backup while any relevant
outcome or publication decision is unresolved. Preserve source/index history and
schema/model identities across restore. A manifest snapshot alone, Flush, row count,
accepted receipt or workflow completion cannot advance the publication checkpoint.

## Small implementation slices and executable acceptance plan

Each row is a separate reviewable implementation slice; later rows depend on the
previous guarantees. All named new tests below are **proposals, not passing tests**.
Use real persistent storage and Raft hosts; mocks cannot certify durability/leases.

| Slice / concrete call sites | Proposed acceptance/fault tests and required assertions |
|---|---|
| 1. Managed source finality and abandoned-actor reconciliation: `managed::{accept,settle}`, `outcome::{mark_unconfirmed,move_outcome}`, storage `DocumentOperation`/`DocumentVersion` | Add `managed_publication` integration target: pause original writer after source sync; race retry/settler/publisher; crash before completion marker. No pending version stages; late finalizer cannot demote terminal settlement. Current-owner reconciler evaluates original actor, rejects foreign authority, preserves outages, cannot un-fence. |
| 2. Explicit skip decisions: `publication::{prepare_index_publication,recover_index_publication,validate_projection_journal,checkpoint_states}`, decision storage/backup readers | Extend `document_catalog::publication::tests`: accepted 40 → unresolved 41 → accepted 42; assert 42 blocks, fence/skip 41, then 42 publishes. Kill before/after skip commit and reopen. Duplicate skip is identical; changed manifest/outcome/generation refuses. No row retires on skip; checkpoint/restore validates the same chain. |
| 3. Admitted local publisher: managed methods, `node::publication::{publish_rows_inner,recover_document_projection_blocking}`, segment commit hooks/query gate | `managed_publication`: revoke INGEST/move epoch before stage, after stage, before manifest, during sync, after decision, before reply, in both orderings. Isolate leader; expire lease during each sync. Assert no unauthorized serving, exact compensation/recovery, final historical decisions immutable. Kill during compensation; third manifest retains intent. Test all parent/chunk rows and both query legs. |
| 4. Bounded hosted publication/status: `HostedDocumentWriteService`, `main::hosted_write_service`, existing service proto in a later PR | Extend `control_raft_hosted_writes`: dropped client, duplicate concurrent operation, changed intent, wrong history/generation, full permits/byte cap, revoked reply disclosure, restart. Add `managed_publication_status`: published A remains historical after B/compaction, snapshot correctly reports A NOT_CURRENT; zero rows/tombstone, status outage vs absence, hidden keys, no source bytes. |
| 5. Java adapter and end-to-end qualification: exact `DocumentService` client read, jobs checkpoint/consumer, search workflow seam (do not repurpose Java Lucene success) | New `ManagedSearchPublicationIT` under `:protomolt-search-service:integrationTest`: actual repository → parser → embedding provider → hosted managed publication → authorized query; preserve exact parts/schema and model identity. Duplicate/out-of-order events, same-key competing consumers, lost acceptance/publication replies, old revision after delete, purge-zero event, ACL revoke, cross-account refusal, oversize adapter input, process restart and new generation switch. |

Existing executable regression commands, **not executed for this docs-only PR**:

```bash
# Rust: isolated checkout at the pinned SHA; never build in another worker's checkout.
cargo test --locked -j 2 --features raft,tls,fault-injection \
  --test control_raft_hosted_writes --test document_projection \
  --test authorization --test membership_visibility --test visibility_stats
cargo test --locked -j 2 --features raft,tls,fault-injection \
  --lib document_catalog::publication::tests

# Java: established exact-read/provider and workflow baselines, not Rust proof.
flock /work/.gradle-krick.lock ./gradlew \
  :protomolt-repo-accession:integrationTest \
  --tests '*RetainedSchemaReadIT' --tests '*VerifiedReadFailuresIT' --max-workers=2
flock /work/.gradle-krick.lock ./gradlew \
  :protomolt-search-service:test --tests '*SearchWorkflowsTest' --max-workers=2
```

After slices add the proposed Rust targets and Java IT, execute (not yet available):

```bash
cargo test --locked -j 2 --features raft,tls,fault-injection \
  --test managed_publication --test managed_publication_status
flock /work/.gradle-krick.lock ./gradlew \
  :protomolt-search-service:integrationTest --tests '*ManagedSearchPublicationIT' \
  --max-workers=2
```

Record exact SHAs/configuration, executed/pass/fail/skip counts and crash points.
Zero selected tests, absent external providers or disabled features are not success.
Existing tests `decisions_survive_reopen_and_keep_acceptance_receipts_unchanged`,
`a_third_manifest_preserves_pending_intent_and_refuses`, and
`an_unconfirmed_write_whose_actor_was_revoked_meanwhile_is_fenced` are useful regression
anchors; none proves the proposed combined managed path.

## Review decisions and limits

Recommended decisions for this design: durable finality marker; explicit per-index
fenced skips; current-owner administrative settlement of abandoned actors;
quarantine plus journaled compensation for uncertain unauthorized attempts;
operator-only initial status; offline generation switch. They are proposals for
review, not changes to an approved production architecture in this PR.

Before implementation, resolve these bounded choices:

- Storage version/migration strategy for old ACCEPTED outcomes and journal entries:
  recommend explicit reconciliation/import, no optimistic inference. Decide whether
  unsupported legacy managed catalogs require rebuilding from repository authority.
- Precise lock ownership and callbacks for finality/compensation; prove no query
  path bypasses quarantine and no old-artifact reclamation races compensation.
  If the current segment catalog cannot retain/restore the prior logical view, this
  blocks slice 3; do not weaken post-sync admission to avoid implementing it.
- Pin canonical generation/intent encodings with Java/Rust fixtures and allocate
  proto names/field numbers only in the later contract PR. Decide model-bundle
  coverage and resource caps from actual provider measurements.
- Choose the account ACL freshness mechanism and principal mapping authority before
  exposing user queries/status. Search grants alone do not implement repository ACLs.
- Physical erasure of Rust source history/backups after repository purge is separate
  from search invisibility and needs explicit retention policy; no erasure claim here.

Design validation for this PR is source trace review, local link resolution
(10 local Markdown targets, zero missing; 14 external targets returned HTTP 200)
and `git diff --check`. Runtime tests executed: **0**; integration tests executed: **0**.
No live provider qualification, compensation prototype, merge or deployment is claimed.
