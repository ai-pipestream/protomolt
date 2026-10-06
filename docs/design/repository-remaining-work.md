# Remaining repository goal work

This is the working order for the additions to the repository composition goal.
It does not replace the [design](repository-composition.md) or declare unfinished
features available. Recovery is one workstream, not the whole goal.

Current restore checkpoint: canonical historical commands, current-READ replay,
V86 historical provenance, snapshot v2 with v1 replay, and shared physical/slot
binding are implemented. The internal assessment path records the exact historical
command with authenticated principal/account checks and stages its selections under
current authorization. See [latest qualification](../evidence/repository/2026-10-05-historical-exact-admission/README.md).

Retained definition loading now has a separate owned scope bound to the captured
caller's authorization and exact live source Use. Selection identity, cancellation,
capacity and reservation cleanup have focused regression coverage. This is a reusable
loader, not whole-command restore execution.

Whole-command fragment capture now supports exact pinned historical references,
mixed upload/current/historical identities, aggregate copy reservations and hash
checks before schema loading. The raw assembly helper has an explicit historical
path, with no policy-mode selection or typed validation grant. The admission library
now supports composite per-source schema routing, exact container agreement, per-source
evidence checking and exact global asset/ordinary-occurrence accounting. The internal
whole-command assessment owner now retains source Uses, binds the exact caller,
assesses members under one supplied policy and time, and rechecks current source
READ during inspection and replay. Its callback-scoped inspection provides immutable
summaries plus the caller-supplied command and policy values; it exposes no borrowed
payload or publication capability. Historical opaque-source classification now
requires explicit sealed OPAQUE admissions and refuses typed downgrades or missing
admissions. The two-source mixed-mode negative case and durable publication
integration remain unfinished.
The SQL assessment suite now also covers one member assembled from two historical
sources, including second-source access revocation and release of both pins. The
ordinary registry resolver is unavailable in that fixture. This closes the host
routing test gap, not the provider-read or publication gates.

The internal unclaimed historical path now retains observed evidence through CREATE,
using current-policy/destination authorization and shared historical physical/slot
locks in its transaction. A production-JAR host qualifies the successful path with
real versioned provider reads. Historical-specific failed/ambiguous CREATE cases and
atomic reference publication remain next; do not infer complete recovery from this
successful path.
Preserve each member's container and occurrence definitions,
source-to-target ordinals, shared byte accounting and source pins. A prior historical
verdict cannot substitute for a new assessment. Claims, sessions and public
historical execution remain disabled until their own integration tests pass.

The independent provider, remote parity, recovery, pruning and hydration work below
remains part of the goal. Continue the bounded non-S3 managed profile review alongside
restore. Use RustFS for performance and LocalStack for S3 correctness. Replica
throughput must be measured under sufficient offered load with latency and contention
evidence; adding processes alone does not demonstrate scaling. Hydration follows the
recovery and retention foundations. The optional JCR assessment continues to govern
foundation boundaries without adding JCR dependencies or asserting compliance.

## Independent work that can advance now

1. **Selected historical reads.** The optional Java SPI, wire contract, response
   verifier and explicit gRPC service exist. Complete malformed-response coverage,
   then the remote client and managed host wiring. Require local/remote parity,
   current authorization at delivery, cancellation and byte-budget release. Test
   exact retained descriptors with the original registry unavailable. Never
   substitute the latest registry definition for historical evidence.
   The client belongs in the `repo/history/grpc` leaf, independent of the service's
   SQL and provider assemblies. Admission exposes a closeable selected-response
   decode result using the already verified descriptor. Bound inbound messages,
   shared reservations and open results, and test the actual in-process service
   before adding managed-host wiring.
   The remote adapter must preserve current READ authorization at each exposure:
   cached decoded bytes and local cancellation checks alone cannot satisfy the
   repository SPI. Bind the caller to transport credentials explicitly; a request
   account ID or locally asserted process authority is not a remote grant.
   `HistoricalOccurrenceClient` provides one-shot verified reads, with fresh server
   authorization for every RPC. That explicit API advances remote use without
   claiming cached results implement the SPI's current-READ semantics. Explicit
   managed mounting now shares native reader lifetime and response capacity with
   ordinary history; selected reads match the local SPI across fresh in-process
   and Netty hosts. Full local/remote repository parity remains unfinished.
2. **Schema resolution and cache.** Reuse the existing resolver and registry
   abstractions. Separate authorized discovery from immutable artifact lookup.
   Follow the [bounded cache plan](repository-schema-cache.md); its first slice
   caches exact artifact bytes, not discovery grants or validation verdicts.
   The optional registry adapter and native runtime-owned resolution scopes now
   exist. Bounded host-owned sharing now isolates caller cancellation and retains
   abandoned load capacity through provider completion. Managed-host composition
   now accepts an explicit host-bound schema scope and lifecycle component. Keep provider
   timeout/allocation and shutdown ownership explicit at that boundary.
   `ManagedDocumentServices` owns a native runtime, but the managed publication
   accessor is package-private and no publication RPC is mounted. `ManagedSchemaAccess`
   supplies optional scope composition and shutdown ownership without a production
   dependency on the registry adapter. Scopes receive the actual caller and member
   and authorize every occurrence. Shutdown rejects new scopes, quiesces publication,
   and drains abandoned registry workers before shared resources close. A drain
   timeout retains those resources for another attempt. The host closes its borrowed
   registry store only after successful service shutdown; failed construction leaves
   schema cleanup with the caller. Configuration is checked before readers register.
   Real PostgreSQL/LocalStack/Git fixtures cover typed/opaque/replay, scoped caller
   forwarding and held-read shutdown. Public
   publication activation additionally needs the scoped authorization and durable
   session work below; schema wiring is not a document-creation grant.
   Establish tenant/security scope, exact schema identity, bounded ownership,
   eviction, concurrent lookup and cancellation before sharing cached entries.
   Test equal type URLs with different occurrence definitions, registry outage,
   revoked access and unresolved envelopes. Raw preservation must remain usable
   without descriptors; selected decoding still requires a definition. An outage
   must not silently become an authoritative missing-definition result.
3. **Provider durability and capacity.** Keep provider identity independent of
   implementation vocabulary. Qualify a non-S3 provider and its startup/dependency
   boundaries. Use RustFS for local performance and LocalStack for S3 correctness;
   record correctness parity separately from timings. Increase offered load in
   the existing multi-JVM benchmark, retaining latency distributions, pool sizes,
   provider failures and database contention. The four- and eight-client measurements
   showed no replica speedup; neither is a saturation or scale-out result. Eight
   clients exposed maximum-size slot-snapshot over-reservation, now fixed with
   exact-size reservation and a SQL length gate. The complete repeated workload
   passed without increasing its 128 MB payload budget. Retained manifest
   reconciliation now also reserves actual length after locked metadata checks,
   with a second size-gated SQL read. Qualification must include that extra round
   trip; no memory optimization alone establishes a latency improvement.
   Redis now has real SIGKILL/restart coverage for AOF with `appendfsync always`
   and `noeviction`: exact bytes/metadata and copies survive, and a second crash
   preserves reclamation of an object already recovered from the first crash.
   A persistence-disabled control loses non-expiring objects as expected. This
   establishes process-crash behavior only. Managed archival activation remains
   gated on immutable identity/key policy, shared lifecycle/recovery qualification,
   and explicit deployment durability requirements; it is not enabled by this test.
   Sol's activation review identified a concrete next slice: Redis's ordinary PUT
   overwrites its hash and COPY uses replacement, so versionless published keys need
   an explicit immutable-write policy. Assess an opt-in atomic create-if-absent path
   using the existing conditional-write capability, with real Redis tests for racing
   writers and exact retry after lost acknowledgment. A content hash alone is not
   permission to overwrite. Keep policy identity distinct and preserve ordinary byte
   store semantics for existing consumers. Redis's buffered InputStream path does not
   advertise streaming writes; qualify bounded allocation before claiming that capability.
   Reuse `SelectedBlobBacking`, `ManagedArchiveServices`, `ArchiveObjectRecovery`,
   `ArchiveCleanupLedger`, and `ArchiveObjectReader` for shared lifecycle qualification.
   Keep the managed-host activation guard until late writes, cleanup, read pins,
   recovery and deployment durability pass; a provider-only test cannot remove it.
   The Redis byte adapter now implements the existing bounded conditional-write and
   authoritative-read SPI with atomic Lua operations. This is a prerequisite only:
   ordinary PUT/COPY retain replacement semantics, and content ETags are not epochs.
   An opt-in Redis `create-only` byte policy now uses disjoint v3 physical keys and
   identity, rejects expiry and refuses replacement across byte/stream PUT and COPY.
   Its 9 MiB limit does not expand the existing conditional bound. The managed host
   has not selected this policy: shared writer integration, archival size requirements,
   identity binding, delayed-write fencing and activation qualification remain open.
   Deleting a create-only key still permits late recreation, explicitly tested; this
   policy must not be treated as a tombstone or proof of writer quiescence.
   `RedisArchiveLifecycleIT` now composes the existing archive writer, reader,
   operations and recovery with real PostgreSQL and create-only Redis. Unary
   publication/reuse/historical reads pass locally and through in-process gRPC;
   real delayed PUT completion after reclamation fails SQL verification and a
   later tombstone cleanup removes its bytes. Read pins prevent cleanup across
   logical deletion, and lost reclamation acknowledgment remains retryable.
   This is library composition, not `RepoServices` managed activation, streaming
   support, scoped authorization qualification or Redis durability qualification.
   The host currently requires `STREAMING_WRITE` for managed ingestion as a whole.
   Before changing that gate, design an explicit bounded-ingress profile with
   per-operation availability, aggregate allocation bounds and unsupported streaming
   behavior. Do not grant Redis a streaming capability or silently buffer an otherwise
   unbounded upload. Keep provider identity and lifecycle selection common to both
   profiles, and qualify restart/cleanup behavior before publishing deployment examples.

## Work gated by publication and retention guarantees

4. **Recovery.** Follow the [recovery design](repository-publication-recovery.md).
   Original-owner stage reconciliation now has real lost-acknowledgment and
   fresh-process forced-exit evidence without a capability handoff file. Claim
   expiry and transfer refuse the stale handle. The bounded manager now owns
   restoration lifetime and shutdown. Next, durably register ordinary runtime
   sessions. Claimed owner heartbeats now renew the owner and execution claim in
   one transaction; expired or transferred claims cannot be revived. This does
   not automatically register ordinary sessions or enable transfer. Also qualify
   the host ownership protocol; a shared token is not proof
   that its previous process is dead. Keep automatic claim transfer off
   until delayed provider writes and cleanup across claim loss are qualified.
   An internal opt-in session now registers claim, preparation and
   modes before admitting a claimed owner, with lost-acknowledgment tests at all
   four stages. Ordinary runtime creation is still unchanged. Before enabling it,
   qualify retained registration lifetime and cleanup. Scoped request authorization
   is now separate from the private journal capability. The
   scoped SQL fixture and production-JAR journaled provider matrix now pass with
   the actual account-bound caller lacking process authority. Existing-source update,
   typed rejection, CREATE rollback/lost acknowledgment, decision acknowledgment loss
   and exact retry are covered; cross-account calls are denied before journal rows or
   upload-backend/schema resolution and again on replay. Setup publishes the writable
   source with process authority. This does not establish scoped absent-destination
   creation or public transport authentication. The
   execution stage now uses the durable assessment-start marker for journaled
   sessions and commits it before CREATE; ambiguous marker acknowledgment leaves
   local staging sticky. Specify retention and
   cleanup for registered commands that later fail policy or ACL checks.
   Registered sessions now pass the real-provider acceptance/rejection matrix,
   including CREATE rollback and lost CREATE/decision acknowledgments. Separation of
   a host-owned journal capability from the actual scoped caller for V82/V83 is now
   implemented; bootstrap/readCommand remain process-only. Current destination/source
   access and revision preflight run before registration and before the sticky V83
   marker, using the frozen plan. Authoritative checks still repeat at mutation;
   separate preflight and journal transactions cannot exclude a concurrent ACL change.
   Before any possible journal commit a denial may discard the in-memory entry;
   afterward retain its exact identity for authorized retry or explicit cleanup.
   Scoped creation of a truly absent destination is currently rejected by replay
   authorization; resolve that policy explicitly, without widening journal access
   into a document-creation grant.
   A private opt-in session manager now retains exact identity after registration
   may have committed and releases proven pre-registration failures when idle.
   It refuses the older unjournaled recovery replacement path; default host/runtime
   construction remains unchanged. Qualify explicit abandonment and cleanup of
   retained registrations, including conservatively retained pre-SQL capacity
   failures, before host activation. Shutdown/drain does not prove restart recovery.
   Initial claim and preparation now commit atomically. Real SQL regression tests
   cover rollback, lost acknowledgment, cancellation before/after insertion and
   commit, exact retry identity and unchanged lease. The earlier claim-without-seeds
   window is closed for this path. Fixed modes and owner admission remain later
   transactions; qualify interrupted registration in a fresh process before ordinary
   activation. Never turn an explicit claim-only row into permission to invent
   replacement seeds.
5. **Restore, pruning and backup.** Test retained schema/content reachability,
   active read and pending-operation pins, current ACLs and failure recovery.
   Restore publishes through the same concurrency and validation boundaries;
   copying stored bytes alone is not a restored document. Demonstrate that
   pruning cannot remove referenced schema assets or provider versions, including
   while restore or recovery is in flight.
   The reviewed [restore contract assessment](repository-historical-restore.md) covers the existing
   publication command, historical materializer, and current authorization. Distinguish
   revision restore from `DocumentPublicationRestoration`, which resumes an interrupted
   execution. Restore must name an exact source revision, retain its complete schema
   closure, and publish a new destination revision against an expected current revision.
   Historical ownership metadata is provenance, not a grant. Recheck current source
   READ, destination WRITE, and current admission policy; preserve historical bytes
   and definitions without silently substituting the latest registry definition.
   Record policy incompatibility as an explicit refusal rather than rewriting history.
   Review whether the existing reuse contract expresses this before adding fields.
   One concrete mismatch is already identified: `PublicationReuse.source` is a
   pre-change current-revision dependency, and `DocumentPublicationCommand` rejects
   a source revision different from the destination's expected revision at the same
   address. An older same-address restore therefore cannot be represented by simply
   filling in today's reuse fields. Preserve that concurrency check; assess an explicit
   historical source selector with retained-read protection instead of weakening it.

   Current deletion protection is stronger than a completed pruning implementation:
   V48 rejects schema artifact mutation pending a cleanup protocol; V55 makes revision
   schema references immutable. `DocumentSchemaRetention` binds exact checked evidence
   to publication, and `DocumentHistoricalSchemas` replays retained definitions without
   a registry. Existing physical retention tests cover reference/cleanup transaction
   ordering, but these protections do not establish a supported history-pruning path.
   Do not relax either guard merely to make a cleanup test pass. Before enabling deletion,
   account for committed revisions, staged operation claims, assessments, active reads,
   restore operations and future JCR references under one reviewed liveness decision.
   Required restore acceptance cases include registry absence, policy change, revoked
   access, stale destination revision, shared-byte reuse, failed finalization, and a
   pruning race with a held restore read. Backup qualification must restore the matching
   SQL metadata, schema artifacts and exact provider identities into an isolated host;
   a SQL dump alone is insufficient evidence.
   This slice concerns ProtoMolt document revisions. It does not implement JCR version
   restoration: frozen graphs, child identity collisions, strong references and
   checked-in restrictions remain in the optional content-repository assessment.
   Keep the underlying publication transaction composable across multiple objects.
6. **Bounded hydration.** Reuse parts and rendition mechanics, with an immutable
   base and an explicit pending revision. Require a sealed component manifest,
   idempotent patch identities and explicit finalization. Test duplicate/missing
   chunks, stale bases, cancellation/expiry, invalid assembled content and atomic
   reader visibility. Normal readers and semantic review see only complete
   committed revisions. Arbitrary field merging remains outside this slice.

## Existing schema components to reuse carefully

`DocumentSchemaAdmission.Resolver` already selects a definition per occurrence;
its host captures authorization context. `SchemaRegistryStore` is an existing
registry abstraction. `ToolkitSchemaResolver` already handles registry, source
and descriptor-set inputs for actions. Reuse these capabilities without adding
action-layer dependencies to the repository admission leaf.

`ConfluentSchemaRegistryLoader` has a per-loader schema-ID cache and a separate
discovery cache. `ApicurioDescriptorLoader` has artifact-name and negative lookup
caches. These are existing registry loaders, not evidence that repository cache
isolation, immutable archival identity or byte budgets are implemented. In
particular, an artifact-name cache cannot stand in for an exact retained artifact
lookup. Apicurio's bulk inventory logs and skips individual load failures; an
admission adapter must preserve a distinction between incomplete discovery,
authoritative absence and failure rather than treating a partial inventory as
proof that a definition does not exist.

## Architectural requirement that applies throughout

Review new transaction and identity boundaries against the
[optional JCR assessment](repository-jcr-compatibility.md): reusable atomic
multi-object commits, stable identities, and reference retention belong in the
foundation. JCR sessions, workspaces, node types and version restoration belong
in the optional content extension. No base storage module gains JCR dependencies,
and no compliance claim precedes capability-specific conformance evidence.

## Current bounded verification

`DocumentRetainedPathMaterializationTest` now also verifies selected wire evidence
against changed revision, account and ordinal; altered Any bytes; corrupt
descriptor/metadata artifacts; and changed occurrence paths. Every cancellation
checkpoint must propagate cancellation and release all verification reservations.
The fixture uses actual descriptor and metadata encodings. This does not prove
remote-client behavior, authentication, or managed-host deployment.

## Registration inspection checkpoint

The private registration inspector now distinguishes partial registration states
using the supplied original live claim, bounded journals and coherent binding
checks. Five PostgreSQL cases plus 34 journal regressions pass. It does not restore
execution or establish coordinator quiescence. Continue both the recovery ownership
protocol and the independent restore contract assessment above; this checkpoint
closes neither workstream. See [inspection evidence](../evidence/repository/2026-10-05-registration-inspection/README.md).

## Next recovery slice: pre-owner abandonment

The private V85 primitive now records an explicit durable abandonment marker for
exact registrations with preparation or modes but no admitted command or owner.
The private session manager now uses confirmed abandonment for idle-entry cleanup.
Bind it to the original claim and preparation identity;
serialize marking against owner admission with the existing claim-first lock order.
If owner admission wins, abandonment must refuse. If the marker wins, preparation
retry, mode binding and owner admission must refuse. Only a confirmed durable marker
may release retained session-manager capacity. Inspection, expiry and lost
acknowledgment alone cannot establish abandonment.

Acceptance requires real PostgreSQL races, lost preparation/modes acknowledgments,
stale or transferred claims, and capacity release only after confirmed marking.
New marker insertion still requires a live original claim. Private read-only exact
confirmation after expiry or transfer and authorized ABANDONED replay now permit
idle session eviction. An absent marker, inspection phase or expired lease alone
does not permit eviction. Public runtime execution returns FAILED_PRECONDITION for
authorized abandoned commands. Ordinary journaled runtime activation remains off.
Owner-admitted or assessment-started registrations remain outside this slice:
qualify coordinator quiescence and delayed real provider writes before takeover or
abandonment. Continue restore admission independently of that recovery protocol.

## Restore assessment checkpoint

Restore has also advanced independently: private SQL-backed typed assessment now
owns fragment copies and a source pin, checks exact source/evidence identities,
and applies the supplied current policy. Source revocation is checked during
delivery, including on errors. See the [assessment checkpoint](repository-historical-restore.md#sql-backed-current-policy-assessment-checkpoint).
This does not close the restore task: destination authorization, active-policy
fencing, command integration and atomic historical reference publication remain.

## Next non-S3 slice: explicit bounded ingestion

Follow the [bounded ingress design](repository-bounded-ingress.md). The reusable
`ArchivePutAdmission` gate now bounds already-decoded unary save admission before
engine copies and storage work. Real Redis library/in-process tests cover capacity,
oversize refusal, delayed-write drain, reuse and historical reads. The optional
composition refuses streaming and bridge generation. An internal bounded archive-only
host now passes Redis save/history/restart and delayed-write shutdown tests. Public
standalone environment activation and HTTP admission remain unfinished. General transport
startup still rejects the internal profile. A dedicated authenticated archive-only
Netty mount now shares the pre-protobuf gate and byte budget with archive writes;
all exposed unary methods are explicitly reviewed. Real Redis tests cover local
and remote retry/history behavior and cancellation without early resource release.
Startup tests also verify physical identity conflicts preserve the original
binding and release newly acquired providers. An explicit public Java factory and
plain-value options now support embedding, with a tested guide. Full repository
parity, standalone activation and response/read-memory bounds remain open. The next
standalone slice requires explicit account/drive bootstrap through the local port;
the archive-only listener cannot provision a drive. Keep environment parsing strict
and test process startup, restart and failed bootstrap before publishing a launch guide.

The default `RepoServices` managed profile still requires streaming, non-expiring
writes and reclamation. `ArchiveObjectWriter` distinguishes byte-array
staging from streaming staging; `RawIngestionOperations` requires streaming.
The internal bounded profile preserves those operation distinctions, shared
provider identity and cleanup without changing default capability requirements.
Before activation, specify aggregate reservations before ingress allocation,
per-request size limits, capacity retention through delayed provider completion,
and which streaming operations report unsupported. Local host evidence does not
establish transport admission or deployment durability. This remains independent of restore
and of the RustFS saturation measurements.
