# Remaining repository goal work

This is the working order for the additions to the repository composition goal.
It does not replace the [design](repository-composition.md) or declare unfinished
features available. Recovery is one workstream, not the whole goal.

Repository network startup now requires explicit operator authentication, and TCP
repo-backed storage requires its own upstream credential. This closes the open
operator-listener prerequisite. It does not provide key-specific absent-destination
creation authority; that grant still needs design, shared admission checks, and
replay/revocation/race tests. See [authentication evidence](../evidence/repository/2026-10-06-required-network-authentication/README.md).

Initial journaled session admission now composes claim, binding, preparation,
modes, operation and first owner in one SQL transaction. Shared standalone helpers
retain legacy partial-journal behavior; existing partial rows are not retroactively
recovered. Six insertion rollback tests and the 202-case affected recovery run
qualify the atomic SQL boundary. The five-case process suite now also qualifies
SIGKILL before and after initial commit, followed by public retry in a fresh JVM.
The focused admission/retry test counts two real commits per call; the small RustFS
mode diagnostic is retained separately. Additional admission races, controlled
capacity measurements and publication-stage performance work remain acceptance work. See
[atomic admission evidence](../evidence/repository/2026-10-06-atomic-initial-admission/README.md).
Scoped mode comparison also composes its retained reads while preserving authority
checks; its focused proof is [recorded separately](../evidence/repository/2026-10-06-composed-mode-verification/README.md).

The internal managed journaled host now composes registration closure, sessions,
scopes, uploads, readers and owned schema workers before V91 local-drain attestation.
It retains the immutable post-barrier identity snapshot across retries and terminal
cache eviction. Failed restoration retains its exact owner identity; closing loaded
restoration releases borrowed resources without dropping the shutdown obligation.
Missing claims remain unresolved and uncertain replies require exact confirmation.

The narrow managed factory accepts a trusted account/principal/operation authority
resolver and an explicit external-worker lifecycle. Public service builders remain
ordinary. No claim token is exposed, and no successor or remote-effect quiescence
is granted by this local attestation. A partial SQL failure keeps shared resources
available until every captured marker is confirmed. Startup observation failure
leaves schema access with its caller and cleans up newly registered readers.

[Managed-host qualification](../evidence/repository/2026-10-06-managed-local-drain/README.md)
now covers a held actual Git descriptor load, two retained operations, a real SQL
failure on the second attestation, retry preserving the first marker, and startup
ownership. The second operation is owner-admitted; the test does not claim two
independent Git loads. The managed factory also has an opaque successful-publication
and exact receipt-replay control: completed sessions require neither V90 nor V91
markers. The full production-JAR regression passed. Future public publication
transport still needs accepted-call drain
before transport teardown. Safe successor execution and late provider effects
remain separate work.

V91 closes execution across all claim epochs. Expiry of an unchanged claim can
still permit exact attestation; transferred authority cannot create a predecessor
marker. Exact confirmation survives transfer. Preserve recovery-only cleanup
fences, retained definitions, reader pins and provider tombstones.

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
admissions. The two-source mixed-mode regression now refuses a typed source in
either position of an opaque target member, checks the specific downgrade refusal,
and verifies both pins and payload reservations drain. This is real SQL/descriptor
qualification with synthetic provider observations, not provider-read proof; durable
publication qualification is recorded below.
The SQL assessment suite now also covers one member assembled from two historical
sources, including second-source access revocation and release of both pins. The
ordinary registry resolver is unavailable in that fixture. This closes the host
routing test gap, not the provider-read or publication gates.

The internal unclaimed historical path now retains observed evidence through CREATE,
using current-policy/destination authorization and shared historical physical/slot
locks in its transaction. A production-JAR host qualifies the successful path with
real versioned provider reads. The same harness now injects acknowledgment loss after
a real historical CREATE commit, discovers the original stage and verifies retained
evidence. It also refuses a borrowed plan after its owner closes. Historical-specific
CREATE-specific policy/access races remain separate from publication qualification.
Internal unclaimed atomic publication now has real-SQL authorization-lock races,
policy rejection, terminal-write rollback, multi-revision selection, and exact replay
coverage. The production-JAR harness adds retained provider reads, post-commit lost
acknowledgment, and mixed fresh-upload/current/historical members with an unverified
upload negative case. These checks do not enable public or claimed restore sessions.
See [mixed provider qualification](../evidence/repository/2026-10-06-historical-mixed-upload/README.md).
The production-JAR probe also combines historical CORE and fresh PARSED fragments
inside one typed member. It uploads to the actual versioned provider, refuses the
whole member without verified upload observations, and verifies exact replay after
publication. A provider reread reconstructs both shapes using only retained schema
artifacts. This covers same-member composition, not public/claimed activation or
pruning/backup completeness.
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

The current independent implementation slice is aggregate archive GetEntry admission,
specified in [the read-response plan](repository-bounded-ingress.md#next-slice-aggregate-archive-read-responses).
Per-object limits currently do not bound a response assembled from many renditions.
The reviewed plan separates engine construction lifetime from transport response
lifetime and includes real-provider local/remote acceptance cases. GetEntry now
has library construction and bounded managed Netty response admission. Real Redis/SQL
tests cover aggregate refusal, historical subset reads and held provider completion.
Managed-host options and transport reservations share the host budget. The seven
read-only unary methods now enforce the same configured response cap before sending,
including metadata and lists. Manifest-bearing reads now also reserve an aggregate
JSON allowance before SQL and retain it through parsing/assembly. A same-statement
SQL size gate excludes oversized JSON from JDBC; version pages are gated in aggregate,
and entry lists share one allowance. Other entry/archive metadata SQL loading,
decoded heap, protobuf construction and local caller retention remain separate.
Mutation acknowledgments need a construction
bound that cannot hide committed success; they are outside this send-time cap. See
[read response evidence](../evidence/repository/2026-10-06-read-reply-cap/README.md). Continue
claimed-session recovery and retention work alongside these remaining limits;
performance qualification must not displace these requirements.

## Independent work that can advance now

1. **Selected historical reads.** The optional Java SPI, wire contract, response
   verifier, remote client and explicit managed gRPC mount exist. Extend malformed
   response and lifecycle qualification at the client boundary. Require local/remote parity,
   current authorization at delivery, cancellation and byte-budget release. Test
   exact retained descriptors with the original registry unavailable. Never
   substitute the latest registry definition for historical evidence.
   The client belongs in the `repo/history/grpc` leaf, independent of the service's
   SQL and provider assemblies. Admission exposes a closeable selected-response
   decode result using the already verified descriptor. Bound inbound messages,
   shared reservations and open results. Existing real-service in-process and managed
   host tests qualify the selected-read path, not every repository operation.
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
   The production-JAR transport probe also replays a captured real response through
   a test endpoint with deliberately changed revision, payload or descriptor bytes.
   The client refuses each response with DATA_LOSS, releases its byte reservations,
   and permits a valid retry on the same one-call client. This is client-verifier
   coverage, not a substitute for the real service's authorization tests. See
   [client evidence](../evidence/repository/2026-10-06-historical-client-verification/README.md).
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
   The current descriptor-graph inventory test enumerates every declared `Any`
   location in `Document`: CORE structured data and PARSED parser shapes. It fails
   when the graph gains an unaccounted route. `DocumentAnyMaterializationTest`
   covers raw preservation without resolution, distinct occurrence-bound definitions
   for an equal type URL, and explicit unavailable outcomes. These are library
   guarantees; new container schemas and public host wiring need their own coverage.
   Persisted historical replay now also covers equal type URLs with distinct retained
   descriptor artifacts across CORE and PARSED occurrences. A fresh SQL reader
   materializes each with its original field definition after active policy changes,
   without a registry dependency. This is SQL/schema qualification with synthetic
   physical observations, not a new provider-read claim. See
   [historical definition evidence](../evidence/repository/2026-10-06-historical-distinct-definitions/README.md).
3. **Provider durability and capacity.** Keep provider identity independent of
   implementation vocabulary. Qualify a non-S3 provider and its startup/dependency
   boundaries. Use RustFS for local performance and LocalStack for S3 correctness;
   record correctness parity separately from timings. Increase offered load in
   the existing multi-JVM benchmark, retaining latency distributions, pool sizes,
   provider failures and database contention. A larger-payload follow-up with
   sixteen clients, 64 KiB values and 64 measured
   iterations/client passed twelve interleaved RustFS windows with exact historical
   reads, terminal replay and durable counts. Two workers with sixteen total SQL
   connections reached about 96 operations/second versus 81 with one worker/eight
   connections; four workers did not beat two. Fixed-total SQL connection profiles
   were slower with more workers. This is a bounded benchmark with growing aggregate
   memory and an uncontrolled host, not linear scaling or a soak. Preserve the
   [measurements and limits](../evidence/repository/2026-10-06-native-64k/README.md).
   The earlier four- and eight-client measurements
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
   establishes process-crash behavior only. The explicit bounded managed profile now
   selects immutable create-only storage; deployment durability still requires
   operator qualification and cannot be inferred from a provider-only test.
   Redis's ordinary PUT overwrites its hash and COPY uses replacement. A content
   hash alone is not permission to overwrite. Keep policy identity distinct and
   preserve ordinary byte-store semantics for existing consumers. Redis's buffered
   InputStream path does not advertise streaming writes.
   Reuse `SelectedBlobBacking`, `ManagedArchiveServices`, `ArchiveObjectRecovery`,
   `ArchiveCleanupLedger`, and `ArchiveObjectReader` for shared lifecycle qualification.
   The Redis byte adapter now implements the existing bounded conditional-write and
   authoritative-read SPI with atomic Lua operations. This is a prerequisite only:
   ordinary PUT/COPY retain replacement semantics, and content ETags are not epochs.
   An opt-in Redis `create-only` byte policy now uses disjoint v3 physical keys and
   identity, rejects expiry and refuses replacement across byte/stream PUT and COPY.
   Its 9 MiB limit does not expand the existing conditional bound. The bounded managed
   host selects this policy with explicit input limits, shared writer integration,
   and persisted provider identity. Full-profile streaming remains unavailable.
   Deleting a create-only key still permits late recreation, explicitly tested; this
   policy must not be treated as a tombstone or proof of writer quiescence.
   `RedisArchiveLifecycleIT` now composes the existing archive writer, reader,
   operations and recovery with real PostgreSQL and create-only Redis. Unary
   publication/reuse/historical reads pass locally and through in-process gRPC;
   real delayed PUT completion after reclamation fails SQL verification and a
   later tombstone cleanup removes its bytes. Read pins prevent cleanup across
   logical deletion, and lost reclamation acknowledgment remains retryable.
   Those cases qualify library composition. The separate bounded `RepoServices`
   profile and production launcher now add authenticated Netty, bounded admission,
   restart without relocation, concurrent drive bootstrap, and graceful shutdown
   during SQL commit and short real Redis acknowledgment delays. Scoped authorization,
   full repository parity and deployment durability remain distinct gates. The
   default profile still requires streaming support. Do not grant Redis a streaming
   capability or silently buffer an otherwise unbounded upload. Keep provider identity
   and lifecycle selection common to both profiles. See the
   [bounded-ingress evidence](repository-bounded-ingress.md) for precise boundaries.

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
   transactions. Initial registration now has fresh-process crash qualification:
   death before its COMMIT leaves neither row; death immediately after COMMIT retains
   the exact preparation, original claim and unchanged lease for inspection/retry.
   The reader receives no private input or token file. Mode binding now also has
   before/after-COMMIT process-death coverage: preparation survives both cases,
   while only a committed mode row is loaded and retried unchanged. Inspection
   distinguishes PREPARATION_ONLY from MODES_BOUND without admitting an owner or
   advancing execution. Owner admission also has before/after-COMMIT crash coverage:
   its command and owner appear together or neither appears. Fresh inspection
   distinguishes MODES_BOUND from OWNER_ADMITTED, and exact retry preserves the
   original owner nonce, generation and lease. No assessment or upload is started.
   These test-only private SQL readers are not a production recovery endpoint.
   Host quiescence and ordinary session activation remain open. Never turn
   an explicit claim-only row into permission to invent
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
   `PublicationHistoricalReuse` now supplies the explicit historical source selector
   and retained-read binding. Ordinary `PublicationReuse.source` remains a pre-change
   current-revision dependency; its concurrency check has not been weakened to admit
   older same-address revisions. Internal unclaimed publication exercises historical
   selectors; public and claimed execution remain separate activation gates.

   Current deletion protection is stronger than a completed pruning implementation:
   V48 rejects schema artifact mutation pending a cleanup protocol; V55 makes revision
   schema references immutable. `DocumentSchemaRetention` binds exact checked evidence
   to publication, and `DocumentHistoricalSchemas` replays retained definitions without
   a registry. Existing physical retention tests cover reference/cleanup transaction
   ordering, but these protections do not establish a supported history-pruning path.
   Do not relax either guard merely to make a cleanup test pass. Before enabling deletion,
   account for committed revisions, staged operation claims, assessments, active reads,
   restore operations and future JCR references under one reviewed liveness decision.
   The internal `DocumentRevisionRetentionInventory` now supplies a bounded,
   single-statement metadata snapshot for an exact sealed revision. It counts native
   object references alongside retention mirrors; normalized schema revision,
   operation-claim and assessment references; source-revision assessment slots and
   physical read-pin rows. It includes retiring/reclaiming state. V88 adds indexes
   for the source-revision lookups. Positive counts, shared revisions, pin release,
   assessment release and migration over an existing revision have real PostgreSQL
   coverage. See [inventory evidence](../evidence/repository/2026-10-06-revision-retention-inventory/README.md).
   This process-authority-only helper is observational: it neither locks a future
   deletion decision nor grants deletion. It always reports unindexed preparation
   journal selectors and future content-repository references as unresolved.
   Expired or terminal claims are counted until explicitly released. V48/V55 and
   revision immutability remain unchanged. Next, resolve pending command references
   and design atomic pruning with the same lock order used by reference acquisition;
   do not derive a deletable flag from an earlier inventory snapshot.
   Source inspection confirms that the current preparation journal cannot register
   historical selectors: `DocumentPublicationPreparationRecord` reconstructs through
   ordinary upload preparation, which calls `requireExecutionSupported`. Historical
   preparation uses a separate internal unclaimed path; claimed historical admission
   remains refused. An indexed journal-selector projection would therefore be empty
   for currently supported registrations. Do not add it or relax that guard solely
   to make the inventory appear complete. Design claimed historical ownership, source
   pin lifetime, canonical decode and atomic reference acquisition together before
   activation. Unknown/legacy journal coverage remains conservative until verified.
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
Internal unclaimed publication now adds destination authorization, active-policy
fencing, command integration and atomic historical reference publication. Real SQL
and production-JAR provider probes qualify that path. Public/claimed activation,
automatic claim transfer, complete recovery and safe pruning remain open.

## Next non-S3 slice: explicit bounded ingestion

Follow the [bounded ingress design](repository-bounded-ingress.md). The reusable
`ArchivePutAdmission` gate now bounds already-decoded unary save admission before
engine copies and storage work. Real Redis library/in-process tests cover capacity,
oversize refusal, delayed-write drain, reuse and historical reads. The optional
composition refuses streaming and bridge generation. An internal bounded archive-only
host now passes Redis save/history/restart and delayed-write shutdown tests. The
standalone environment launcher is implemented; general HTTP is outside this
profile. General transport startup still rejects the bounded profile. A dedicated authenticated archive-only
Netty mount now shares the pre-protobuf gate and byte budget with archive writes;
all exposed unary methods are explicitly reviewed. Real Redis tests cover local
and remote retry/history behavior and cancellation without early resource release.
Startup tests also verify physical identity conflicts preserve the original
binding and release newly acquired providers. An explicit public Java factory and
plain-value options now support embedding, with a tested guide. Full repository
parity and response/read-memory bounds remain open. The standalone
`RepoBoundedArchiveMain` now bootstraps its explicitly selected account/drive through
the local port before opening its archive-only listener. Child-process tests cover
startup, restart without relocation, failed bootstrap, and SIGTERM during a real SQL
commit wait. Real Redis reply-gate tests cover short delayed acknowledgments and a
lost acknowledgment after provider write, followed by retry and normal lease-expiry
recovery. A separate standalone SIGKILL test now covers service death after a verified
Redis write but before SQL publication, restart, retry, and normal lease-expiry cleanup.
Provider-crash durability, delayed writes surviving process death, and deployment
qualification remain separate gates; see
the [bounded-ingress plan](repository-bounded-ingress.md).

The default `RepoServices` managed profile still requires streaming, non-expiring
writes and reclamation. `ArchiveObjectWriter` distinguishes byte-array
staging from streaming staging; `RawIngestionOperations` requires streaming.
The internal bounded profile preserves those operation distinctions, shared
provider identity and cleanup without changing default capability requirements.
The qualified archive-only listener applies aggregate reservations before protobuf
decoding and per-request limits, retains capacity through delayed provider completion,
and leaves streaming unavailable. This transport evidence does not establish general
repository parity, response/read-memory bounds or deployment durability. This remains independent of restore
and of the RustFS saturation measurements.

## Coordinator identity and cache observability checkpoint

The private journaled manager now binds its initial claim and preparation to one
incarnation in the same transaction. Other managers cannot resume its live owner;
authorized terminal replay remains available across managers. Legacy registration
cannot retry a bound claim. This prerequisite does not activate ordinary journaled
runtime execution, prove drain, or permit automatic takeover. See the
[identity boundary](repository-publication-recovery.md#private-coordinator-registration-identity-v89).

Independently, the registry adapter exposes bounded, read-only statistics for cache
reuse, actual reads, joined loads and owned bytes. The counters distinguish cold and
warm behavior without weakening per-occurrence authorization. They do not replace
RustFS scaling measurements, provider durability, retention/pruning or the optional
JCR assessment. Continue those slices alongside the local-drain protocol.

V90 adds private SQL admission closure under the original coordinator binding.
Actual new registrations, owners, upload attempts and assessment starts are fenced;
exact retries and same-generation settlement retain their existing authority checks.
Read-only exact drain confirmation survives claim expiry without granting new work.
Full local drain and safe successor execution are still required. This does not
close the independent restore/pruning, transport parity,
provider durability or hydration requirements.

Local provider-start permits now close before runtime session cleanup. Permitted
PUT/read-back calls retain their resources through return; queued refusals do not
cancel siblings or discard their completed observations. Transfer-only idle is not
full shutdown or remote-effect quiescence. The journaled manager now composes this
gate with retained nonterminal V89/V90 identities through a registration barrier.
Next compose full runtime quiescence, preserving uncertain markers, late-effect
tombstones and existing schema/revision retention protections.

## Schema adapter corruption qualification

The real Git registry adapter now has a corrupt-then-repair admission regression.
A digest mismatch must leave no cached artifact, retained load or payload reservation;
repair must trigger a new registry read and allow admission through the same resolver.
See [qualification](../evidence/repository/2026-10-06-schema-corruption-retry/README.md).
This adds adapter coverage without changing production behavior or broadening the
cache into an authorization or validation-verdict cache.

## Graceful successor reservation

V92 adds a private immutable handoff record and atomic expired-claim transfer after
exact local drain. It preserves V89 initial bindings and V90/V91 execution closure.
No runtime invokes this reservation or gains provider access. The next increment
must bind a new owner generation and fresh attempts to that successor, then qualify
late predecessor writes and cleanup isolation. Pre-owner and admitted-owner states
must be handled explicitly. Abrupt-death recovery cannot assume local attestation.
Focused acceptance covers terminal cancellation; a successful-terminal handoff
negative case remains to qualify alongside successor execution.
The [V92 qualification](../evidence/repository/2026-10-06-graceful-handoff/README.md)
records 35 focused cases and the passing production-JAR regression. Exact concurrent
retries converge without a second claim transfer; lost acknowledgments confirm the
original row, and cancellation before commit rolls back both transfer and record.

## Atomic successor-generation registration

V93 installs fresh preparation and modes and advances an existing expired owner in
one transaction. Its proof applies only to those exact records in that transaction;
assessment, attempt and publication paths stay closed. The deferred check requires
both successor claim and new owner leases to remain live through commit. A held
transaction reproduced an expired-owner commit before that additional check.

Next qualify explicit execution authority for the installed generation with current
WRITE/policy/schema/placement checks, fresh attempts, late predecessor writes and
cleanup isolation. Registration checks current source READ but is not an execution
approval. Pre-owner recovery and abrupt-death recovery remain separate required
paths. The provider, historical, pruning, RustFS and JCR work remains in scope.

## Successor execution boundary

V94 adds an exact SQL execution identity and successor binding. This is an internal
boundary, not completed automatic recovery. The private Java activation operation
now separates coordinator authority from the execution caller and supports exact
readback without renewal. Private successor sessions and manager retention now
exist; next qualify the provider effect checks above and end-to-end
recovery alongside those paths. No public API advertises successor execution.
Exact readback after revocation confirms a past commit only; execution must check
current rights again. Conservative maximum-size byte reservations still need load
and fairness measurements before capacity claims.

## Successor manager integration

Private successor sessions attach to the installed owner and restore durable
assessment-start state. The manager now reserves capacity and retains the exact
session before activation can commit. Its registration barrier covers reservation,
activation and attachment. Failed attachment keeps the session and drain identity;
changed handoff/preparation/modes cannot replace it on retry. Closing admission may
refuse attachment after activation commits, so shutdown must reconcile the retained
identity. The graceful real-provider publication probe below now covers the positive
handoff path. Late-effect cleanup and abrupt-death recovery remain required before
enabling automatic recovery. Admission from an expired owner,
revoked policy or closed registration scope must never be inferred from an earlier
activation confirmation.

The production-JAR provider qualification interrupts generation one after a real
upload but before publication and retains its preparation, attempts and physical
object version. It completes local admission/provider/read drain and V90/V91, then
waits for actual database claim and owner expiry. A fresh manager supplies the V92
successor incarnation, installs V93 and activates V94, then publishes the same
command through the real provider. Assertions cover generation-two selection and
receipt replay, distinct attempt/object identities, unchanged predecessor evidence,
retained Any schema binding and released local resources. No lease timestamps are
manually edited. A separate controlled late-effect test now covers a remote PUT
finishing after local drain, as described below.

For that late-effect qualification, the existing transfer tests that pause before
calling the provider are insufficient: their local worker is still active and cannot
legitimately attest local drain. First qualify a bounded HTTP request gate over a
real S3-compatible service. Hold one complete PUT before forwarding, let the real
SDK time out and its local worker drain, then forward the original request after
successor publication and observe the provider's actual response. Preserve signing
and exact backend identity, and explicitly control SDK retries. Holding only a
response tests lost acknowledgment, not a delayed write. Then verify that the old
attempt remains unverified/unreferenced and that another cleanup pass removes only
its exact object versions while the successor stays readable. The test-only
`DelayedS3PutGatewayIT` now qualifies the bounded transport seam and physical
exact-key reclamation against LocalStack. The SDK times out and its inbound handler
drains before an independently buffered request is delivered. The first cleanup
pass observes absence; a second removes the late version while preserving a
prefix-sharing neighbor. `DocumentSuccessorLatePutIT` composes that seam with SQL
tombstones and successor publication. The SDK uses an HTTP proxy while keeping
the original endpoint and registered backend identity. Both V90/V91 drain records
exist and the old SDK is closed before forwarding. After natural lease expiry, a
second manager installs and executes the successor. Recovery observes absence,
the old request reaches LocalStack, and another recovery pass removes its version.
The old object stays unverified and unreferenced; the successor's bytes and receipt
survive. This is an admin/opaque fixture, not additional scoped or typed coverage.
Abrupt death, automatic host recovery, arbitrary SDK framing/retries and sustained
RustFS scaling remain separate unfinished work.

Before adding abrupt recovery, V95 closes a reproduced gap where transferring an
expired, bound but undrained coordinator claim could pass the general SQL mutation
guard without successor activation. Bound operations now require the exact initial
binding or a live successor execution grant. Crash recovery still needs a separate
reservation reason/protocol, retained predecessor read pins and tombstones, a
pre-owner transition, paused-host fencing tests and process-kill/restart evidence.
See the crash boundary in `repository-publication-recovery.md`. Lease expiry is
neither reader quiescence nor permission to prune retained sources.

V96 provides the shared immutable reservation parent while preserving V92's
graceful evidence. V97 adds the private expired-unquiesced SQL source with exact
expired owner and coordinator identities and no synthetic drain records.
Installation/activation now consume a kind-bound common Java proposal; attachment
also confirms its kind and old owner. The private expired Java reservation supports
exact confirmation and retry, but it does not enable automatic lease-based failover
or prove process death. Real-provider publication and delayed predecessor cleanup
now pass through a fresh manager in the same JVM with admin/opaque CORE data.
Qualify a genuinely fresh process and scoped typed recovery, and connect trusted
host discovery/authorization and bounded resource ownership. Graceful fresh-process
bootstrap now has a separate reserved preparation-read authority; its existing V91
fence still refuses the general loader before V94 activation. Fresh-process graceful
discovery and host integration remain unfinished.
V98 handles replacement death between reservation and V94 activation with fresh
identities and phase-specific preparation/owner checks. These states lack a
current-epoch binding and cannot use V97. Host integration of this explicit
supersession protocol is still required before automatic recovery can be claimed.

The fresh-process qualification is now distinct from the same-JVM proof:
`DocumentPublicationProcessRecoveryIT` kills and reaps a writer after a completed
real LocalStack PUT, then starts a new JVM with only the retried public command
and payload. Private SQL discovery, natural expiry and V97/V93/V94 lead to normal
publication. Old bytes remain unverified and unselected, old reader incarnations
remain ACTIVE, and receipt replay performs no BlobStore calls. This removes the
specific missing process-death proof for the admin/opaque post-owner case. It does
not remove the production hosting, scoped typed, pre-owner,
pin reclamation or performance requirements above. Replacement-before-activation
now has the additional process qualification described below.

Private exact-operation discovery now lives in
`RepositoryCoordinatorRecoveryDiscovery` and is used by the fresh-process test.
It reads bounded metadata with SQL timeouts, classifies structural recovery gaps,
and returns expired identities without changing ownership. V97 still rechecks
under locks. Host orchestration, fleet discovery if required by that host, and
automatic retry scheduling remain unfinished; no endpoint is advertised.

`RepositoryReservedPreparation` now reads an exact reserved predecessor without
stamping execution rights. Both graceful and expired reservations use the same
byte-bounded decoder and recheck current caller access before delivery. The real
delayed-provider tests and scoped typed production-JAR probe use this path.
V98 now supplies the immutable unactivated-replacement supersession source and
phase checks. The three-case process test covers a killed writer alone and a
second killed JVM after reservation or installation. Each final JVM receives only
the public command and payload, discovers private recovery state, and completes
publication while preserving old unselected bytes and ACTIVE reader pins. This
closes those admin/opaque process windows; automatic hosting, scoped typed process
recovery, pre-owner recovery and safe pin reclamation remain unfinished.

## Scoped creation authority: review before implementation (2026-10-06)

A caller's account membership and a command's proposed ACL do not authorize a new
object. A grant copied into `RepositoryCaller` also cannot prove live revocation.
The candidate design is a host-issued, durable grant for an exact authenticated
binding, account, operation UUID and canonical command digest. Its target set,
ownership/security, datasource and placement commitments must be exact. The grant
only supplies absent-target creation authority: it grants no source READ,
existing-target WRITE, policy mutation or process authority.

Review the following before implementing the grant:

- Decide how credential identity and generation survive transport binding. Principal
  name alone cannot distinguish two keys for the same principal. The present
  `RepositoryCaller` has no key identity; do not silently treat it as one.
- Check grant liveness under the same transaction and destination revision locks as
  admission/commit. Revocation must serialize with that decision; do not call an
  external policy service while holding those locks. Document lock ordering.
- Apply the shared rule to registration, upload, assessment, final CREATE and
  successor activation. Rejection, stale-condition inspection and PENDING replay
  must not expose absent-target outcomes after revocation. Successful replay uses
  current target READ policy, rather than requiring historical creation authority.
- Define provisioning, expiry, cancellation, bounded retention and recovery behavior.
  An already-started provider effect remains owned by recovery after revocation.
- Prove exact-grant success and wrong account/target/digest/ownership/placement
  rejection; race revocation against admission, upload and commit; race another
  creator; test pending, rejection and successful replay separately.

A private SQL-backed grant is a candidate for making the decision atomic with the
existing ledger. This is design work, not an implemented endpoint or a decision to
put SQL in the byte SPI. Publication transport and host key/account bindings remain
separate unfinished boundaries.

## Scoped creation design follow-up (2026-10-06)

The [scoped creation design](repository-scoped-creation.md) records the remaining
credential-identity, durable authorization, revocation, replay and performance
requirements. It is not an available API. Pending observation now applies current
destination/source access checks, without reapplying revision conditions. This
closes an observation gap before new creation authority is introduced.

## Authenticated identity prerequisite (2026-10-06)

Optional key identity now survives a single resolver lookup, resolver chaining,
gRPC context and the explicit DocumentGrpcService host mapper. The SPI retains
that identity without importing authentication or storage implementations.
Legacy resolvers have no key binding and cannot authorize key-specific creation.
Next implement the durable authority records and install/revoke protocol from
[scoped creation](repository-scoped-creation.md), including lock-order proof and
current-key checks at publication and recovery. Identity propagation alone does
not satisfy those requirements.

## Credential authority foundation (2026-10-06)

V99 now retains current key generation and irreversible per-generation revocation.
The internal port has real SQL coverage for registration, CAS rotation, revocation,
shared readers and revocation waiting, prior-row migration and unsupported isolation.
Publication does not call it yet. Next install the exact-command grant atomically
with a new execution-scope insertion; never lock an existing scope in the shared V79
function. The reviewed [serialization design](repository-scoped-creation.md#transaction-and-revocation-rules)
explains the claim/scope deadlock rejected during review. Test both first-admission
winners before integrating grant checks into the publication transaction.

## Creation grant primitive checkpoint (V100)

The exact-operation installer and revoker now exist internally, with SQL scope
arbitration and credential/placement binding. This supersedes the earlier need to
choose a durable grant representation; it does not complete scoped creation.
Next integrate the check into the shared publication authorization path, deriving
placement hashes from actual selected placements under the domain locks. Cover
registration, assessment, final publication, successor activation and pending/rejected
observations. Successful replay continues to use current target READ policy.
Revocation races, scoped recovery, host provisioning and bounded retention remain.

## Scoped execution and observation integration

The shared grant check is now wired through journaled registration, upload,
assessment, commit and successor activation/attachment with the host's backend gate.
Command-only pending/rejection checks establish visibility, not placement proof;
successful receipt replay checks original key identity and live credentials while
ignoring creation-grant liveness. Local typed/opaque initial publication and
historical byte/schema recovery have real-provider coverage.

Remaining qualification includes scoped successor publication, revocation during
recovery, mixed source/target policy cases under a valid grant,
and expiry at other execution phases. Internal provisioning still needs bounded
retention/lifecycle rules before an external endpoint can be exposed. These checks
do not close recovery discovery, pin reclamation, pruning, provider conformance,
performance or progressive hydration requirements elsewhere in this inventory.

## Scoped successor activation qualification

Real SQL tests now cover scoped successor activation, live attachment and generation-two
upload admission, plus refusal after grant revocation, key substitution or host backend
rejection. Exact immutable activation confirmation after revocation does not allow live
attachment. See [evidence](../evidence/repository/2026-10-06-scoped-successor/README.md).
At that checkpoint manager-level recovered provider publication remained open;
activation tests alone do not qualify automatic recovery discovery or scheduling.

The later [scoped process-recovery qualification](../evidence/repository/2026-10-06-scoped-process-recovery/README.md)
now covers successful opaque publication through the real manager after killing
and reaping the writer JVM. A separate JVM uses the original scoped binding and
durable grant for preparation delivery, activation, publication and receipt replay.
This closes the positive scoped opaque manager path; typed recovery, revocation
during recovery and host policy refusal in this composed path remain open. The
deterministic fixture identity does not qualify network API-key authentication.

## Publication-first revocation race

Typed and opaque real-provider tests now pause the actual final success-writing
transaction before commit and observe grant revocation blocked on its PostgreSQL
PID. Releasing commit lets publication finish, then revocation succeeds; committed
receipt READ remains valid until credential revocation. See
[evidence](../evidence/repository/2026-10-06-scoped-commit-race/README.md).
Revocation-first refusal is now covered at the opaque direct final-commit boundary:
real provider uploads finish first, the publisher demonstrably waits on the grant
row, and committed revocation leaves no destination or success record. A matching
non-revoking control commits. Provider-versioned attempt rows remain for recovery;
this does not yet qualify their cleanup. A third case now proves expiry at final
authorization: the publisher waits while the grant is live, the database clock
passes its immutable deadline, and release causes refusal without publication.
