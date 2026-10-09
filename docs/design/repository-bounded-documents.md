# Bounded document hosts on non-streaming providers

Status: design for implementation. No Redis document host is available yet.
This extends repository composition requirement 3 without changing protobuf
names, field tags, import paths, type URLs or document commit semantics.

## Existing behavior

RepoServices requires S3-compatible backing for full managed documents. That
assembly includes streaming raw ingestion. Redis provides bounded object writes,
not streaming writes: the InputStream overload buffers the declared object.
Advertising STREAMING_WRITE for this adapter would misstate the guarantee.

The bounded archive assembly already qualifies Redis with zero TTL, a finite
object cap within 9 MiB and create-only writes. It disables document publication
and document recovery. Reusing that archive profile unchanged would therefore
leave the document requirement unsatisfied.

Redis replacement storage has a different physical identity from create-only
storage. A document host must select create-only storage explicitly and bind the
resulting identity to the configured generation. Existing replacement generations
must not be adopted or relabeled. SQL document revisions do not require provider
version IDs: immutable unique keys, hashes and provider receipts bind the bytes.
Historical reads must prove this behavior with a versionless provider.

## Composition

Add an explicit bounded-document host option/profile. Reuse the journaled
publication repository, schema lifecycle, historical reader and attempt recovery.
Keep provider creation in the selected factory; no S3 client or fallback belongs
in Redis startup. The profile owns finite object, request, response, concurrency
and aggregate reservation limits. Check uploads against the selected object cap
before staging or provider I/O. Preserve existing conditional-write limits.

Use Redis create-only storage, zero TTL and a finite object cap within 9 MiB.
Require operator retention qualification, including persistence and eviction
policy. Those deployment guarantees cannot be inferred from the adapter's TTL.
Keep object reclamation tied to the original generation and physical identity.

Mount authenticated publication and bounded history APIs. Do not mount legacy
streaming raw ingestion, unrestricted legacy document/archive mutations or bridge
routes through this profile. Unsupported operations must be explicit; do not
route them through a buffered approximation of streaming. Archive-only and
bounded-document profiles remain distinct assemblies over shared primitives.

Accepted publication, provider, schema and read work retain the provider and SQL
resources until drain completes. A timeout keeps those resources available for
retry. Recovery uses the same attempt fences and persistent cleanup state as the
S3-backed composition.

## Acceptance work

1. Add a real PostgreSQL/Redis host fixture with a selected-provider spy that
   delegates Redis calls and fails if S3 is selected. Invalid settings must fail
   before opening an unrelated provider or registering readers.
2. Exercise typed publication, invalid-candidate rejection, exact receipt replay
   and historical decoding using retained descriptors after registry removal.
3. Publish multiple revisions, restart the host and verify historical bytes by
   immutable object identity despite absent provider version IDs.
4. Inject lost PUT acknowledgement and late PUT completion around the real Redis
   adapter. Prove no stale overwrite, no publication of unverified bytes, and
   eventual reclamation without touching retained neighbors.
5. Enforce per-object and aggregate bounds before provider I/O; verify local and
   authenticated gRPC outcomes agree, including authorization and cancellation.
6. Hold real provider/schema/read work during shutdown and prove resources remain
   usable until the accepted work exits. Verify unsupported routes are absent or
   refused rather than entering an unbounded path.
7. Preserve S3 host behavior and the archive-only Redis suite. Document the new
   option only after the host and lifecycle acceptance checks pass.

## Foundation and JCR boundary

This work changes provider composition and bounded admission, not the repository
transaction model. Reuse the foundation's stable identity, multi-object commit,
retention and authorization primitives. It does not claim JCR workspace, session,
node-type, reference or restoration compliance. The optional JCR assessment remains
in repository-jcr-compatibility.md; no JCR dependency enters storage modules.

Sol reviewed the current source and identified the streaming, immutable-storage,
assembly and recovery constraints above. Implementation and qualification remain
open.

The public publication parser already limits the envelope to 10 MiB and aggregate
uploads to 8 MiB. Those checks do not enforce a smaller configured Redis object
cap. The direct runtime accepts part maps, so either gate that entry before SQL
admission or leave it inaccessible from this profile. Reuse backend qualification
checks from the archive mode, not ArchivePutAdmission's rendition/manifest model.
The general services() and startNetty paths must respect the profile's method set.

## Upload-bound primitive

DocumentPublicationInput now accepts an optional smaller upload-object maximum,
and the runtime repository facade applies that maximum before receipt lookup,
storage selection and durable admission. The default remains 8 MiB. References
to existing historical or reused objects are not incoming uploads and are not
rejected by this upload cap.

The cap is an endpoint replay policy as well as an admission limit: reducing it
can prevent a previously valid full-body request from retrieving a terminal
receipt through that endpoint. The durable receipt remains stored. Do not lower
an established endpoint cap without an explicit replay-access plan. A distinct
receipt-only API would require separate contract and authorization review.

## Reviewed assembly map

Use a third explicit host profile, distinct from full and bounded-archive modes.
Existing builders retain their current defaults. Validate the new profile before
opening SQL or provider resources. Select create-only Redis for this profile;
require bounded reads, non-expiring writes and physical reclamation without
requiring streaming support.

Build document-attempt recovery and ManagedDocumentServices, with the publication
facade's object maximum capped by both the provider limit and the 8 MiB protocol
limit. Leave raw ingestion, raw recovery and managed archive unset. Reuse existing
shutdown ordering rather than introducing another host with duplicate cleanup.

The service list contains configured authenticated publication and history
adapters only. Restrict legacy repository/archive/drive accessors, HTTP, the
archive listener and package-level direct publication-runtime access. Trusted
drive provisioning needs an explicit bootstrap path rather than a general mounted
DriveService. Continue document reader reconciliation, attempt recovery and schema
workers; omit legacy purge/sweeper and unrelated raw/archive maintenance.
A host fixture must enumerate rejected access paths, not merely prove a successful
publication. Sol reviewed this assembly map against the current source.

## Internal implementation checkpoint

A package-private Redis document profile now passes typed Any publication,
receipt replay and historical validation after the live resolver closes.
Evidence: `docs/evidence/repository/2026-10-07-bounded-document-startup`.
The public factory remains unavailable pending the other acceptance cases above.

## Uncertain write acceptance status

A real Redis PUT followed by an injected acknowledgement failure now has a test:
bytes exist, but the document revision does not advance. Immediate exact retry
is fenced because the selected attempt is unverified. A separate operation can
publish; that is not recovery of the failed operation. Before exposing this
profile, qualify recovery and reclamation of the original attempt, including
late completion and preservation of committed neighboring objects. Evidence is
in `docs/evidence/repository/2026-10-07-bounded-lost-ack`.

## Orphan cleanup checkpoint

Real-expiry Redis cleanup now passes in a fresh JVM and preserves both committed
histories. The test exposed and fixed repeated selection of retained native
objects. All object references now exclude attempts from scan and locked claim;
the final database guard remains unchanged. Evidence:
`docs/evidence/repository/2026-10-07-bounded-orphan-cleanup`.
Late writes, cleanup-provider failure and active-work shutdown remain open.

## Cleanup retry checkpoint

Real Redis reappearance and exact-key cleanup retry now pass. An injected error
is returned and stored as retry state; a later real reclaim confirms absence.
Both committed histories remain valid. Evidence:
`docs/evidence/repository/2026-10-07-bounded-cleanup-retry`.
This models reappearance with a new PUT and uses direct recovery calls. A delayed
network PUT race, scheduled recheck timing and active-work shutdown remain open.

## Delayed request qualification plan

The remaining late-write test must hold the complete conditional-write EVAL frame
sent by the real Redis client, before forwarding it to Redis. Bound retained
frame count and bytes. Other connections must remain usable for recovery. Let the
original client time out or cancel normally; do not clear its interrupt to make
an adapter call succeed. A partial request is an error, not a captured write.

Wait for the actual attempt lease to expire, then use recovery with the original
generation and physical identity to record ABSENT while the key is absent.
Forward the exact captured frame once and require Redis's real success response
and matching bytes. The failed publication must still have no verified attempt,
committed revision or object references. Recover that exact attempt again and
verify absence while both retained neighboring revisions remain readable.

This uses an explicit recovery pass and does not qualify the scheduled recheck
interval. Keep provider-call shutdown as a separate case: a short held reply
must retain resources until the accepted call exits. Neither a dropped reply nor
a manually repeated PUT establishes delayed delivery of the original request.
Sol reviewed this test approach; implementation and execution remain open.

## Read shutdown checkpoint

Library historical reads now qualify resource retention across caller cancellation
and timed shutdown. The test holds a completed real Redis read inside its provider
call, verifies SQL remains usable and Redis remains open, then releases the call
and retries shutdown. Evidence:
`docs/evidence/repository/2026-10-07-bounded-read-shutdown`.
Active publication shutdown, gRPC cancellation and delayed requests remain open.

The delayed-request transport fixture now passes with the real Redis adapter:
`docs/evidence/repository/2026-10-07-delayed-redis-request`.
It binds capture to the physical key and forwards the original frame once after
the caller disconnects. Repository attempt/tombstone integration remains open;
direct provider absence is not evidence of durable SQL cleanup state.

## Remaining profile checks before public configuration

After delayed-write recovery, qualify a contract-invalid typed candidate through
this Redis host. Use valid protobuf bytes and matching upload lengths/digests,
with a retained descriptor annotation that the candidate violates. Require a
durable rejection with assessment identity, no current revision, and the same
receipt on local and authenticated gRPC replay after closing the live resolver.
A checksum or request-parser error does not establish runtime validation.

Also hold an accepted publication across timed shutdown and qualify authenticated
gRPC cancellation during actual provider work. The existing read gate only covers
historical reads. Retain SQL, provider resources and reservations until accepted
work exits; refuse new work after admission closes. Keep provider-write and schema
load windows explicit rather than treating one held stage as evidence for both.

Once these cases pass, expose bounded document configuration through the existing
composition API with explicit object, shared payload and transport limits. Redis
persistence and eviction remain operator qualifications. Sol reviewed this order;
the public factory is not available yet.

## Delayed request recovery checkpoint

The production-JAR host now qualifies late delivery of an original Redis request
after actual lease expiry and durable ABSENT cleanup. Late bytes remain unverified
and unreferenced; current revision identity is unchanged, a second exact recovery
removes the bytes, and both committed histories remain readable. Evidence:
`docs/evidence/repository/2026-10-07-delayed-document-recovery`.
The parent proxy retains the original frame across Redis restart. The final
reclaim is direct; scheduled recheck timing and active publication shutdown remain
open. The public profile is still unavailable pending the checks above.

## Typed rejection checkpoint

The bounded Redis host now rejects a valid protobuf payload that violates a
retained CEL annotation. The assessment identifies the intended rule and CORE
root; the operation has one durable rejection, no success receipt and no committed
revision. Local and authenticated gRPC retries return the same rejection after
the live schema resolver closes, without another provider upload or read.

This exposed a production bug: loading retained assessment inputs used read-only
SQL calls that bounded transaction views prohibit. The loader now reads its
manifest, policy, roots and descriptor artifacts in one bounded transaction.
Provider reads and CEL evaluation remain outside that transaction. Tests cover
both transaction modes for successful loading, capacity exhaustion, cancellation,
source revocation, corrupted retained data and interruption. Evidence:
`docs/evidence/repository/2026-10-07-bounded-typed-rejection`.

Publication shutdown and gRPC cancellation remain open. Test provider-write and
schema-load windows in separate hosts. In the confirmed held-PUT window, native
publication waits for the provider worker to exit, so a cancelled RPC must retain
its call slot until that producer exits. Check actual byte persistence separately
from committed document visibility. Sol reviewed these acceptance conditions.

## Publication cancellation qualification

`BoundedPublicationShutdownProbe` holds a completed real Redis PUT inside the
provider call. Library cancellation and a short host close leave the accepted
worker active, SQL usable and the provider open. New publication is refused after
admission closes. After release, the publisher reports cancellation, no success
or revision commit exists, and the physical upload remains available for recovery.
Final host shutdown releases the provider. This does not prove remote quiescence
or immediate cleanup of the cancelled upload.

A separate host runs `BoundedPublicationRpcCancellationProbe` with authenticated
operator-token RPCs and a one-call transport limit. The fixture waits for actual
server-context cancellation before checking that the held producer still occupies
the call slot. A second RPC receives RESOURCE_EXHAUSTED. After the PUT exits and
the transport drains, a distinct operation publishes and replays successfully.
This is not scoped-key authorization qualification.

Both cases passed in the complete production-JAR storage regression. Evidence:
`docs/evidence/repository/2026-10-07-publication-cancellation`.

The separate bounded schema-load case now passes too. It holds actual Git-fetched
descriptor bytes while the resolver worker remains active after caller cancellation.
SQL and Redis stay open until worker drain; the closed resolver discards the late
result. Evidence: `docs/evidence/repository/2026-10-07-schema-worker-shutdown`.
The public factory and its consumer qualification are recorded below.

## Public composition design

Expose bounded document limits through `BoundedDocumentOptions`, following the
existing bounded archive options convention. A `RepoServices.buildBoundedDocuments`
factory should use the existing assembly path and provider discovery. Validate
required schema/publication options and the Redis configuration before discovery
or resource acquisition. Preserve library-only use: historical transport and
publication transport remain explicit, independent options with their existing
byte and concurrency limits. Do not duplicate those limits in document options.

Before documenting that factory as available, test a consumer using the public
entry point with discovered Redis, typed publication and historical decoding.
Test invalid providers, lifecycle/retention settings, TTL and object limits against
an unreachable database to prove configuration fails before resource acquisition.
Keep legacy document, archive and HTTP APIs unavailable in this composition.

Redis persistence and eviction policy remain operator qualifications. The existing
lifecycle invokes the recovery scan and revisits eligible ABSENT tombstones; the
late-request evidence exercises direct recovery, not the elapsed background
recheck interval. Do not advertise an observed automatic cleanup deadline. This
embedded factory is not a new standalone deployment entry point.

## Public factory checkpoint

`BoundedDocumentOptions` and `RepoServices.buildBoundedDocuments` implement the
design above. Seven focused configuration tests and the complete production-JAR
storage regression pass. An out-of-package consumer uses discovered Redis for
typed publication, receipt replay and historical reads, with separate library-only
and authenticated RPC hosts. It leaves schema lifecycle with the host. Evidence:
`docs/evidence/repository/2026-10-07-bounded-public-factory`.
The [publication guide](../repo/publication.md#bounded-redis-composition) describes
the available embedded API and its required host configuration. Sol reviewed the
factory, consumer and guide. This is not a new published-service metadata gate or
standalone launcher, and scoped-key provisioning remains separate work.
