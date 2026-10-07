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
