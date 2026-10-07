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
