# Exact reclamation for revision pruning

Status: design, not implemented or enabled.
Original source inspection: 5f8acfb413febd0a3ce1233d06d9a143d1220f86.
Resumed checkout: 3563150556f4695fbd863bb0072f6b396d6ba310, verified on both remotes.
Recheck the implementation inventory on this base before writing migrations.
Companion: [revision pruning](repository-revision-pruning.md).

## Existing behavior and the missing capability

`repo/blob/spi/.../ObjectReclaimer.java` defines cleanup of one complete key, including
all versions and delete markers. `repo/blob/s3/.../S3ObjectReclaimer.java` implements
that contract by listing versions and deleting bounded batches. This is intentional
lifecycle behavior; it is not an exact-version deletion port. Do not change its meaning
or pass a revision-prune candidate to it.
`BlobCapability.PHYSICAL_RECLAMATION` advertises that same whole-key contract;
it must not be interpreted as support for the proposed exact-version operation.

V25 repository_physical_locations binds object_id to backend generation, storage realm,
namespace, key and an origin tuple. It does not contain the provider version. For
DOCUMENT_PART, resolve the origin through document_part_attempt_objects using attempt_id
and ordinal and verify physical_object_id and verified state. The provider_version and
etag belong to that verified observation (V21 and later guards). For ARCHIVE, resolve the
binding/upload observation through the existing archive ledger; ArchiveObjectLedger.Readable
already carries the provider version. Preserve all identity checks when extracting this
into a reclamation resolver. Missing, ambiguous or inconsistent provenance is a refusal,
not permission to delete the current version or select another configured backend.
The existing archive `readable` query requires a LIVE upload and retained version
reference. It is a read-authority query, not a cleanup resolver after reference release.
Capture the verified identity before release and use a dedicated origin verification
query for cleanup; do not relax that read-authority predicate.

V25 enforces one registered physical location per realm/namespace/key, but provider-side
versions and late writes can still exist. That uniqueness alone does not authorize deletion
of every provider version. The cleanup capability must express the approved target exactly.

## Proposed internal contracts

The first document-revision implementation admits only verified DOCUMENT_PART
origins. ARCHIVE origins require a separate extension; the archive observations
above identify reusable behavior, not permission to include them in this slice.

The logical prune result binds account, request identity and digest, node/revision,
released object identities and schema ownership. It commits in the same transaction as
live-reference removal and durable reclamation candidates. Immutable publication records,
parts, commit identities and authorized receipt replay survive.

A physical reclamation candidate contains the immutable object_id and a snapshot of its
backend generation, realm, namespace, key, source kind/id/ordinal and verified provider
version. Bind the observed size/checksum and ETag for diagnosis and consistency checks;
none substitutes for a missing version identity. Persist no credentials. Resolve secrets
only through the registered backend identity when processing a claim.

One logical release can nominate an object still retained by a sibling. Such a candidate
is pending eligibility, not deletion authority. The cleanup worker rechecks all native
owners and retention state while holding the established complete origin/retention locks.
Only a durable state that excludes new acquisitions may authorize provider I/O after those
locks are released. A lease expiring must not reopen acquisition while a former worker
could still be deleting that exact version.

Introduce a distinct exact-version reclaim capability behind the storage SPI. Do not add
an implicit implementation through ObjectReclaimer. It accepts a nonblank provider version
and the resolved namespace/key, and distinguishes an acknowledged deletion or verified
absence of that exact version from a retryable uncertain result. Provider errors retain
explicit failure and retry state. Key-only HEAD cannot establish absence of an old version.
For S3, use exact VersionId deletion and verification rather than ListObjectVersions plus
whole-key removal. Deleting an already absent version must be safe on retry.
Require a qualified versioned namespace. Reject Java null, blank version IDs and
the S3 literal version ID `null`; none establishes an immutable version target.

Providers without exact-version identity/capability require their own reviewed immutable
object or conditional-delete protocol. The initial implementation must leave their physical
cleanup unsupported and explicit; never turn null version into delete-current semantics.
Logical prune eligibility must declare whether unreclaimable storage is allowed by policy;
this choice is unresolved and must be fixed before an operation is exposed.

## Concurrency and recovery obligations

1. The prune transaction follows the existing document fence, V65 complete origin set,
   retention set and ordered schema locks. No provider calls or waits on worker completion
   occur while those locks are held. Account-level unknown coverage blocks eligibility.
2. Reclaim claims use bounded scans and fenced claim identities shared through SQL, not a
   process-local mutex. Different objects must progress on different service instances.
3. A successful delete followed by lost SQL acknowledgement leaves a retryable candidate.
   The retry verifies/deletes the same immutable target; it cannot advance to a newer version.
4. A stale worker cannot complete another worker's claim. Provider calls can overlap after
   lease expiry, so exact deletion itself must remain idempotent and acquisition must remain
   closed throughout that overlap. SQL fencing alone does not stop a provider request.
5. Completion records retain target identity and outcome. Rollback before logical release
   must leave neither removed live references nor actionable reclaim work.
6. Schema bytes are a separate resource. V48 stores them in the artifact catalog, and
   V55/V56/V57 preserve associations. Exact blob deletion does not reclaim descriptor payloads.
   Payload separation and schema GC require their own migration and live-owner proof.
7. Establish V26's permanent reclaiming fence before provider I/O, proving that all
   native owner kinds are absent and retention mirrors agree. An expired cleanup
   lease cannot remove that fence. A late PUT can still finish after fencing: exact
   deletion must spare its new version, whose orphan cleanup is separate work.

## Required acceptance tests before activation

- One key with two provider versions: reclaim only the approved old version; current bytes
  and delete markers remain unchanged. Another key sharing its prefix remains untouched.
- Two revisions share an object: pruning one never deletes content still owned by the other.
- Current and historical pins, assessment slots, and preparation roots race with pruning in
  both lock orderings. No new capture enters after durable reclamation authorization.
- Unknown legacy coverage, missing provider version, mismatched source identity, unsupported
  capability and backend resolution failure refuse without provider effects.
- Real-provider delete failure, SQL failure after deletion, lost acknowledgements, duplicate
  workers, expired claims and fresh-process restart converge on the same target.
- Unrelated document publications/reads and unrelated cleanup claims continue while one
  provider request is held. Record transaction and lock durations as well as correctness.
- Hold a real PUT across the reclamation fence. Delete only the approved version,
  prove the late version survives, and record that orphan for separate cleanup.
- Authorized receipt replay remains byte-identical after content release; unauthorized
  callers cannot discover prune state. Retained content corruption stays distinct from prune.

Use PostgreSQL and LocalStack for correctness; use pinned RustFS for performance. This
design introduces no migration, new SPI implementation, public API or deletion permission.
