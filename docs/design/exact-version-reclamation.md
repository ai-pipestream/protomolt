# Exact-version reclamation provider contract

Status: implemented as a provider prerequisite. The SQL prune transaction,
permanent reclamation fence, ownership checks and durable cleanup claims that
would call this port are NOT part of this change; this document and the code
describe the byte-provider mechanism only.

## Purpose and naming

`docs/design/repository-prune-reclamation.md` requires "a distinct exact-version
reclaim capability behind the storage SPI" and forbids implementing it through
`ObjectReclaimer`. This change introduces that capability:

- `BlobCapability.EXACT_VERSION_RECLAMATION` — advertises exact immutable
  provider-version deletion. `PHYSICAL_RECLAMATION` keeps its whole-key meaning
  ("Exact-key physical reclamation includes all historical versions and
  markers"); neither the enum value nor `ObjectReclaimer` changes behavior.
- `ExactVersionReclaimer` — the new port, exposed on `OpenedBlobStore` next to
  `reclaimer()` with the same capability-requires-port invariant.
- `ExactVersionOutcome` — the three-way result.

Naming follows the enum convention (`PHYSICAL_RECLAMATION` →
`EXACT_VERSION_RECLAMATION`) and the prune document's own phrase. Reclamation is
the storage-lifecycle anchor word (ADR-001); "exact version" names the target,
not a new domain. Coordinates use the SPI's existing `bucket`/`key` vocabulary;
the repository's "namespace" resolves to that provider coordinate before the
port is invoked.

## API shape

```java
ExactVersionOutcome reclaim(String bucket, String key, String versionId);
```

A single synchronous call bound to one immutable target. The port is a
functional interface mirroring `ObjectReclaimer`; providers that do not qualify
keep the existing `OpenedBlobStore` constructors and receive the default port,
which throws `UnsupportedOperationException` — capability advertisement always
matches an actually wired port.

## Target validation (before any destructive I/O)

The implementation rejects, with `IllegalArgumentException` and no provider
call:

- null or blank `bucket`, null or empty `key`;
- null, blank, or literal `"null"` `versionId` — the S3 literal `null` version
  is not an immutable-version guarantee and never becomes delete-current
  semantics.

The current version is never substituted: the version coordinate is mandatory,
so there is no code path that resolves "latest".

## Namespace qualification

A qualified versioned namespace is required per call. For S3 the implementation
reads bucket versioning state before deleting:

- `ENABLED` — qualified;
- `SUSPENDED`, never enabled (null status) — refused with
  `BlobStoreException(FAILED_PRECONDITION)`;
- bucket absent — refused with `BlobStoreException(NOT_FOUND)`;
- state unreadable (denied, transport) — the failure propagates classified;
  nothing is deleted.

Qualification is established by the provider at call time, not assumed from
endpoint brand: an S3-compatible endpoint that cannot report or honor these
semantics surfaces as `UNIMPLEMENTED`/failure, and the backend is then
unqualified for this capability. Administrative changes between the check and
the delete (suspend versioning, delete the bucket, policy change) are an
explicitly documented trust boundary: they cannot make the operation delete
anything other than the exact version id, because only that id is ever
addressed.

## Outcomes

`ExactVersionOutcome` has exactly three values:

- `DELETED` — the exact version was observed present (versioned HEAD), the
  provider acknowledged its deletion, and a versioned HEAD afterwards confirms
  absence.
- `ABSENT` — verified absence of that exact version by exact-version evidence
  (versioned HEAD → 404). Key-only HEAD is never used as absence evidence.
  Absence is permanent once verified because version ids are immutable and are
  never reused by the provider; a later PUT to the same key creates a new
  version id.
- `UNCERTAIN` — the provider could not prove either terminal state: request or
  response timeout, connection failure, 5xx, interruption, or a delete that was
  acknowledged while post-delete verification stayed inconclusive. Retrying the
  same `(bucket, key, versionId)` is safe and is required to converge.

Explicit failures throw rather than return an outcome:

- `IllegalArgumentException` — invalid target (above), before I/O.
- `BlobStoreException(PERMISSION_DENIED | UNAUTHENTICATED)` — authorization
  failure; retrying without a credential change is pointless. Never reported as
  absence.
- `BlobStoreException(NOT_FOUND)` — namespace does not exist.
- `BlobStoreException(FAILED_PRECONDITION)` — namespace not version-enabled.
- `BlobStoreException(UNIMPLEMENTED)` — the backend reports (501) that it
  lacks a required semantic.

Anything the provider cannot classify explicitly — timeout, connection
failure, server error, interruption, corruption evidence — is `UNCERTAIN`, so
access denial, missing namespace, corruption and unsupported behavior can
never surface as successful absence. Causes are preserved via exception
chaining for internal diagnosis; public messages carry only bucket/key/version
id — never endpoints or credentials.

Interruption during provider I/O yields `UNCERTAIN` with the thread's interrupt
status restored, not a `CANCELLED` throw: for a destructive request the caller
cannot prove non-application, and `UNCERTAIN` is the honest signal. (The
bounded-read path's `CANCELLED` mapping is a read-only convenience and is not
mirrored here.)

## S3 mechanism

Per call, bounded to four API calls (SDK retry attempts per call follow the
client owner's configured retry policy; retries are safe because the target is
immutable and the delete idempotent):

1. `GetBucketVersioning` — namespace qualification (above); 501 →
   `UNIMPLEMENTED`.
2. `HeadObject` with the exact `VersionId` — existence. 404 → `ABSENT`
   (classified on status, not the error code: RustFS answers `NoSuchKey`
   rather than `NoSuchVersion` for a deleted version id, and HEAD carries no
   error body). A delete-marker version (405 with `x-amz-delete-marker`)
   counts as an existing target; the port deletes exactly the addressed
   version id whatever it holds, and the caller is responsible for binding
   only verified byte versions.
3. `DeleteObject` with the exact `VersionId` — the only mutation. Never
   whole-key `ObjectReclaimer`, never prefix deletion, never
   list-then-delete of unrelated versions. DELETE responses carry an error
   body, so a 404 is classified by its S3 error code: `NoSuchVersion` means a
   racing same-target deleter already removed it — continue to verification
   and report `ABSENT`, never `DELETED` (this call acknowledged no deletion);
   `NoSuchBucket` → `NOT_FOUND`.
4. `HeadObject` with the exact `VersionId` — verified absence → `DELETED`
   (or `ABSENT` after a `NoSuchVersion` delete); anything inconclusive →
   `UNCERTAIN`, including refusals: after an acknowledged delete a 403 on
   verification must not read as denial-as-absence.

## Concurrency

- Duplicate or overlapping calls for the same immutable target are safe:
  exact-version delete is idempotent, and an already-absent version stays
  absent, so a second caller converges to `DELETED`/`ABSENT`.
- A newer version or delete marker on the same key survives: only the exact
  version id is addressed. A PUT completing concurrently creates a new version
  id and is never touched (the repository's future SQL fence decides whether
  such an orphan can exist at all; the port only guarantees it is spared).
- Distinct targets proceed fully independently: the port is stateless, holds
  no locks, and assumes nothing about single-replica execution.

## Cancellation and timeouts

The port runs on the provider-owned client and inherits its configured
connect/socket/api-attempt/api-call timeouts (all required, explicit, no
defaults at open) and its configured credentials. No new configuration keys,
no fallback credentials, no extra threads. Work per call is bounded by the
request count above times the configured api-call timeout. The AWS sync
client offers no asynchronous cancellation; interruption is surfaced as
`UNCERTAIN` as described. The client lifetime stays with the provider's
`OpenedBlobStore` close.

## Capability availability

Advertisement stays static at open (existing convention): `s3` advertises
`EXACT_VERSION_RECLAMATION` and wires `S3ExactVersionReclaimer`.
Advertisement is justified by the acceptance qualification in this change:
LocalStack proves correctness and RustFS proves compatibility on a pinned
image. A backend that fails a required behavior is marked unqualified in test
evidence rather than skipped silently. Redis, cache and grpc providers remain
explicitly unsupported — none supplies a qualified immutable-version
mechanism, and their existing constructors route to the unsupported default
port.

Re-wrapping never inherits this capability: `OpenedBlobStore` constructors
that cannot wire an exact-version port drop `EXACT_VERSION_RECLAMATION` from
the advertised set instead of advertising it against an unsupported port.
This is deliberately asymmetric with `PHYSICAL_RECLAMATION`, which predates
decorator re-wrapping (~17 sites in `repo/service`/`repo/container` already
thread the whole-key reclaimer through) and keeps its requires-port refusal.
A decorator that learns to compose exact-version reclamation advertises it
explicitly through the six-arg constructor.

## Caller obligations (the future prune worker)

This port is a privileged mechanism, not permission to delete. Before invoking
it the caller must:

1. Establish repository authorization and prune eligibility in SQL (ownership,
   retention, account coverage) and install the permanent irreversible
   reclamation fence; only a durable state that excludes new acquisitions may
   authorize provider I/O.
2. Bind the target from a verified observation (the write/read-back ledger's
   provider version), never from `repository_physical_locations` alone, never
   from listing selection, and never the current version.
3. Hold no SQL locks or connections while invoking the port.
4. Treat `UNCERTAIN` as retry-the-same-target with durable retry state; treat
   thrown failures as explicit, retained failure state. A retry verifies or
   deletes the same immutable target and can never advance to a newer version.
5. Record that a late PUT finishing after fencing creates a new version id
   that survives; its orphan cleanup is separate work.

## Explicit non-goals

No SQL migration, retention-predicate change, schema payload split, archive
pruning, public protobuf or API change, hydration, pruning endpoint, or
background deletion. Other providers stay unsupported until they supply their
own reviewed immutable-version mechanism.
