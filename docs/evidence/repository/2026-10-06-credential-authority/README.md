# Repository-local credential authority

Baseline: `a66fcb11f1d23fea853ab23adcdab922c1f31c28`.

V99 adds durable current-generation/revocation state and an internal process-only
provisioning port. It stores opaque key identity and principal, never raw tokens.
It supplies no creation grant and is not yet wired into publication/replay.

`affected-green.tar.gz` retains 38 passing PostgreSQL cases, with no skips:
7 credential authority, 12 execution claim, 7 initial admission and 12 replay.

The new cases cover exact registration retry; principal conflict; irreversible
same-generation revocation; rotation and stale-generation fencing; missing-key and
untrusted provisioning refusal; migration from V98 with existing operation rows
and no automatically invented authority; database identity/rotation/deletion guards;
and unsupported isolation even for an absent credential. SQL guard tests check the
expected error reason, not merely any exception.

The concurrency case holds an actual shared authorization transaction. Another
reader completes while that lock is held. Revocation is then observed waiting via
PostgreSQL blocking metadata; releasing the reader allows revoke to finish and
subsequent authorization is refused. This proves shared-reader behavior for one
key, not sustained throughput or multi-host scaling.

Sol reviewed the mutation/locking semantics. Its isolation finding was addressed
with a guarded SQL lookup function and mutation trigger, tested at REPEATABLE READ.
Registration and lookup retain one client round trip per SQL statement; the live
check is designed to join the owning admission transaction, not start another.

## First-admission design finding

No lock was added to the existing execution-scope function. Doing so would invert
claim/scope ordering between operation admission and claim insertion retry. The
reviewed next design uses unique scope INSERT arbitration and installs the new
scope and grant in one transaction. It refuses retroactive installation when an
existing scope has no grant. That grant protocol remains unimplemented.

## Limits and retry behavior

- Registration is idempotent for an exact active record. Revocation cannot be undone
  by registration retry. It returns UNAUTHENTICATED for the revoked generation.
- Revocation is idempotent for the exact current generation. Missing or replaced
  generations return NOT_FOUND, without a promise to block future registration.
- Rotation is compare-and-set. Repeated or stale requests conflict; an uncertain
  response does not authorize a guessed second rotation. A future public admin API
  still needs a status/idempotency contract before exposure.
- Live checks require matching caller/key/principal/generation and READ COMMITTED.
  They establish neither account membership nor document creation rights.
- Tombstones cannot be deleted yet. Bounded retention and safe pruning remain work.
- This does not revoke unrelated platform authentication or wire key authority into
  publication/recovery. No public grant API, deployment or performance claim follows.

The production-JAR qualification also passed:
`./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`.
Its PostgreSQL/LocalStack result is retained in `packaged-green.tar.gz`. This is
local migration/runtime regression evidence, not hosted CI, merge or deployment.
