# Current-policy authorization for pending replay

Baseline: `6bc32d582a65e1c17ea79340e662d8ab96c734f1`.

Pending publication observation previously checked the operation principal/account
and command binding but returned before current document access checks. The shared
read-set authorization now runs before returning PENDING. It does not compare
expected revisions, renew a lease, write a receipt or grant takeover authority.

## Evidence

- `red.xml.gz`: four PostgreSQL tests ran; three failed on the old behavior.
  A denied target and a scoped absent target returned PENDING, and pending replay
  did not wait for a conflicting policy update. The committed-replay control passed.
- `focused-green.tar.gz`: 31 tests passed across publication replay, rejection and
  scoped registration, without skips.
- `affected-green.tar.gz`: 41 tests passed across replay, sessions, initial admission
  and registration inspection, without skips. This adds a denied source outside the
  destination set. Readable stale revisions remain observable as PENDING.

The policy race holds a real PostgreSQL transaction, observes the waiting backend,
commits revocation, and expects denial. It does not infer ordering from a sleep.
Tests use real SQL and explicitly synthetic publication fixtures; they do not claim
provider byte verification from fixture setup.

Sol reviewed the shared lock order and recovery semantics. Process creation may
observe an absent ifAbsent destination; scoped creation still requires a future
explicit grant. Successful replay keeps its existing current-target-read behavior.
The existing command-conflict signal remains principal/account scoped and precedes
current document access checks. No protobuf, migration or receipt-format changes.

The [scoped creation design](../../../design/repository-scoped-creation.md) is a
reviewed implementation plan, not an available grant API. Its key-identity and
revocation obligations are not fulfilled by this replay fix.

`./gradlew :protomolt-repo-container:admissionStorageTest --console=plain` also
passed its production-JAR PostgreSQL/LocalStack scenario, with no skips. Its result
is retained in `packaged-green.tar.gz`. This is local correctness/recovery evidence,
not a performance, hosted CI or deployment result.
