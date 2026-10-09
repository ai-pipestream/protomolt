# Expired coordinator reservation

V97 adds a private SQL reservation source for an expired but unquiesced coordinator.
The guard locks claim then owner, checks both exact identities and lease expiry,
rejects terminal operations and current-epoch local drain, and atomically transfers
the claim with its source and parent records. The parent retains UNKNOWN remote
state and the old owner tuple. V93 validates that tuple; V95 still denies general
execution until activation. Reader pins and cleanup tombstones are unchanged.

On 2026-10-06 this command passed 101 tests in 1m 6s, with zero failures, errors or
skips:

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryCoordinator*IT' --tests '*RepositorySuccessor*IT' --tests '*DocumentSuccessor*IT' --console=plain
```

Compressed XML files contain the individual results. The new 17-case SQL suite
covers natural lease expiry, V90-only predecessors, exact identity mismatches,
live claim/owner, absent owner, terminal outcome, current V91 evidence, immutable
records, stale predecessor fencing, execution closure after reservation, rollback,
parent-publication failure, competing identical/different proposals, and unchanged
lease after a losing repeat. Existing activation tests now migrate already-active
V95 and V96 databases to the latest schema. These are real PostgreSQL transactions;
SQL fault injection is explicit. No provider completion or process death is inferred.

Sol reviewed the migration and final tests without blocking findings. Initial test
compilation errors in overloaded transaction lambdas and an owner accessor were
fixed before execution; the final results above are the executed suite.

This does not qualify Java exact confirmation after lost acknowledgment, automatic
host failover, termination of a paused predecessor, pin recovery or performance.
The common proposal, attachment integration and real process-death recovery remain
unfinished. No new public protobuf operation is advertised.

The separate `./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`
also passed. `storage-runtime.xml.gz` records the production-JAR integration result,
including the existing graceful successor publication and retained-schema checks.
It does not execute the new uncertain reservation through a public host. These are
local test results, not hosted CI, merge or deployment evidence.

Source review also identified a required remaining failure window: replacement death
after reservation and before V94 activation leaves no current-epoch binding. Both
reservation-only and V93-installed states need a separate supersession protocol;
the current guard rejects them. See the design and remaining-work inventory.
