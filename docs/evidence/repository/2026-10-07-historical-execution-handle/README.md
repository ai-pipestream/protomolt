# Initial historical execution handle

Baseline: `c832e7e363e9fd637c8c29053b56b5e68c58d88d`. Tested source fingerprints
are in `source-sha256.txt`; the implementation and evidence are committed together.

The registration can now construct a private, closeable initial-owner handle.
Construction retains a registration Call and the original source owner's Work,
checks current source and destination authority, loads and validates private
preparation/modes, and compares the complete canonical preparation bytes, digest
and nonce in SQL. The final transaction fences the claim and owner, locks the
preparation and retention header, verifies fixed modes/root coverage, current
authorization, placements, origins, retention and physical source witnesses, then
confirms the initial capture and live pins. A tentative in-memory capture alone
is insufficient. No missing metadata is repaired.

The handle retains bounded pin/mode scratch, exact prepared references and owner
identity. It currently exposes only close: assessment start, CREATE, publication,
restart and successor methods are not enabled. Later operations must acquire
their own accepted lifetime permits or synchronize with close and repeat current
authority/fencing checks; handle possession is not a permanent access grant.

Sol found that the first implementation omitted the registration admission
barrier. This was corrected before landing: the registration Call is acquired
before mint, released on failure and transferred into the handle on success.
Handle close releases source Work and scratch before the registration Call.
The binding-only mode check was renamed `requireBoundModes`; replay behavior is
unchanged and covered by the existing commit suite.

## Validation

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentHistoricalExecutionIT' \
  --tests '*DocumentHistoricalRegistrationAuthorizationIT' \
  --tests '*DocumentPublicationCommitIT' \
  --max-workers=2 --console=plain
```

Final execution: exit 0 in 59 seconds, 44 tests, zero failures, errors or skips:
ten handle cases, two scoped registration authorization cases and 32 existing
publication-commit cases. Final XML and Gradle output are in `final/`. Earlier
six-case and lifecycle runs passed; their Gradle logs are retained separately,
but their XML was overwritten and is not claimed as independently archived.

New handle checks cover valid retention, final-transaction closure, real JDBC
registration rollback and lost acknowledgement, changed modes/owner, missing
capture ownership, corrupted roots, and closed source/registration admission.
The gate detects the final transaction's actual shared read-pin table lock; it
does not use sleep-based scheduling. Source and registration barriers remain
non-drained while that transaction and then its returned handle remain active.
The scoped fixture verifies new handle construction fails after READ revocation.

Rollback and lost-ack tests reconstruct tentative owner coordinates from the
registration's private seeds and identity. They assert the capture object exists
in both cases, then prove SQL refuses the rolled-back registration and accepts
only the committed one. Reconstructed Java values are not used as proof of
durability. Corruption cases deliberately bypass immutable-row triggers using
transaction-local replication mode.

SQL/retained descriptor tests use PostgreSQL 18 and initial synthetic provider
observations from the existing fixture. These results do not establish claimed
assessment execution, historical success publication, transport parity or
performance. Sol reviewed the final implementation and tests with no remaining
blocker. No hosted CI, merge or deployment is claimed by this local evidence.
