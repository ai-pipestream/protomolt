# Recovery-limit SQL foundation

Base `d91db10b4e74713d81e514b36c9aff009a18841b` plus recorded source hashes.

V110 adds immutable limit evidence under claim-before-owner locking. It validates
the exact live installed identity, command/modes, retained preparation/root digest,
initial capture ownership and bounded ancestry. CAPTURES additionally checks the
complete anchor, 16 sealed batches, original creation identity, coordinator owner
bindings and each child count/digest. ANCESTRY checks an exact 65-edge prefix and
records its endpoint; a longer suffix is not asserted as verified.

Reason 4 requires this sidecar in the same transaction. The rejection guard and
deferred pairing recheck live identity and absence of current activation. Generic
execution/owner fences are unchanged. The initial draft lacked those repeated
checks; Sol identified the late-activation and lease-gap risks, and they were
corrected before this checkpoint. Liveness is checked when the guards execute.
Early `SET CONSTRAINTS ... IMMEDIATE` is not proof that wall-clock leases remain
live at a later commit instant.

The real recovery-bound fixtures now exercise:

- Both exhausted limits pair one sidecar and canonical rejection atomically.
  Existing authorized replay reads reason 4; execution remains closed.
- Omitting the rejection fails the deferred pairing check and rolls back.
- Inserting V94/binding after the sidecar cannot co-commit a reason-4 rejection.
- Under-bound input and a reason-4 rejection without sidecar refuse.

Final focused command exited 0 in 1m54s: 4 tests, zero failures/errors/skips.

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryHistoricalRecoveryBoundIT' \
  --tests '*RepositoryHistoricalInstalledCancellationIT' --max-workers=2 --console=plain
```

Existing rejection regressions exited 0 in 21s: 15 tests, zero failures/errors/skips.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentAdmissionRejectionMigrationIT' \
  --tests '*DocumentPublicationRejectionIT' --max-workers=2 --console=plain
```

The separate archives preserve both runs. This uses actual PostgreSQL 18; source
publication observations remain synthetic. Receipts are written by a test helper,
not a completed production decision handler. Explicit lease-expiry injection,
early constraint firing, corruption variants, concurrent decisions, lost replies,
handler authorization/replay and retained-root release remain unfinished.
