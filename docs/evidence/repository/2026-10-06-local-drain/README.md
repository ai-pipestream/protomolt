# Private durable local-drain ledger

Base: `3e48116baa91acb841dd701f796d99e806aa8710`, plus the accompanying changes.

`./gradlew :protomolt-repo-container:test --tests '*RepositoryCoordinatorLocalDrainIT' --tests '*RepositoryLocalDrainCleanupIT' --tests '*RepositoryCoordinatorDrainIT' --tests '*RepositoryClaimMutationFenceIT' --console=plain`

Passed 41 cases in 32 seconds. Compressed XML preserves the complete output.
The tests exercise actual PostgreSQL migrations, transactions and mutation guards.
The new ledger cases cover:

- Exact V89/V90 identity and process-authority requirements, immutable records,
  and refusal of incorrect confirmation identities.
- Expired but unchanged claim attestation without renewal, refusal after transfer,
  exact confirmation after later transfer, and execution closure across epochs.
- Lost reply after actual marker COMMIT, cancellation before/after COMMIT, and
  preservation of the original cancellation exception.
- A fence stamped earlier in the same transaction cannot authorize writes after
  attestation; failed transaction work rolls back.
- An actual renewal blocked on the attestation transaction's claim lock is refused
  after COMMIT. The test observes the blocker through `pg_blocking_pids` before release.
- Migration from V90 preserves an existing draining operation without inventing
  local-drain evidence; settlement remains possible until explicit attestation.
- Positive recovery-only cleanup: the V50 function removes one superseded schema
  claim under V49's fence while preserving the current claim, artifact, generation,
  token, lease and write fence. Raw unfenced deletion and execution renewal fail.

Sol identified and reviewed the operation-wide closure requirement: matching only
the drained epoch would allow low-level claim transfer to reopen settlement. The
final guards apply to every epoch until an explicit successor protocol is designed.
The positive cleanup case closes the separate retention evidence gap found in review.

This is SQL protocol qualification. No runtime invokes the attestation primitive
yet, and these tests do not pretend to drain host or provider workers. Complete
host/runtime/schema-worker proof, remote late effects, automatic successor execution
and public activation remain unfinished. No throughput, hosted CI, merge or
deployment claim is made.

The production-JAR PostgreSQL/LocalStack regression also passed:

`./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`

Attached runtime XML records compatibility through migration V91 and the existing
publication/retention/runtime scenarios. The host does not insert local-drain
attestations in this regression; the focused SQL cases above exercise those rows.
Sol's final review found no remaining blocker in this private ledger checkpoint.
