# Reuse local session retirement after successor installation

Base: `dbf55e3ff2cf28d2a2731ec8f0c3bc027d46ee2e`.

No production API was added. The existing session retirement method checks the
locally retained owner generation/nonce and exact command under an SQL owner lock.
It excludes concurrent local users without holding the manager monitor during SQL.

The new real-PostgreSQL cases cover activation failure before installation and
activation rollback after installation when the execution caller lacks access.
Manager A keeps the failed session. Natural lease expiry does not permit retirement.
Manager B reserves V98 supersession; this still does not permit retirement because
only the claim changed. B installs the next owner through V93, after which A's
existing retirement method frees its sole session slot and command bytes without
changing the durable claim/owner. B activates successfully and A admits an unrelated
operation, demonstrating that released capacity is usable.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentSuccessorManagerIT' --tests '*DocumentPublicationRecoveryIT' --console=plain
```

All 24 cases passed, without skips: 10 manager and 14 recovery cases. The existing
recovery suite covers same-session exclusion, cancellation and failed readback.
`affected-green.tar.gz` preserves those results. Sol reviewed the sequence and
recommended making the distinct manager incarnation assertion explicit.

The test originally attempted same-manager supersession, which the existing Java
and SQL identity guards reject. Each manager has one fixed incarnation, while a
successor must differ from its predecessor. That guard was preserved. Automatic
same-host recovery needs a reviewed fresh-incarnation lifecycle before integration.
The two-manager result does not establish it.

These tests perform no provider I/O and do not qualify pin reclamation. Retirement
releases local session capacity only. Recovery-attempt budget release, uncertain
activation reconciliation, managed shutdown and automatic scheduling remain open.

After adding that assertion, both new parameterized cases passed again, without
skips (`final-green.tar.gz`):

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentSuccessorManagerIT.failedActivationRetiresOnlyAfterReplacementOwnerIsInstalled' --console=plain
```

Local verification does not establish hosted CI, merge or deployment.
