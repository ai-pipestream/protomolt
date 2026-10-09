# Manager-owned successor activation

The private manager reserves capacity and retains the complete successor session
before V94 activation SQL. The outer registration barrier covers preparation,
reservation, activation and attachment. Exact retries compare the complete handoff
and digests of both preparations and modes. Failed attachment keeps the session's
epoch/token/incarnation available to shutdown reconciliation.

The focused PostgreSQL suite passed 100 cases, with no skips or failures:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentSuccessorManagerIT' \
  --tests '*DocumentSuccessorSessionIT' \
  --tests '*DocumentJournaledSessionsIT' \
  --tests '*RepositorySuccessor*IT' \
  --tests '*RepositoryCoordinator*IT' \
  :protomolt-repo-container:admissionStorageTest --console=plain
```

Six new manager cases cover exact retry without renewal, ordinary execution using
the retained successor, changed-plan refusal, an uncommitted install followed by
exact retry, count-capacity/closed-manager refusal, coordinator/current-caller
checks, actual JDBC commit acknowledgment loss, and shutdown racing a committed
activation whose attachment is cancelled. The shutdown snapshot waits for the
accepted activation, then records and attests the retained epoch-two identity.

The production-JAR PostgreSQL/LocalStack storage and restart aggregate also passed
(one harness case, 163.432 seconds). This is a correctness/regression check, not a
RustFS performance measurement or a new successor provider-publication proof.

A final test-only extension passed eight manager cases with:

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentSuccessorManagerIT' --console=plain
```

This adds the non-cancelled shutdown race (attachment returns UNAVAILABLE after
activation committed), checks exact cancellation/refusal codes, and proves the
serialized-command byte limit also refuses before activation with no retained
entry or command-byte accounting. `manager-final.xml.gz` records this final run;
the earlier manager XML records the six cases included in the 100-case run.

The ordinary execution case intentionally stops at the missing schema-policy
gate. Its fail-fast provider ports establish that no provider was called, not a
successful provider publication. Early fixture failures attempted to recreate an
existing drive; the corrected fixture uses distinct operations on the same drive.
Sol reviewed the production ordering and tests without finding a blocker.

Public factories and protobuf contracts are unchanged. Full successor provider
publication, delayed predecessor effects, abrupt-death recovery and performance
qualification remain open. An accepted activation does not promise attachment
completion after shutdown closes the inner registration admission.
