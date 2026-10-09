# Graceful successor reservation

The focused PostgreSQL suites pass 35 cases: six handoff cases, fifteen admission
drain cases and fourteen local-drain cases. The command is:

`./gradlew :protomolt-repo-container:test --tests '*RepositoryCoordinatorHandoffIT' --tests '*RepositoryCoordinatorLocalDrainIT' --tests '*RepositoryCoordinatorDrainIT' :protomolt-repo-container:admissionStorageTest --console=plain`

Handoff checks cover an expired exact predecessor, missing drain and incorrect
identity refusal, process authority, concurrent conflicting and identical proposals,
one immutable winner, exact retry after successor lease expiry without renewal,
continued execution closure, and terminal cancellation refusal. A control throws
after the INSERT trigger transfers the claim but before commit; both claim and
handoff roll back. A JDBC wrapper throws after the real commit; exact confirmation
recovers its lost reply. No success-shaped fake storage participates.

The existing drain suites also exercise migrations over prior populated schemas.
Sol reviewed transaction ordering, trigger effects, retry behavior and tests. The
successor row is separate from the initial V89 binding. It does not create a new
owner generation or attempt, open provider access, or enable runtime handoff.
Successful-terminal refusal still needs its own case; the present terminal test
uses a real cancellation. Abrupt death without local attestation remains separate.

The complete combined command passed in 3 minutes 16 seconds, including the
production-JAR storage host and restart processes. `runtime.xml.gz` retains that
result. These LocalStack-backed checks establish correctness, not performance.

All 7 handoff tests pass, including the recovery entry points after V92. Exact
retry retrieves the saved successor claim without renewing it. V91 blocks
preparation save, modes bind and owner takeover. The original owner generation
stays unchanged, no successor records are created and reserved bytes return to
zero. See `entrypoints.xml.gz`. Sol reviewed the test and atomic-install design.
