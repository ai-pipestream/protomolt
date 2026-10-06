# Private successor sessions

The focused run passed 93 PostgreSQL cases: 6 new successor session cases, 25
existing journaled session cases, and 62 activation/install/handoff/drain cases.
Sol reviewed the session constructor, registration/journal changes, terminal-aware
owner attachment and test scope.

The session tests verify exact installed owner and seed reuse without another
takeover, repeated attachment, restoration of a durable assessment start, refusal
of fresh stage creation after restoration, retention of the drain identity after
an unactivated attachment, current scoped authorization, fixed coordinator/modes,
registration and local-drain closure, and terminal-owner refusal. They use real
SQL journals; they do not claim provider publication or remote-effect recovery.

The manager still needs to reserve and retain the successor before uncertain
activation. Default runtime factories remain unchanged. The private session's
registration barrier and drain identity are ready for that integration, which must
be tested with shutdown/cancellation races and real provider execution.

Focused command:
`./gradlew :protomolt-repo-container:test --tests '*DocumentSuccessorSessionIT' --tests '*DocumentJournaledSessionsIT' --tests '*RepositorySuccessor*IT' --tests '*RepositoryCoordinator*IT' --tests '*RepositoryOperationLedgerIT' :protomolt-repo-container:admissionStorageTest --console=plain`

There is no suite named RepositoryOperationLedgerIT; that pattern matched no tests.
The reported 93 cases are the suites retained here. Owner-specific regression is
run separately using the actual RepositoryOperation suite names.

The production-JAR storage and restart regression passed (`runtime-final.xml.gz`).
Owner-specific checks then exposed three obsolete error-message expectations: an
unknown account/principal/operation scope is rejected by the execution-scope guard
before reaching the owner-fence guard. The saved before-result records those
assertions. The revised test checks that earlier rejection, creates the alternate
scope in a separate transaction, and verifies that it still cannot use the original
owner's fence or insert a probe row. Production fence behavior was not changed.

The final owner/session command passed 63 cases:
`./gradlew :protomolt-repo-container:test --tests '*DocumentSuccessorSessionIT' --tests '*RepositoryOperation*IT' --console=plain`.
Its `owner-final-*` XML includes a seventh successor-session case validating the
owner identity overload's claim scope and positive generation. Beyond the earlier
runtime run, production changes were limited to correcting a class comment.
These are local test results, not hosted CI, merge or deployment evidence.
