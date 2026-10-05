# Private publication modes journal

Command: `./gradlew :protomolt-repo-container:test --tests '*DocumentPublicationModesJournalIT' --tests '*DocumentPublicationPreparationJournalIT' --tests '*RepositoryExecutionClaimLedgerIT' --console=plain`

26 tests passed on real PostgreSQL: seven mode-journal cases, ten preparation cases
and nine execution-claim cases. Complete output and JUnit XML are retained here.
The concurrent mode test observes a real database lock waiter before releasing
the first writer and proving the conflicting second writer fails.

The lost-acknowledgment fixture throws only after a separate transaction sees the
committed mode row. It does not kill a process. Another fixture directly inserts
valid JSONB with wrong command members; private loading must return DATA_LOSS.

V82 and the private journal are prerequisites. Automatic session recovery,
assessment-stage persistence and SQL consumption guards remain unfinished.
No provider throughput, multi-host failover or deployment is claimed.

Sol reviewed the final implementation and tests with no blocking findings for
this inactive prerequisite. The review explicitly retained the SQL/member-coverage
limitation and the requirement to use validated loading before activation.
