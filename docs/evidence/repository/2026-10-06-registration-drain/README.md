# Retained registration drain

Base: `fe680a1b318c2761089794347f8608da848bc231`, plus the accompanying changes.

`./gradlew :protomolt-repo-container:test --tests '*DocumentJournaledSessionsIT' --tests '*RepositoryCoordinatorDrainIT' --tests '*DocumentPublicationPreparationJournalIT' --console=plain`

Passed 60 cases in 43 seconds. Compressed XML preserves the complete test output.
These tests use actual PostgreSQL registration, claim and marker transactions.

New cases cover two retained identities, supplied process authority, a lost reply
after real marker commit, a held registration commit, an accepted call delayed before
registration, and a refused commit whose absent claim remains unresolved. Retained
identity, command capacity and original lease remain unchanged after drain marking.
The transfer interleaving checks both outcomes: an exact committed marker can be
confirmed after transfer; an unmarked transferred claim remains fenced. Repeated
cancellation using the same exception instance preserves that original failure.

Sol's review identified the lookup/transfer race and exception self-suppression;
both received targeted regression tests. The earlier run exposed authorization
ordering and a duplicate-drive fixture error, fixed before the final passing run.

This marks retained nonterminal registrations only. Terminal entries already
evicted with durable proof are excluded. No LOCAL_DRAINED marker, provider remote
quiescence, automatic successor authority or ordinary journaled runtime activation
is claimed. This is local validation, not hosted CI, a merge or deployment.

The same production source also passed
`./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`.
Attached runtime XML records the PostgreSQL/LocalStack production-JAR regression.
This exercises existing provider/runtime paths; it does not qualify LOCAL_DRAINED
or automatic successor execution.
