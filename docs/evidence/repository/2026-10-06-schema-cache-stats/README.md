# Schema cache statistics

Base: `af91e0aae6be401c81086a54871d86116ba88c9f`, with the adjacent working changes.
Sol reviewed the implementation and tests without finding a remaining blocker.

`./gradlew :protomolt-repo-schema-registry:test :protomolt-repo-container:test --tests '*DocumentPublicationPreparationJournalIT' --tests '*DocumentJournaledSessionsIT' --tests '*RepositoryExecutionClaimLedgerIT' --console=plain`

Passed in 38 seconds: nine registry cases and 52 repository cases, no skips or
failures. Attached XML records the registry cases. Real Git fixtures verify one
cold read, reuse within an attempt, a warm cache in another attempt, three store
calls across missing/outage/recovery, and a joined read despite caller cancellation.
Completed loads and cache ownership drain. Registry reads include failed calls;
joined loads count join attempts even if the waiter later cancels.

Statistics are cumulative events and current gauges, not an atomic snapshot.
Serialized cache ownership excludes provider allocation and descriptor/JVM heap.
No timing, throughput or horizontal-scaling claim follows from these counters.
