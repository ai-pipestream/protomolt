# Bounded typed rejection

Command: `./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`

The production-JAR regression passed in 7m 47s using real PostgreSQL, Redis and
LocalStack containers. The recorded red run failed while loading retained
assessment inputs through a bounded transaction view. The candidate had failed
validation; processing its rejection failed before returning the receipt.

The loader now uses one transaction for the retained SQL inputs. Provider reads
and runtime rule evaluation occur outside it. Six retained-input cases run with
both ordinary and bounded transactions: successful loading, capacity exhaustion,
cancellation, access revocation, stored corruption and interruption.

The Redis host publishes valid typed controls and rejects the fixture value
`contract-invalid` through the retained CEL rule `bounded-fixture-value`. The
fixture checks sealed assessment identity, the exact rule and CORE root, one
rejection, and no success or revision commit. Local and authenticated gRPC replay
after resolver closure return the same receipt without another provider call.
The enclosing regression also covers actual Redis restart, retained historical
decoding and delayed-write recovery after real lease expiry.

Sol reviewed the production change and test conditions. These are local test
results, not hosted CI or deployment evidence. Active publication shutdown and
gRPC cancellation during provider work remain separate acceptance cases.
