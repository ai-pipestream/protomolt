# Shared managed publication facade

The packaged admission/storage integration passed in 217 seconds; Gradle completed
in 3 minutes 40 seconds. All 39 SPI tests and the runtime dependency check passed.
No tests were skipped. The invocation was:

```
./gradlew :protomolt-repo-container:admissionStorageTest :protomolt-repo-spi:test :protomolt-repo-spi:checkRuntimeBoundaries --console=plain
```

DocumentPublicationRepository accepts the staged protobuf request and returns a
committed/rejected response. Journaled runtimes provide the implementation with a
trusted host selector. Runtime admission, shared permits and byte reservations
span execution and final receipt verification. Only durable runtime rejection
signals become rejection responses. The final observation must match the exact
execution receipt.

ManagedJournaledDrainProbe publishes an opaque document through this facade using
real PostgreSQL, versioned LocalStack storage and the packaged runtime. Terminal
retries use a selector that throws if invoked; incomplete uploads and changed modes
are rejected before selection. An accepted typed call finishes after shutdown
starts while a real Git schema lookup waits. Existing recovery scenarios passed.
Input documents are synthetic; storage implementations are real.

Sol reviewed control flow, memory lifetime, receipt races and callback ownership.
Full operation-key guarding applies on hosts with recovery enabled; journaled hosts
without recovery use session/SQL serialization. Selector calls may overlap and
must return immutable host snapshots plus a lazy authorized schema-scope factory.

This qualifies the library facade. Tests supply trusted fixture placements and the
built-in Document definition. Production service selection, gRPC adapters, transport
parity, response-delivery budgets and facade rejection/capacity tests remain.
No new RPC is mounted. This run does not measure RustFS performance or scaling.
