# Service publication selection

The final Gradle invocation passed in 3 minutes 50 seconds. Packaged storage
integration ran for 218 seconds. The scoped lookup test passed; the unchanged
schema test remained up-to-date after passing earlier. No failures or skips were
reported. Admission dependency checks passed.

```
./gradlew :protomolt-repo-admission:test --tests '*BuiltinDocumentSchemaTest' :protomolt-repo-admission:checkRuntimeBoundaries :protomolt-repo-container:test --tests '*DriveScopedLookupIT' :protomolt-repo-container:admissionStorageTest --console=plain
```

Journaled RepoServices now supplies publicationRepository with configured backend
selection and the bundled Document schema. The packaged test uses that path for
opaque publication and a typed call completing during shutdown. PostgreSQL,
versioned LocalStack and Git storage are real. Missing and foreign drive IDs
return NOT_FOUND before operation ownership, including incompatible foreign
backend settings.

Selection batches up to 64 IDs in one bounded SQL transaction. The full result
must match before any backend validation. Tests cover incomplete batches,
account boundaries, a complete pair, lock timeout and retry. Selection uses one
profile query plus one batch query. Latency and throughput benchmarks remain.

The schema includes complete generated imports, descriptor identity,
producer-reported compiler identity and loaded protobuf runtime version. The
compiler.properties build task uses the protoc dependency version provider.
Reflection avoids Java constant inlining. Missing identity fails initialization
before reader construction; resource-corruption startup tests remain separate.

Sol reviewed privacy, SQL limits, batching, provenance and construction order.
Public journaled configuration, transport adapters and library/gRPC parity remain.
No RPC is mounted. This run does not qualify performance or scaling.
