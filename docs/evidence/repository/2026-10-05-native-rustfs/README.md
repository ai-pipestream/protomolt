# Native repository correctness on RustFS

The opt-in `:protomolt-repo-container:admissionRustFsTest` passed against the
existing pinned `rustfs/rustfs:1.0.0-beta.11-preview.1` container with PostgreSQL 18.
The fixture compiles its child probes against the production JAR graph and runs
validation/storage/recovery in standard JVMs without JUnit on their classpath.
The same fixture remains the default LocalStack `admissionStorageTest` gate.

This qualifies the existing suite against RustFS: real versioned provider reads,
typed publication and invalid retained assessment, immutable-part reuse, exact
replay after lost acknowledgements, schema-only and routing-metadata revisions,
registry-free historical decoding, current authorization, cancellation/drain,
and controlled fresh-process recovery. Internal negative fixtures retain their
explicitly labelled synthetic observations; provider-dependent cases execute the
actual adapter. It is not a claim that every internal negative fixture performs
network I/O or that all S3 operations have been qualified.

The single JUnit wrapper passed without failures, errors or skips. It asserts the
individual probe completion markers and child exit status. Its 126.08-second
elapsed duration includes expiry waits, process startup and deliberate failures.
**It is not a latency or throughput measurement.** Native 1/2/4-process traffic
with timed operation samples, fixed/added SQL budgets and resource telemetry is
still pending, as specified in the repository composition design.

`backend.txt` preserves the selected backend, image tag and local image ID from
the test's standard output. The image ID is not a registry manifest digest.
`sources.sha256` identifies the selector, harness and task wiring used for this
run. These are local results, not hosted CI, deployment, production capacity or
completion of the repository goal.

```sh
./gradlew :protomolt-repo-container:admissionRustFsTest --console=plain
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```

The unchanged LocalStack-default path also passed after the selector extraction,
with no failures, errors or skips. Both backends therefore execute the same
fixture and acceptance markers through distinct Gradle tasks and reports. Sol
reviewed selection, container lifetime and task wiring without a blocker.
