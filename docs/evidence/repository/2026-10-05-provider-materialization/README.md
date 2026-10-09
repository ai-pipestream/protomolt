# Selected historical decoding through a real provider

Parent commit: `46bd8a46c0edf6bf7c5bc0e84174d03269129f42`.
Source hashes, admission runtime artifact inventory and JUnit XML are retained
alongside this file.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```

The production-JAR PostgreSQL/LocalStack host and restart/expiry gate passed. The
harness compiles its probe against the actual production JAR classpath, starts a
separate JVM, and asserts `NATIVE_HISTORICAL_MATERIALIZATION_OK` alongside its
existing host/recovery markers. This is one JUnit integration case with several
probe scenarios, not five separate JUnit tests.

The new probe establishes:

- A historical multiparts revision fetches only the requested fragment through
  the exact recorded backend generation/profile, namespace, key and version.
- A newer version at the same provider key does not replace the archived bytes.
- Decoding returns the original payload and its recorded schema, with an
  independently retained pin and reserved bytes until result close.
- An unknown ordinal performs no provider GET and returns NOT_FOUND.
- Revocation after a real GET suppresses both successful content and a subsequent
  injected private provider exception, including its cause/suppressed details.
- Cancellation after a real GET returns no result; corruption injected into real
  returned bytes fails the provider digest check as DATA_LOSS.
- Provider work, result bytes and ledger pins drain after every scenario.

Fault injection wraps real provider calls and only changes behavior after GET
returns. It does not manufacture successful storage. The corruption case tests
provider byte integrity, not malformed Any parsing. The fixture restores current
security after its tests. Sol reviewed the implementation and probe and found no
material blocker.

This correctness gate does not establish latency, throughput, forced-crash host
recovery, public RPC compatibility or deployment. SQL materialization still
captures the full bounded retained-schema artifact set. RustFS remains the local
performance backend. The new concrete Java operation does not yet extend the
HistoricalDocumentRepository SPI or protobuf/gRPC contracts.
