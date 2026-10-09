# Exact retained definition delivery

Parent: `834252e9dc5cb1a86f02644a85d5ba61399ad318`.
Sol reviewed selected-boundary identity, retained bytes and ownership with no blocker.

```sh
./gradlew :protomolt-repo-admission:test \
  :protomolt-repo-spi:checkRuntimeBoundaries \
  :protomolt-repo-container:admissionStorageTest --console=plain
```

All 224 admission tests pass, along with the SPI dependency boundary and the
production-JAR PostgreSQL/LocalStack storage/restart gate (one JUnit test with
multiple real-provider scenarios). The gate log and focused JUnit XML are retained.

The nested materialization test now checks exact child descriptor/metadata bytes
and the recorded asset reference after overwriting the original borrowed inputs.
It deletes its file-backed schema store, then reconstructs the selected descriptor
and decodes the original Any using only delivered bytes. Descriptor and metadata
digests match their independent recorded identities. Closed-result access fails.
The production-JAR SPI probe also checks both hashes and performs offline decoding
of a real archived provider payload using the delivered definition.

No extra reader call, byte copy, source export, schema compilation or registry
lookup was added. Returned immutable Java objects remain technically retainable;
callers must observe the borrowed lifetime. Close releases reservations/pins and
prevents new views, rather than revoking previously exposed Java references.

This is a Java delivery prerequisite for the planned selected-read wire contract.
No new RPC, performance claim, forced-crash qualification, merge or deployment.
