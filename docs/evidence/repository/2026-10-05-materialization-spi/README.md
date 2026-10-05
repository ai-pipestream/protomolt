# Selected historical materialization SPI

Parent: `d3bbe288747b6d1ee5da7868ea8b8c709fe1396a`.
Sol reviewed the interface, adapter and real-provider probe with no blocker.

```sh
./gradlew :protomolt-repo-spi:checkRuntimeBoundaries \
  :protomolt-repo-container:admissionStorageTest --console=plain
```

Local gate passed in 2m 13s. The storage task is one JUnit case running production
JARs in separate JVMs with real PostgreSQL and LocalStack, including its existing
restart/expiry gates. This is not a hard-crash recovery or performance result.

The five selected-materialization probe scenarios now call the optional SPI type.
They retain old-version selection, exact backend identity, one selected GET,
unknown ordinal without GET, revocation after GET, cancellation, corruption and
resource-drain checks. The success scenario additionally checks request/ordinal
correlation, current denial on a subsequent view, post-close refusal and idempotent
close. Faults wrap real provider responses; they do not simulate successful storage.

The SPI runtime dependency gate excludes container/engine/service, SQL, Kafka and
S3 modules. No new SPI dependency was added. Public protobuf/gRPC contracts remain
unchanged; registry cache and transport exposure remain separate unfinished work.
