# Publishing schemas to registries

`SchemaPublisher` (in `protomolt-proto-sources`) is the write-side
counterpart of `DescriptorLoader`: it registers a `ProtoSourceSet` — however
it was [gathered](gathering.md) — with a schema registry. Implementations
exist for Confluent-compatible registries and Apicurio Registry v3, so a
typical bridge reads: gather from Git (or jars, or disk), publish to the
registry your Kafka serdes already talk to.

```java
var sources = gatherer.gather();

try (var publisher = new ConfluentSchemaPublisher(URI.create("http://localhost:8081"))) {
    PublishResult result = publisher.publish(sources, PublishOptions.defaults());
    result.throwIfFailed();
}
```

## Semantics

All publishers share these guarantees:

- **References first.** Files register in reverse-topological import order
  (`ProtoSourceSet.topologicalOrder()`), so every schema reference exists
  before the file that imports it. `google/protobuf/*` imports are never
  registered — registries do not hold the well-known types.
- **Idempotent.** Re-publishing identical content reports `UNCHANGED` and
  writes nothing, so a publish step can run on every CI build without
  minting spurious versions.
- **Compatibility is the registry's.** A server-side compatibility or
  validity rejection surfaces as a `FAILED` outcome for that file — with the
  registry's message — and the remaining files still publish. Only
  registry-level failures (unreachable, authentication, server errors) throw.
- **Dry run.** `PublishOptions.dryRunDefaults()` performs every read but no
  write, reporting `WOULD_WRITE`/`UNCHANGED` per file.

`PublishResult` carries one outcome per file (`CREATED`, `UPDATED`,
`UNCHANGED`, `WOULD_WRITE`, `FAILED` with detail) plus counts and
`throwIfFailed()`.

## Subject naming

`SubjectNamingStrategy` maps import paths to subject/artifact names. The
default is the import path itself (`common/v1/core.proto`), which
round-trips with how registries carry references — a reference's `name` is
the import path, so a subject named after it is discoverable from the
reference alone, and the registry loaders resolve the graph without extra
configuration. `baseName()` and `prefixed(...)` are available, and any
function works:

```java
var options = PublishOptions.defaults()
    .withNaming(SubjectNamingStrategy.prefixed("schemas/"));
```

## Confluent-compatible registries

`ConfluentSchemaPublisher` (in `protomolt-schema-confluent`) speaks the
subjects REST API — the same protocol Apicurio's ccompat facade serves. It
checks for identical existing content via the schema-lookup endpoint before
writing, distinguishes `CREATED` from `UPDATED`, and maps HTTP 409/422 to
per-file failures. Constructors mirror `ConfluentSchemaRegistryLoader`
(base URI, optional timeouts, `AutoCloseable`, anonymous HTTP).

## Apicurio Registry

`ApicurioSchemaPublisher` (in `protomolt-schema-apicurio`) uses the Apicurio
v3 SDK client with a configurable group. Idempotency rides Apicurio's
find-or-create-version semantics; imports become artifact references with the
import path as the reference name, so `ApicurioDescriptorLoader` resolves
them back.

```java
var publisher = ApicurioSchemaPublisher.builder()
    .registryClient(client)
    .groupId("default")
    .build();
```

## Round-trip guarantee

The integration test suites publish reference-linked source sets and load
them back through the corresponding loaders against three live endpoints
(Apicurio v3, Apicurio ccompat, Confluent Schema Registry), asserting that
the root type and its imports resolve. See
[Building and testing](building.md) for running them.
