# Apache Iceberg

`protomolt-iceberg` turns protobuf messages into Iceberg tables — real committed
snapshots any engine (Spark, Trino, DuckDB, Snowflake) can read — with the data
files written by ProtoMolt's own descriptor-driven Parquet emitter. Works
against any Iceberg catalog: REST, JDBC, Glue, Nessie, in-memory for tests.

A note on transport: Iceberg's catalog protocol is
[REST/OpenAPI](https://iceberg.apache.org/rest-catalog-spec/), not gRPC —
reports to the contrary usually trace back to systems that put their *own*
gRPC layer in front of an Iceberg library (Cloudberry's proposed Iceberg agent
does exactly that). This module speaks the catalog API through iceberg-core,
so a REST catalog is just a `RESTCatalog` instance.

## Writing

```java
Catalog catalog = new RESTCatalog();           // or JDBC, Glue, InMemory...
catalog.initialize("lake", Map.of("uri", "http://iceberg-rest:8181"));

Table table = IcebergSink.ensureTable(catalog,
        TableIdentifier.of("protomolt", "events"), eventDescriptor);
List<DataFile> committed = IcebergSink.append(table, eventDescriptor, batchOfMessages);
```

`ensureTable` creates the table with a schema converted from the descriptor:
every field optional (proto3 semantics), repeated fields as lists, maps as maps,
nested messages as structs, `google.protobuf.Timestamp` as `timestamptz`, and the
JSON well-known types (`Struct`/`Value`/`ListValue`) as JSON-carrying string
columns. Recursive message types cannot exist in a table schema and are rejected
with the cycle named.

`append` writes the batch through the table's `FileIO` and commits it as one
snapshot. The file schema carries **the table's own field ids**, stamped at write
time — columns resolve the way a native Iceberg writer's would, with no
name-mapping fallback in the read path. (That id-stamping seam,
`ProtoParquetSchemas.FieldIdResolver`, lives in the Parquet emitter and is
usable by any other table format.)

Each committed file carries **column metrics** — per-field lower and upper
bounds, value counts, and null counts, keyed by those same stamped field ids —
so query engines skip files that cannot match a predicate. The metrics are read
from the Parquet footer already in memory, at the Thrift level, so the read
stays **Hadoop-free** the way the emitter's write path is; a classloader-isolation
test fails the build if it ever loads a Hadoop class.

The round trip is enforced by test: files written here are read back through
*Iceberg's* generic reader — nested structs, lists, maps, timestamps, JSON
columns — so drift between the emitter's shapes and what Iceberg readers
expect fails the build, not a production query.

## Partitioning

```java
Table table = IcebergSink.ensureTable(catalog,
        TableIdentifier.of("protomolt", "events"), eventDescriptor, null,
        List.of(new IcebergPartitions.PartitionField("at", "day"),
                new IcebergPartitions.PartitionField("region", "identity")));
```

Partition columns are given by name with a transform — `identity`,
`year`/`month`/`day`/`hour` (on a timestamp source), `bucket[N]`, or
`truncate[W]`. The spec is bound **by column name**, so it survives the fresh
field ids a catalog assigns on creation, and a transform that does not fit its
source (say `day` on a string) fails at `ensureTable`, not at write time.

`append` then routes each message to its partition and writes one data file per
partition the batch touches, all in a single snapshot; each file carries its
partition value and its own column metrics. A descriptor can also declare its
partitioning inline through the `iceberg.partition` field label, read with
`IcebergPartitions.fromHints` (that label is a `map` value in the metadata option,
so it must be compiled with `protoc` — ProtoMolt's own inline compiler does not
yet parse map-typed custom options).

## Reading the lake's shape back

```java
String protoSource = IcebergSchemas.toProtoSource(table.schema(), "lake.v1", "Event");
```

A table schema becomes registrable `.proto` source — nested structs as nested
messages, lists as `repeated`, string/int-keyed maps as proto maps (other key
types become entry messages), timestamps as `google.protobuf.Timestamp`. The
generated source compiles, so the lake's shape can live in the schema registry
and flow through every other verb.

## The live integration suite

`docker-compose.integration.yml` includes `apache/iceberg-rest-fixture` on
port 18181 with a warehouse volume shared between the container and the host,
and `IcebergRestLiveIntegrationTest` drives the whole lane against it —
tables created over the wire, snapshots committed through REST, data read
back by Iceberg's reader. The suite skips when the catalog is down; CI runs
it with the stack up and fails if it skipped. Two portability notes baked
into the rig: table locations are client-owned (the catalog container and
the test JVM run as different users on CI, and whoever owns the tree must be
the data writer), and the client uses `LocalFileIO` — a plain-`java.nio`
`FileIO` this module ships because Iceberg's default hands `file://` paths
to `HadoopFileIO`, which relies on `Subject.getSubject`, removed in JDK 24+
(JEP 486).

## Object stores (S3)

By default the sink writes wherever the catalog's `FileIO` points, which for a
plain catalog is a local filesystem. `protomolt-iceberg-s3` is the opt-in escape
hatch to any S3-compatible store — RustFS, SeaweedFS, Ceph, or AWS S3 — through
Iceberg's own `S3FileIO`, with no Hadoop and no MinIO:

```java
Map<String, String> props = new HashMap<>(S3Catalogs.pathStyle(
        "http://localhost:9000", "us-east-1", accessKeyId, secretAccessKey));
props.put(CatalogProperties.URI, "http://localhost:8181");
RESTCatalog catalog = new RESTCatalog();
catalog.initialize("lake", props);   // tables now live on the object store
```

`S3Catalogs.pathStyle` sets `io-impl` to `S3FileIO`, the endpoint, path-style
addressing, credentials, and the JDK HTTP client (the module ships
`url-connection-client`, so Iceberg's default Apache client and Netty stay off the
classpath). For real AWS S3, `S3Catalogs.awsRegion(region)` drops the endpoint and
static credentials and lets the SDK's default provider chain supply them.
`S3FileIO` is only the file plane; atomic commit stays with the catalog, so no
store needs S3 conditional writes.

The live suite runs against **RustFS** (an Apache-2.0 S3 store) plus a second REST
catalog whose warehouse is on it, both in `docker-compose.integration.yml` — one
write, one read back through Iceberg, proving the whole lane on an object store
rather than a filesystem.

## Streaming in with Kafka Connect

`protomolt-connect-iceberg` is a Kafka Connect sink: topic records land as Iceberg
table rows, committed as snapshots through the same descriptor-driven emitter, with
no generated stubs and no gRPC. Each `put` batch is one snapshot, so offsets advance
only after the commit; delivery is at-least-once (a redelivered batch appends again,
and Iceberg does not deduplicate).

```properties
connector.class=ai.pipestream.proto.kafka.connect.iceberg.IcebergSinkConnector
topics=orders
value.converter=org.apache.kafka.connect.converters.ByteArrayConverter
schema.descriptor.set.base64=<base64 FileDescriptorSet>
message.type=shop.v1.Order
value.format=protobuf                        # or confluent, or json
iceberg.table=lake.orders
iceberg.partition=at:day,region:identity     # optional; applied only on create
iceberg.catalog.name=lake
iceberg.catalog.type=rest
iceberg.catalog.uri=http://iceberg-rest:8181
iceberg.catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO   # any catalog + FileIO
```

The descriptor set declares the row message (from `compile`/`reflect` or a registry
endpoint) and `message.type` selects it; record values decode as raw `protobuf`
bytes, `confluent` wire format, or proto3 `json`. Any catalog and `FileIO` Iceberg
supports work through the `iceberg.catalog.*` passthrough, so the object-store
escape hatch above applies here too. Undecodable values are `DataException`s the
worker routes by its error tolerance; a failed commit is retriable, so the
framework redelivers.

## Boundaries

- One data file per partition per `append` call; size your batches accordingly.
  Equality deletes and v3 variant columns are later phases.
- Row data goes only where the catalog and its `FileIO` say — this module
  never picks a storage location, matching the toolkit's disk policy.
- `iceberg-core` is the one heavyweight dependency, confined to this leaf
  module; the Parquet emitter underneath remains Hadoop-free.
