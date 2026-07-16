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
DataFile committed = IcebergSink.append(table, eventDescriptor, batchOfMessages);
```

`ensureTable` creates the table (unpartitioned) with a schema converted from
the descriptor: every field optional (proto3 semantics), repeated fields as
lists, maps as maps, nested messages as structs,
`google.protobuf.Timestamp` as `timestamptz`, and the JSON well-known types
(`Struct`/`Value`/`ListValue`) as JSON-carrying string columns. Recursive
message types cannot exist in a table schema and are rejected with the cycle
named.

`append` writes the batch as one Parquet file through the table's `FileIO`
and commits it as a snapshot. The file schema carries **the table's own field
ids**, stamped at write time — columns resolve the way a native Iceberg writer's
would, with no name-mapping fallback in the read path. (That id-stamping seam,
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

## Boundaries

- One data file per `append` call; size your batches accordingly. Partitioned
  tables and equality deletes are later phases.
- Row data goes only where the catalog and its `FileIO` say — this module
  never picks a storage location, matching the toolkit's disk policy.
- `iceberg-core` is the one heavyweight dependency, confined to this leaf
  module; the Parquet emitter underneath remains Hadoop-free.
