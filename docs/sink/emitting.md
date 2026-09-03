# Emitting bundles

The `acquire/gather/*` modules turn locations into proto sources. The
`sink/emit/*` modules reverse the flow. Renderers turn schemas or messages
into an ordered **bundle** of files. **Sinks** deliver a bundle to a
destination the caller names explicitly. Nothing in the emit pipeline ever
chooses a location on its own, keeping the toolkit's disk-footprint rule:
message data goes only where a caller deliberately sends it.

| Module | What it adds |
|---|---|
| `protomolt-emit` | `Bundle`, `BundleSink`, `DirectorySink`, `GitSink`, in-memory zip |
| `protomolt-emit-okf` | Open Knowledge Format renderer + the `emit-okf` verb |
| `protomolt-emit-parquet` | Descriptor-driven Parquet files from protobuf messages |
| `protomolt-emit-parquet-s3` | Parquet files uploaded to any S3-compatible store (RustFS, AWS S3) |

## The bundle and its sinks

A `Bundle` is an insertion-ordered map of bundle-relative paths to bytes.
Paths are validated on entry (no absolute paths, no `..`), so no sink can be
steered outside its destination.

```java
Bundle bundle = Bundle.builder()
        .add("index.md", "# hello\n")
        .add("tables/orders.md", ordersDoc)
        .build();

new DirectorySink(Path.of("/srv/knowledge")).write(bundle);
new GitSink(Path.of("/srv/knowledge-repo"), "Update knowledge bundle").write(bundle);
byte[] zip = Bundles.zip(bundle); // deterministic, entirely in memory
```

`DirectorySink` is write-only: it creates and overwrites the bundle's own
files and never deletes anything else. `GitSink` opens (or initializes) a
repository, stages exactly the bundle's paths, and commits: an unchanged
bundle produces no commit, so it is safe to emit on every schema change.
An S3 sink is planned as the next destination.

## Open Knowledge Format bundles

[OKF v0.1](https://github.com/GoogleCloudPlatform/knowledge-catalog/tree/main/okf)
is Google's open, vendor-neutral format for the curated knowledge agents and
data catalogs consume: plain markdown files with YAML frontmatter, one file
per concept, cross-linked into a knowledge graph.

ProtoMolt renders that straight off descriptors, because the schema already
carries the knowledge:

- every message, enum, and service becomes a concept document
  (`messages/<full.name>.md`, ...), with `type`, `title`, `description`, and
  `tags` frontmatter filled from the `ai.protomolt.proto.meta.v1`
  annotations;
- the `# Schema` table lists each field with its type, label, declared
  description, and sensitivity class, a column generic producers
  cannot offer because their metadata does not travel with the schema;
- message-typed fields become bundle-relative links to their type's concept
  document, so the descriptor graph *is* the knowledge graph;
- `index.md` files provide progressive disclosure, and the bundle root
  declares `okf_version: "0.1"`.

The `emit-okf` verb (available on gRPC, REST, MCP, and the registry's action
mount like every other verb) takes the standard schema envelope and returns
the bundle inline plus as one base64 zip:

```json
{"schema": {"sources": {"shop/v1/shop.proto": "..."}}, "title": "Shop schemas"}
```

No destination rides in the request: a remote caller gets bytes, never a
say in server filesystem paths. Delivery is the caller's move through a sink.

### A whole registry as a knowledge bundle

`OkfRegistryBundles.render(store, options)` renders every subject in a
schema registry: each subject becomes a `Registry Subject` concept with its
version table, a `resource` link to the registry API, and links to the
message types its latest schema declares: which get the standard concept
documents. Pointed at a `GitSink` on the registry's own repository, the
documentation lives beside the schemas it describes and updates with them.

## Parquet

`protomolt-emit-parquet` writes protobuf messages: dynamic or generated,
only the descriptor matters: as Parquet, entirely in memory:

```java
byte[] parquet = ParquetEmitter.toBytes(descriptor, messages);
Bundle file = ParquetEmitter.bundle("readings.parquet", descriptor, messages);
```

Mapping: plain singular scalars are `required` (proto3 defaults are values),
`optional`-keyword scalars and message fields are `optional` and written only
when present, repeated fields are `repeated`, maps are repeated
`(key, value)` groups, enums are plain UTF-8 strings holding the value
name, and unsigned 32-bit values widen to `int64` so no value changes
sign. Recursive
message types cannot exist in a columnar schema and are rejected with the
cycle named.

Every message is validated against the constraint and CEL rules its descriptor
declares before it is written, and the write fails on the first offending
message with every violation named. Opt out per export with
`ParquetExportOptions.NONE.withoutValidation()` (or on any other options
instance): for replaying rows that predate the rules, or when an upstream
stage already validated.

### Projection and masking on export

`ParquetExportOptions` controls what leaves in the file. Two independent tools:

```java
// Keep only some columns:
ParquetEmitter.toBytes(descriptor, messages, ids,
        ParquetExportOptions.project(Set.of("id", "amount")));

// Obscure sensitive fields, keeping every column:
ParquetEmitter.toBytes(descriptor, messages, ids,
        ParquetExportOptions.masking(Set.of("pii"), SensitivityMasker.Strategy.REDACT, null));
```

**Projection** drops columns from the file schema entirely; **masking** runs each
message through `SensitivityMasker` first, so fields carrying one of the named
sensitivity classes are redacted, encrypted, or cleared before they are written.
They compose. One distinction that matters: a proto3 plain scalar is a `required`
column, so `REMOVE` only clears it to its default: a zero still writes. To keep a
sensitive column *out* of the file, project it out; use `REDACT`/`ENCRYPT` to keep
the column and obscure the value. Masking reads the `sensitivity` field option, so
the descriptor must carry ProtoMolt's metadata extensions (as `compile` and
`reflect` produce) or nothing is masked. No Hadoop is added: the same
classloader-isolation test covers the export path.

The module depends on `parquet-hadoop` and `snappy-java`, with **zero Hadoop
jars**. Hadoop types appear only in `parquet-hadoop` method signatures
(compile-time), never at run time: the emitter supplies its own snappy-java
codec factory, so parquet's default `CodecFactory` (which materializes a
Hadoop `Configuration` and with it the 19 MB client-api jar plus the 34 MB
client runtime) is never touched. This is not an aspiration but a test:
`HadoopFreeParquetTest` runs the writer inside a classloader with every
Hadoop jar removed, so any regression fails naming the offending class. The
module is deliberately a leaf: depend on it only where Parquet output is
wanted.

### Landing files on S3

`protomolt-emit-parquet-s3` is the destination half: `S3ParquetSink` renders with
`ParquetEmitter` and uploads the bytes with one `putObject` per file, to any
S3-compatible store. `S3Clients` builds the client in the same idiom as
`protomolt-iceberg-s3`'s `S3Catalogs` - path-style with an endpoint override for
self-hosted stores (RustFS, SeaweedFS, Ceph), region-only for AWS S3:

```java
try (S3ParquetSink sink = new S3ParquetSink(
        S3Clients.pathStyle("http://localhost:9000", "us-east-1", accessKey, secretKey),
        "readings-lake")) {
    sink.put("readings/part-00000.parquet", descriptor, messages);
}
```

The bucket is fixed at construction and keys are validated as relative, so a caller
can name files but never steer the sink to another store. The Confluence connector's
`ParquetChangeSink` builds on it: crawl output buffers per entity kind and flushes to
`<prefix>/<entityType>/<runId>-part-<NNNNN>.parquet` once
`CONFLUENCE_PARQUET_S3_BUCKET` is set.

Alongside that raw archive, `ProjectedParquetChangeSink` (activated by
`CONFLUENCE_PROJECTED_PARQUET_S3_BUCKET`, same `CONFLUENCE_PROJECTED_PARQUET_S3_*`
option family) writes a derived dataset: every change is projected through the
`MessageProjection` engine into one flat `ConfluenceContentRow` (change id, operation,
content id and type, space, title, unified status, author, created_at, web URL,
version number, label names, storage-body length), where the page and blog post arms
of the entity oneof fill the same columns via the projection's candidate paths. Rows
land at `<prefix>/content/<runId>-part-<NNNNN>.parquet` (default prefix
`confluence-content-rows`), and DELETE changes appear as tombstone rows with only the
identity and operation columns set.

There is intentionally no `emit-parquet` verb: Parquet output is message
data, and this surface keeps message data out of server-side destination
arguments. Use the library API (or `S3ParquetSink` above) where
the caller is the operator.
