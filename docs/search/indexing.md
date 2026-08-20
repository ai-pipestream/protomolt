# Search indexing

The index modules project protobuf messages into search engines. The design
separates three concerns: *hints* say how a field should be indexed, a
*mapping* resolves hints for a concrete message type, and *engine plugins*
interpret the mapping for a specific backend. NDJSON output is engine-agnostic
and does not interpret hints at all.

| Artifact | Role |
|---|---|
| `protomolt-index-spi` | Mapping model, hint sources, engine SPI, the hints `.proto` |
| `protomolt-chunker` | Deterministic chunking-policy execution ([chunking](chunking.md)) |
| `protomolt-search-service` | The mapping-subject-gated query and indexing gRPC service ([search service](service.md)) |
| `protomolt-index-ndjson` | Message → NDJSON lines (including bulk-index pairs) |
| `protomolt-index-lucene` | Lucene `Document` mapping |
| `protomolt-index-opensearch` | OpenSearch document-map mapping |
| `protomolt-index-solr` | Solr document-map mapping |
| `protomolt-index-qdrant` | Qdrant point mapping (repo Document semantic chunks → named vectors), a gRPC sink, and collection-schema generation; validates declared rules on write |
| `protomolt-protobuf-indexing` | Facade chaining optional validation → mapping → NDJSON; registers the declared-rules gate for unpacked Any payloads |
| `protomolt-kafka-connect-opensearch` | Kafka Connect sink over this write path ([guide](../sink/kafka-connect-opensearch.md)) |

## Indexing hints

Hints are protobuf `FieldOptions` extensions that bake into the descriptor,
so plain `protoc` or the protobuf Gradle plugin is all the code generation
required. The `.proto` ships inside `protomolt-index-spi` (and is available
on the classpath as a resource):

```protobuf
import "ai/pipestream/proto/index/hints/v1/indexing_hints.proto";

message Doc {
  string doc_id = 1 [(ai.pipestream.proto.index.hints.v1.index) = { type: INDEX_FIELD_TYPE_KEYWORD }];
  string title  = 2 [(ai.pipestream.proto.index.hints.v1.index) = {
    type: INDEX_FIELD_TYPE_TEXT
    analyzer: "english"
    sub_fields: [{ name: "raw", type: INDEX_FIELD_TYPE_KEYWORD }]
  }];
  repeated float embedding = 3 [(ai.pipestream.proto.index.hints.v1.index) = {
    type: INDEX_FIELD_TYPE_VECTOR
    vector_dims: 768
    vector_similarity: VECTOR_SIMILARITY_COSINE
    hnsw: { m: 16, ef_construction: 128 }
  }];
}
```

### The hint surface

| Concern | Hint fields | Notes |
|---|---|---|
| Core | `type`, `name`, `stored`, `indexed` | `TEXT` vs `KEYWORD` distinguishes analyzed from exact-match strings |
| Vectors | `vector_dims`, `vector_similarity` (cosine, dot product, L2, max inner product), `vector_element_type` (float32, byte), `hnsw { m, ef_construction }` | Lucene emits `Knn(Float\|Byte)VectorField` with the similarity function; OpenSearch/Solr carry the parameters into schema generation; Qdrant renders named vectors with size and distance (`MAX_INNER_PRODUCT` maps to `Dot`, as OpenSearch maps it to `innerproduct`) |
| Multi-fields | `sub_fields` | The classic text-plus-keyword pattern; named `field.sub` (OpenSearch) / `field_sub` (Solr) |
| Text analysis | `analyzer`, `search_analyzer` | Engine-interpreted names, carried into the mapping and schema generation |
| Missing values | `null_value`, `skip_if_missing` | `null_value` substitutes a typed value when the field is unset |
| Sorting/faceting | `sortable`, `facetable` | Doc values in Lucene/Solr terms, `doc_values` in OpenSearch |
| Ranges | `INDEX_FIELD_TYPE_{INT,LONG,FLOAT,DOUBLE,DATE}_RANGE` | Applies to a message field with `(gte, lte)` or `(min, max)` scalar pairs; misuse is a mapping error carrying the field path |
| Maps | `map_mode` | `FLATTEN` (dynamic keys), `ENTRIES` (key/value entries), `JSON` (one serialized field), `SKIP` |
| Dates | `date_format`, `date_resolution` (millis, seconds) | Controls `Timestamp` emission and schema-level formats |
| Escape hatch | `engine_params` | `map<string,string>` with engine-scoped keys, e.g. `opensearch.index_options`; carried verbatim into schema generation |

All of this is expressible programmatically through
`CatalogIndexingHintSource` and the `ResolvedFieldHint` builder for schemas
you cannot annotate.

### Engine schema generation

Each engine plugin can generate its schema artifact from an `IndexMapping`,
so index setup and document mapping come from the same declaration:

- `OpenSearchMappingGenerator`: mappings JSON, including `knn_vector`
  (dimension, space type, HNSW method), multi-fields, `doc_values`,
  `null_value`, date formats, and `*_range` types.
- `SolrSchemaGenerator`: managed-schema field and fieldType definitions,
  including `DenseVectorField` types and copyField rules for sub-fields.
- `LuceneFieldSpecs`: Lucene has no schema file; this is a typed per-field
  report (doc-values type, vector encoding and similarity, analyzers) that
  consumers apply at `IndexWriter` level.
- `QdrantSchemaGenerator`: collection schema: one named vector per VECTOR
  hint (declared size and distance) plus payload field indexes for the
  scalar kinds, ready to apply with `CreateCollection` and
  `CreateFieldIndex` calls. On the write path, `QdrantPointMapper` enforces
  the declared `vector_dims` on every embedding and `QdrantSink` creates
  the collection from the same specs.

Hints do not have to live in the schema. `IndexingHintSource` is a
functional interface resolving a hint per field, and sources compose with
`orElse`:

- `ProtoOptionsIndexingHintSource`: reads the descriptor options above
- `CatalogIndexingHintSource`: programmatic side-car hints keyed by
  `messageFullName.fieldName`, for schemas you cannot annotate
- `InferringIndexingHintSource`: infers a sensible field kind from the
  protobuf type when nothing else matches

## Mappings and engines

`IndexMappingFactory` walks a descriptor with the configured hint sources
and produces an `IndexMapping`: the indexable fields, their resolved kinds,
and their paths (nested messages expand to dotted paths unless marked as
object/nested). Engine plugins implement `SearchEngineIndexerProvider` and
are discovered via `ServiceLoader`:

```java
ExtensionRegistry extensions = ExtensionRegistry.newInstance();
ProtoOptionsIndexingHintSource.registerExtensions(extensions);
// parse the FileDescriptorSet / build descriptors with that registry

var mapping = IndexMappingFactory.defaults(new CatalogIndexingHintSource()).create(desc);
var engines = SearchEngineIndexers.createAll(new IndexerContext(fieldMapper));
engines.get("lucene").map(message, mapping);
engines.get("opensearch").map(message, mapping);

new ProtoNdjsonWriter().writeBulkIndex(bulk, "docs", id, message);
```

As with the other descriptor-option standards, register the hint extensions
before parsing descriptor sets, or the options arrive as unknown fields.

### Paths under repeated ancestors

A mapping path may traverse a repeated message field — a `CHUNKS` block scope
expands its children (`chunks.text`), and an explicit hint can expand any
repeated message. On the write path every engine reads mapping entries through
`MappingValues`, which fans out over repeated intermediates depth-first and
emits the flattened leaf values as one multi-valued field, in document
order. An empty repeated ancestor (or an element whose singular parent is
unset) reads as missing, so `null_value` substitution applies as usual.
Paths without a repeated intermediate keep the field mapper's exact
semantics, Struct keys and Any unpacking included.

The exception is `VECTOR`: a KNN vector is one whole value, and
flattening sibling elements' floats would build a meaningless one, so a
vector path under a repeated ancestor fails loudly in the flat engines.
Per-element vectors index as their own entities instead — a `CHUNKS`
block scope on a block engine, or Qdrant's one-point-per-chunk mapping.

### `google.protobuf.Any`

Mapping time cannot know a single packed type (for example
`Document.structured_data`), so inference treats `google.protobuf.Any` as a
well-known kind (`ANY`), not a silent `OBJECT`. A hint that resolves the
field to any other kind — `SKIP` included — has said otherwise and wins;
only `ANY` entries expand. `blob_bag` bytes stay out of the index; Any is
not a blob.

At write time the indexer unpacks a set Any through the `DescriptorRegistry`
on `IndexerContext` (the same registry NDJSON already accepts for proto3 JSON
rendering). Inner fields are mapped with the parent hint chain and emitted
under `any_field.inner_path` (proto field names; engine names prefixed with
the Any field's engine name). Payloads that pack further Anys expand
recursively, bounded at 8 levels. An empty or unset Any contributes no inner
fields. An unknown type URL, a type URL without the `/` the Any contract
requires, value bytes that do not parse as the registered type, or value
bytes without a type URL is an error that names the field path (and type
URL) — never a silent skip, and each failure names its actual cause. Repeated Any fields, and Any fields under a
repeated ancestor (a `CHUNKS` block), have no single packed type per path
and keep their inert mapping entry instead. Schema generation does not invent
inner mappings for Any; those appear only when a concrete packed type is
seen on the write path.

Unpacked payloads pass through every `AnyPayloadValidator` discovered via
`ServiceLoader` before their fields are mapped. `protomolt-protobuf-indexing`
registers the declared-rules validation standard, so with that module on the
classpath a payload carrying `ai.pipestream.proto.validate.v1` (or, with the
optional reader, `buf.validate`) rules is validated on unpack and a violation
aborts the document — violation paths are reported under the Any field's
path. Payload types declaring no rules validate clean at no cost, and the
standard's own escape hatches (`skip_when`, per-field `ignore`) apply
unchanged.

The schema can also opt a single field out: `validate_payloads: false` on
the field's `(index)` hint indexes and renders that Any exactly as before
but keeps the validators off for payloads unpacked from it — on both the
engine and NDJSON write paths. Malformed Anys and unknown type URLs still
fail, and Anys nested inside the payload are gated under their own fields'
settings. The default is `true`.

The NDJSON path runs the same gate. A `ProtobufIndexer` whose writer carries
a `DescriptorRegistry` walks each outgoing message with `AnyPayloadGate`
before rendering: every set Any — singular, repeated elements, map values,
payloads packing further Anys (same 8-level bound) — is unpacked against
that registry and offered to the same validators. Because the walk follows
message values rather than mapping paths, repeated Anys and Anys under repeated
ancestors — inert on the expansion path — are validated here element by
element, with paths like `attachments[1].title` or `extras[cover].title`.
A registry-less writer cannot render packed Anys at all, so no gate runs
there.

## The validate-then-index facade

`ProtobufIndexer` in `protomolt-protobuf-indexing` chains the pieces for the
common case: optionally validate, then map, then emit NDJSON:

```java
var indexer = ProtobufIndexer.defaults(
    ProtoValidator.forMessageType(doc.getDescriptorForType()));
indexer.mapping(doc.getDescriptorForType());
indexer.toNdjsonLine(doc);   // validates first when a validator is configured
```

Validation and indexing remain independent standards; chain them only when
you want the gate. Engine plugins may gate on their own write path:
`QdrantPointMapper` runs the same validator before a document becomes
points, so an invalid document is never upserted.


## Sensitivity in the index

`render-index-mappings` (OpenSearch) accepts a `sensitivity` object that
applies the schema's declared sensitivity classes
(`ai.pipestream.proto.meta.v1.field.sensitivity`) to the search layer:

- `{"encrypt": ["pii"]}`: those fields render as store-only ciphertext
  containers (`"type": "keyword", "index": false, "doc_values": false`).
  Pair with the masker's `encrypt` strategy. The document carries AES-GCM
  ciphertext the engine cannot read or search, while the
  kNN embedding computed from the plaintext stays fully searchable.
  Semantic search over content the engine never sees in the clear; only
  key holders decrypt what comes back.

  **Know the boundary: this is not encryption of the search itself.**
  The text is cryptographically protected, but the vector is derived from
  the plaintext and leaks through two channels: *neighborhood* (clustering
  reveals what documents are about and which are alike, no key needed)
  and *inversion* (for known embedding models, published attacks
  reconstruct approximate text from the vector). Treat the vectors as
  confidential in their own right: index-level access control and
  encryption at rest are part of the design, not optional extras. What
  this buys concretely: a leaked `_source`, backup, or over-broad reader
  yields ciphertext, and verbatim text never exists inside the engine.
- `{"mask": ["pii"]}` and `{"exclude": ["secret"]}`: emitted as a
  security-plugin role fragment (`masked_fields` hashes values at query
  time; `fls` entries like `~field` hide fields outright). Apply the
  fragment to reader roles on a security-enabled cluster. Note the
  plugin's own boundary: masked fields cannot be *searched*. Masking is
  applied after indexing, so the inverted index still holds the original
  terms and the plugin refuses to query them.
- `{"maskFormat": {"pii": "::SHA-512"}}`: a per-class format appended to
  each `masked_fields` entry, verbatim. The plugin's default hash is
  BLAKE2b; `::SHA-512` (or any JVM-provided algorithm) picks another, and
  `::/regex/::replacement` rewrites instead of hashing, chaining left to
  right.
- `{"role": {"indexPatterns": ["docs-*"], "allowedActions": ["read"]}}`,
  additionally renders `security.role`, a complete role body ready to
  `PUT _plugins/_security/api/roles/{name}`: the schema decides what is
  masked and hidden, the request decides which indexes the role covers.
  Empty `masked_fields`/`fls` are omitted, since absent and empty mean
  different things to the plugin.

With `sensitivity` present the response becomes `{mappings, security}`.
The live integration suite proves the encrypted-store pattern end to end
against a real OpenSearch: index the ciphertext, find it by vector,
watch the engine refuse a term search on it, decrypt with the key.

## Semantic search with a rerank head

`protomolt-index-opensearch` also carries the read side of semantic search.
`OpenSearchSearch` is the thin sibling of `OpenSearchSink`: `knn(index,
vectorField, vector, k)` POSTs `/{index}/_search` with a `knn` query clause
and parses the hits into `OpenSearchHit` records (id, score, source map).

`RerankedSemanticSearch` builds the full pipeline on top of it, given an
`EmbeddingProvider` and a `RerankProvider`:

1. Embed the query with the same provider the index was built with.
2. Recall a deep candidate set with kNN (`candidates` hits).
3. Score every candidate's text against the query with the cross-encoder
   rerank provider.
4. Answer the reranked top-`k` as `RankedHit` records.

The two passes divide the work: the kNN list is recall (cheap, approximate,
runs over the whole index), the cross-encoder is precision (expensive per
candidate, so it only sees the recalled set). Run `candidates` comfortably
larger than `k`: the reranker can only reorder what the kNN pass recalled,
so the candidate depth bounds how much reordering is possible. Each
`RankedHit` carries both the kNN score and the rerank score; the two are
not commensurable, so neither can stand in for the other.

```java
var search = new OpenSearchSearch("http://localhost:9200");
var semantic = new RerankedSemanticSearch(search, embedder, reranker);
List<RankedHit> hits = semantic.search(
        "sentences", "embedding", "sentence", "a young dog", 10, 50);
```

The module depends only on the SPI jars (`protomolt-embeddings`,
`protomolt-rerank`); consumers pick the providers. See
[rerank.md](rerank.md) for the rerank SPI and the available providers.

The live lanes: `RerankedSearchLiveIntegrationTest` runs the pipeline
against a Testcontainers OpenSearch with fixture providers and needs only
Docker. `TeiSemanticSearchLiveIntegrationTest` runs it against real models
on a self-provisioned Testcontainers stack: OpenSearch plus two TEI CPU
containers (sentence-transformers/all-MiniLM-L6-v2 for embeddings,
BAAI/bge-reranker-base for reranking), with nothing running but Docker.

The TEI lane is tagged `tei` and excluded from the default test task, so
GitHub CI never runs it. Run it by hand with:

```
./gradlew :protomolt-index-opensearch:teiIntegrationTest
```

In CI it runs on the Forgejo lane `.forgejo/workflows/tei-integration.yml`.
The first run downloads about 1.2 GB of models from the HF hub. Setting
`PROTOMOLT_TEI_CACHE` to an existing directory bind-mounts it into both
TEI containers at `/data`, so the models persist across runs; the Forgejo
lane points it at `/work/tei-model-cache` on the runner.
