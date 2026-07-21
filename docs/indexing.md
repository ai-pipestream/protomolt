# Search indexing

The index modules project protobuf messages into search engines. The design
separates three concerns: *hints* say how a field should be indexed, a
*plan* resolves hints for a concrete message type, and *engine plugins*
interpret the plan for a specific backend. NDJSON output is engine-agnostic
and does not interpret hints at all.

| Artifact | Role |
|---|---|
| `protomolt-index-spi` | Plan model, hint sources, engine SPI, the hints `.proto` |
| `protomolt-index-ndjson` | Message → NDJSON lines (including bulk-index pairs) |
| `protomolt-index-lucene` | Lucene `Document` mapping |
| `protomolt-index-opensearch` | OpenSearch document-map mapping |
| `protomolt-index-solr` | Solr document-map mapping |
| `protomolt-protobuf-indexing` | Facade chaining optional validation → plan → NDJSON |

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
| Vectors | `vector_dims`, `vector_similarity` (cosine, dot product, L2, max inner product), `vector_element_type` (float32, byte), `hnsw { m, ef_construction }` | Lucene emits `Knn(Float\|Byte)VectorField` with the similarity function; OpenSearch/Solr carry the parameters into schema generation |
| Multi-fields | `sub_fields` | The classic text-plus-keyword pattern; named `field.sub` (OpenSearch) / `field_sub` (Solr) |
| Text analysis | `analyzer`, `search_analyzer` | Engine-interpreted names, carried into the plan and schema generation |
| Missing values | `null_value`, `skip_if_missing` | `null_value` substitutes a typed value when the field is unset |
| Sorting/faceting | `sortable`, `facetable` | Doc values in Lucene/Solr terms, `doc_values` in OpenSearch |
| Ranges | `INDEX_FIELD_TYPE_{INT,LONG,FLOAT,DOUBLE,DATE}_RANGE` | Applies to a message field with `(gte, lte)` or `(min, max)` scalar pairs; misuse is a planning error carrying the field path |
| Maps | `map_mode` | `FLATTEN` (dynamic keys), `ENTRIES` (key/value entries), `JSON` (one serialized field), `SKIP` |
| Dates | `date_format`, `date_resolution` (millis, seconds) | Controls `Timestamp` emission and schema-level formats |
| Escape hatch | `engine_params` | `map<string,string>` with engine-scoped keys, e.g. `opensearch.index_options`; carried verbatim into schema generation |

All of this is expressible programmatically through
`CatalogIndexingHintSource` and the `ResolvedFieldHint` builder for schemas
you cannot annotate.

### Engine schema generation

Each engine plugin can generate its schema artifact from an `IndexingPlan`,
so index setup and document mapping come from the same declaration:

- `OpenSearchMappingGenerator` — mappings JSON, including `knn_vector`
  (dimension, space type, HNSW method), multi-fields, `doc_values`,
  `null_value`, date formats, and `*_range` types.
- `SolrSchemaGenerator` — managed-schema field and fieldType definitions,
  including `DenseVectorField` types and copyField rules for sub-fields.
- `LuceneFieldSpecs` — Lucene has no schema file; this is a typed per-field
  report (doc-values type, vector encoding and similarity, analyzers) that
  consumers apply at `IndexWriter` level.

Hints do not have to live in the schema. `IndexingHintSource` is a
functional interface resolving a hint per field, and sources compose with
`orElse`:

- `ProtoOptionsIndexingHintSource` — reads the descriptor options above
- `CatalogIndexingHintSource` — programmatic side-car hints keyed by
  `messageFullName.fieldName`, for schemas you cannot annotate
- `InferringIndexingHintSource` — infers a sensible field kind from the
  protobuf type when nothing else matches

## Plans and engines

`IndexingPlanFactory` walks a descriptor with the configured hint sources
and produces an `IndexingPlan`: the indexable fields, their resolved kinds,
and their paths (nested messages expand to dotted paths unless marked as
object/nested). Engine plugins implement `SearchEngineIndexerProvider` and
are discovered via `ServiceLoader`:

```java
ExtensionRegistry extensions = ExtensionRegistry.newInstance();
ProtoOptionsIndexingHintSource.registerExtensions(extensions);
// parse the FileDescriptorSet / build descriptors with that registry

var plan = IndexingPlanFactory.defaults(new CatalogIndexingHintSource()).create(desc);
var engines = SearchEngineIndexers.createAll(new IndexerContext(fieldMapper));
engines.get("lucene").map(message, plan);
engines.get("opensearch").map(message, plan);

new ProtoNdjsonWriter().writeBulkIndex(bulk, "docs", id, message);
```

As with the other descriptor-option standards, register the hint extensions
before parsing descriptor sets, or the options arrive as unknown fields.

## The validate-then-index facade

`ProtobufIndexer` in `protomolt-protobuf-indexing` chains the pieces for the
common case — optionally validate, then plan, then emit NDJSON:

```java
var indexer = ProtobufIndexer.defaults(
    ProtoValidator.forMessageType(doc.getDescriptorForType()));
indexer.plan(doc.getDescriptorForType());
indexer.toNdjsonLine(doc);   // validates first when a validator is configured
```

Validation and indexing remain independent standards; chain them only when
you want the gate.


## Sensitivity in the index

`render-index-mappings` (OpenSearch) accepts a `sensitivity` object that
applies the schema's declared sensitivity classes
(`ai.pipestream.proto.meta.v1.field.sensitivity`) to the search layer:

- `{"encrypt": ["pii"]}` — those fields render as store-only ciphertext
  containers (`"type": "keyword", "index": false, "doc_values": false`).
  Pair with the masker's `encrypt` strategy: the document carries AES-GCM
  ciphertext the engine cannot read — and refuses to search — while the
  kNN embedding computed from the plaintext stays fully searchable.
  Semantic search over content the engine never sees in the clear; only
  key holders decrypt what comes back.

  **Know the boundary — this is not encryption of the search itself.**
  The text is cryptographically protected, but the vector is derived from
  the plaintext and leaks through two channels: *neighborhood* (clustering
  reveals what documents are about and which are alike, no key needed)
  and *inversion* (for known embedding models, published attacks
  reconstruct approximate text from the vector). Treat the vectors as
  confidential in their own right — index-level access control and
  encryption at rest are part of the design, not optional extras. What
  this buys concretely: a leaked `_source`, backup, or over-broad reader
  yields ciphertext, and verbatim text never exists inside the engine.
- `{"mask": ["pii"]}` and `{"exclude": ["secret"]}` — emitted as a
  security-plugin role fragment (`masked_fields` hashes values at query
  time; `fls` entries like `~field` hide fields outright). Apply the
  fragment to reader roles on a security-enabled cluster. Note the
  plugin's own boundary: masked fields cannot be *searched* — masking is
  applied after indexing, so the inverted index still holds the original
  terms and the plugin refuses to query them.
- `{"maskFormat": {"pii": "::SHA-512"}}` — a per-class format appended to
  each `masked_fields` entry, verbatim. The plugin's default hash is
  BLAKE2b; `::SHA-512` (or any JVM-provided algorithm) picks another, and
  `::/regex/::replacement` rewrites instead of hashing, chaining left to
  right.
- `{"role": {"indexPatterns": ["docs-*"], "allowedActions": ["read"]}}` —
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
Docker. `TeiSemanticSearchLiveIntegrationTest` runs it with the real TEI
providers and needs two servers besides Docker:

| Environment variable | Points at |
|---|---|
| `PROTOMOLT_TEI_TARGET` | TEI embeddings server `host:port`, e.g. `localhost:8071` (384-dim all-MiniLM-L6-v2) |
| `PROTOMOLT_RERANK_TEI_TARGET` | TEI rerank server `host:port`, e.g. `localhost:8072` |

Without both variables the TEI lane skips cleanly.
