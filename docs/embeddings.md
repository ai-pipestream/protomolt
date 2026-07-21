# Text embeddings

The embeddings modules fill the VECTOR fields of a search document from its
TEXT fields, using the same [indexing plan](indexing.md) the engine plugins
interpret. A *provider* turns text into fixed-length float vectors; the
*plan embedder* applies a provider to the document maps an engine mapper
produced, locating and validating both fields through the plan.

| Artifact | Role |
|---|---|
| `protomolt-embeddings` | The `EmbeddingProvider` SPI and the plan-driven `PlanEmbedder` |
| `protomolt-embeddings-model2vec` | A Model2Vec static-embedding provider backed by OpenNLP |

## The provider SPI

`EmbeddingProvider` is a `ServiceLoader` contract with three methods: a
stable `providerId()` (e.g. `model2vec`), the fixed `dimension()` of every
vector the provider produces, and `embed(String)`. A default `embedAll`
loops over `embed` in order; providers with a real batch API override it.

`EmbeddingProviders` discovers implementations: `all()` returns every
provider on the classpath keyed by id, and `byId(String)` resolves one —
failing with the list of available ids when the requested provider is not
there. Registration is the standard
`META-INF/services/ai.pipestream.proto.embeddings.EmbeddingProvider` file.

## Embedding a mapped document

`PlanEmbedder` joins a provider to an `IndexingPlan`. Its input is the
`Map` form a search-engine mapper produced (OpenSearch or Solr document
maps); it reads the plan's TEXT field from the document, embeds it, and
writes the vector into the plan's VECTOR field as a `List` of boxed
`Float`s — the same shape engine mappers emit for a repeated float field.
The document is mutated in place and returned.

The no-argument `embed(document)` requires the plan to have exactly one
TEXT and one VECTOR field; plans with several take
`embed(document, textFieldName, vectorFieldName)`. Field resolution and
the dimension check — the provider's `dimension()` against the hint's
`vector_dims`, when the hint sets one — run before the document is
consulted, so a misconfigured embedder fails on every call, not only on
documents that carry text. A text field that is absent or empty leaves the
document unchanged: there is nothing to embed, and a placeholder vector
would poison similarity scores.

The flow end to end — hints declare the two fields, the plan resolves
them, the mapper produces the document, the embedder fills the vector, and
the sink lands it in OpenSearch:

```protobuf
import "ai/pipestream/proto/index/hints/v1/indexing_hints.proto";

message Sentence {
  string sentence = 1 [(ai.pipestream.proto.index.hints.v1.index) = { type: INDEX_FIELD_TYPE_TEXT }];
  repeated float embedding = 2 [(ai.pipestream.proto.index.hints.v1.index) = {
    type: INDEX_FIELD_TYPE_VECTOR
    vector_dims: 256
  }];
}
```

```java
var plan = IndexingPlanFactory.defaults(new CatalogIndexingHintSource())
        .create(message.getDescriptorForType());
var mapper = new OpenSearchDocumentMapper(new ProtoFieldMapperImpl(new DescriptorRegistry()));
var embedder = new PlanEmbedder(EmbeddingProviders.byId("model2vec"), plan);

try (var sink = new OpenSearchSink("http://localhost:9200")) {
    sink.ensureIndex("sentences", plan); // knn_vector mappings, index.knn enabled
    sink.bulkWrite("sentences",
            List.of(embedder.embed(mapper.map(message, plan))), true);
}
```

As with the other descriptor-option standards, parse descriptor sets with
the hint extensions registered (see [Search indexing](indexing.md)), or
the options arrive as unknown fields and the plan sees no hints.

The live suite (`SemanticSearchLiveIntegrationTest` in
`:protomolt-embeddings-model2vec`) proves exactly this flow against a real
OpenSearch: sentences indexed with model-computed vectors, a query
embedded with the same provider, and kNN ranking the semantically nearest
sentence first — the query word appears in no indexed sentence, so only
the vector space connects them.

## The Model2Vec provider

`Model2VecEmbeddingProvider` registers under the id `model2vec` and wraps
OpenNLP's `StaticEmbeddingModel`: a distilled per-token vector table with
subword tokenization and mean pooling, no model forward pass — so there is
no inference runtime to ship. A text with no in-vocabulary tokens yields a
zero vector. The loaded model is immutable and the provider is safe for
concurrent use.

The model directory is a Model2Vec release layout as
`StaticEmbeddingModel.load(Path)` reads it: the `model.safetensors`
token-vector table next to its tokenizer definition (`tokenizer.json` for
SentencePiece tokenizers; `vocab.txt` with `tokenizer_config.json` for
WordPiece) and `config.json`.

Constructed with a `Path`, the provider loads the model eagerly. The
no-argument constructor — the one `ServiceLoader` uses — resolves the
directory on first use instead, so discovery never fails on an
unconfigured provider that is not actually used:

| Property | Environment variable |
|---|---|
| `protomolt.embeddings.model2vec.path` | `PROTOMOLT_MODEL2VEC_PATH` |

The system property wins when both are set; first use without either
fails, naming both knobs.

The provider depends on `ai.pipestream:opennlp-embeddings`, which resolves
from `mavenLocal` or the ai.pipestream registries — not from Maven Central
release repositories.
