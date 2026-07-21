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
| `protomolt-embeddings-tei` | A remote provider for Hugging Face Text Embeddings Inference over gRPC |
| `protomolt-embeddings-ovms` | A remote provider for OpenVINO Model Server over the KServe v2 gRPC protocol |
| `protomolt-embeddings-harness` | Pairwise cosine-equivalence certification for providers serving the same model |

The SPI lives in `embeddings/core`, the providers under `embeddings/providers`,
and the certification harness in `embeddings/harness`:

```
embeddings/
  core/                  protomolt-embeddings
  providers/model2vec/   protomolt-embeddings-model2vec
  providers/tei/         protomolt-embeddings-tei
  providers/ovms/        protomolt-embeddings-ovms
  harness/               protomolt-embeddings-harness
```

## The provider SPI

`EmbeddingProvider` is a `ServiceLoader` contract with three methods: a
stable `providerId()` (e.g. `model2vec`), the fixed `dimension()` of every
vector the provider produces, and `embed(String)`. A default `embedAll`
loops over `embed` in order; providers with a real batch API override it.

The SPI is `AutoCloseable`: providers may hold connections, and `close()`
releases whatever the provider holds. The default is a no-op, so
in-process providers need no override, and `close()` never throws a
checked exception. Whoever obtained a provider closes it.

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

## Remote providers

Two providers call remote inference servers over gRPC. Both use a plaintext
channel, because each server serves plain gRPC behind a trusted network
boundary; TLS can be added later when a deployment calls for it. Both follow
the model2vec configuration pattern: the no-argument constructor ServiceLoader
uses resolves its knobs on first use from a system property, falling back to
an environment variable, so discovery never fails on an unconfigured provider.
First use without the knobs fails with a message naming them.

### TEI

`TeiEmbeddingProvider` registers under the id `tei` and calls a Hugging Face
Text Embeddings Inference server. TEI serves one embedding model per process,
chosen server side, so the knobs are the gRPC target and a truncation switch:

| Property | Environment variable | Example |
|---|---|---|
| `protomolt.embeddings.tei.target` | `PROTOMOLT_TEI_TARGET` | `localhost:8071` |
| `protomolt.embeddings.tei.truncate` | `PROTOMOLT_TEI_TRUNCATE` | defaults to `true` |

By default every request asks the server to truncate inputs past the model's
maximum sequence length instead of failing the batch; a deployment that wants
hard failure on overlength input sets the truncate knob to `false`. The value
is parsed with `Boolean.parseBoolean` and read once, on the first embed call.
`normalize` is deliberately left unset on requests, so the TEI server's own
default applies.

`embedAll` issues one concurrent unary Embed call per text on virtual
threads. TEI batches concurrent requests server side (dynamic batching), so
concurrent unary calls get the batching benefit without a client-side batch
API. Results are collected in input order; when a collected call has failed,
every call not yet collected is cancelled and the failure propagates. A
failure is only noticed once collection reaches it, so a still-running call
earlier in the input order delays it, bounded by the per-call 30 second
deadline. TEI's Info RPC reports no vector dimension, so `dimension()` embeds
a fixed probe string once and caches the vector length.

### OVMS

`OvmsEmbeddingProvider` registers under the id `ovms` and calls an OpenVINO
Model Server embeddings servable over the KServe v2 gRPC prediction protocol
(the same wire protocol NVIDIA Triton speaks):

| Property | Environment variable | Example |
|---|---|---|
| `protomolt.embeddings.ovms.target` | `PROTOMOLT_OVMS_TARGET` | `localhost:9071` |
| `protomolt.embeddings.ovms.model` | `PROTOMOLT_OVMS_MODEL` | `bge-small-en` |
| `protomolt.embeddings.ovms.input` | `PROTOMOLT_OVMS_INPUT` | defaults to `input` |
| `protomolt.embeddings.ovms.output` | `PROTOMOLT_OVMS_OUTPUT` | defaults to `embedding` |

`embedAll` splits the batch into chunks of at most 256 texts
(`OvmsEmbeddingProvider.MAX_TEXTS_PER_REQUEST`) and sends one
`ModelInferRequest` per chunk, each under its own fresh 30 second deadline
rather than one deadline over the whole batch: gRPC's default 4MB message cap
means a few thousand texts in one request hit RESOURCE_EXHAUSTED, and a large
batch on a CPU server can outrun a single 30 second deadline. Each request
carries a single BYTES input tensor of shape `[N]` with the chunk's N texts
as UTF-8, since OVMS embeddings servables accept raw strings and tokenize
server side; the FP32 output tensor of shape `[N, dim]` is read from
`fp32_contents` when populated and from `raw_output_contents` (little-endian
F32) otherwise, because KServe servers commonly answer with raw contents. The
chunks' vectors return concatenated in input order. `embed()` is unaffected:
a single-text call is one request. A failed chunk call still throws
`IllegalStateException` naming the model and target, so a multi-chunk
`embedAll` fails as a whole — no partial results are returned. As with TEI,
`dimension()` is learned by a one-time probe.

## Certification

Two providers serving the same model must produce near-identical vectors
(per-text cosine ~1) before a runtime can mix them, for example indexing with
one and querying with the other. `EmbeddingEquivalence.compare(a, b, texts,
threshold)` in `protomolt-embeddings-harness` embeds a corpus with both
providers and reduces the per-text cosines to an `EquivalenceReport`: the pair
is certified when the worst text still clears the threshold. `compare` is
static — call it directly, there is no instance to construct.

Besides the worst and mean cosine, the report carries `minNormRatio` and
`maxNormRatio`: the range of per-text L2-norm ratios `norm(a) / norm(b)`,
with two zero norms defined as 1.0 and a nonzero norm over a zero norm as
positive infinity. Certification remains cosine-only: cosine certifies
direction, and because cosine is scale-invariant a pair can certify at 1.0
while disagreeing on normalization — a min/max norm ratio of 0.5, say, when
one provider returns unnormalized vectors. The norm ratios expose that scale
disagreement, which matters when an index scores with L2 or dot product.
`Cosines` exposes the pieces: `cosine(float[], float[])` and the L2
`norm(float[])`.

model2vec is a different model family from the transformer models TEI and
OVMS serve, so it never certifies against a transformer provider. It is the
standalone CPU fast path, not a mixable provider.

`TeiOvmsEquivalenceLiveIntegrationTest` runs this check against live servers
when `PROTOMOLT_TEI_TARGET`, `PROTOMOLT_OVMS_TARGET`, and
`PROTOMOLT_OVMS_MODEL` are all set, and skips otherwise.
