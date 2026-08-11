# Reranking

The rerank modules score a query's candidate documents so a search pipeline
can re-order its hits before answering. A *provider* takes a query and a list
of texts and returns one relevance score per text; the scores align with the
input order, higher meaning more relevant.

Absolute score scales are provider-specific: TEI answers sigmoid-normalized
scores by default while other runtimes answer raw logits. Only the ORDER a
provider produces is comparable across providers, so the certification
harness compares rankings, never raw score values.

| Artifact | Role |
|---|---|
| `protomolt-rerank` | The `RerankProvider` SPI and `RerankProviders` discovery |
| `protomolt-rerank-tei` | A remote provider for Hugging Face Text Embeddings Inference over gRPC |
| `protomolt-rerank-ovms` | A remote provider for OpenVINO Model Server over the REST rerank endpoint |
| `protomolt-rerank-harness` | Ranked-list equivalence certification for providers serving the same model |

The SPI lives in `search/rerank/core`, the providers under `search/rerank/providers`, and
the certification harness in `search/rerank/harness`:

```
rerank/
  core/                  protomolt-rerank
  providers/tei/         protomolt-rerank-tei
  providers/ovms/        protomolt-rerank-ovms
  harness/               protomolt-rerank-harness
```

## The provider SPI

`RerankProvider` is a `ServiceLoader` contract with two methods: a stable
`providerId()` (e.g. `tei`) and `score(query, texts)`, which returns one
`Double` per text in input order. A default `rank(query, texts, topK)` scores,
sorts by descending score, and truncates, returning `ScoredText` records that
carry the input index so callers can map the ranking back to their own
candidate list.

`RerankProviders` discovers implementations: `all()` returns every provider
on the classpath keyed by id, and `byId(String)` resolves one, failing with
the list of available ids when the requested provider is not there.
Registration is the standard
`META-INF/services/ai.pipestream.proto.rerank.RerankProvider` file.

## The TEI provider

`TeiRerankProvider` registers under the id `tei` and calls a Hugging Face
Text Embeddings Inference server over its gRPC Rerank API, reusing the
generated `tei.v1` stubs from `protomolt-embeddings-tei`. TEI serves one
reranker model per process, chosen server side, so the only knob is the gRPC
target:

| Property | Environment variable | Example |
|---|---|---|
| `protomolt.rerank.tei.target` | `PROTOMOLT_RERANK_TEI_TARGET` | `localhost:8071` |

One unary Rerank call carries the query and every candidate text, with
`raw_scores` left false so the server answers sigmoid-normalized scores, the
scale most deployments compare. The response's ranks arrive sorted by score,
each carrying the index of the text it scores, so the provider scatters them
back into input order; ranks that do not cover the request's texts exactly
once fail the call. The channel is plaintext with a 30 second deadline, the
same posture as the embeddings provider.

## The OVMS provider

`OvmsRerankProvider` registers under the id `ovms` and calls an OpenVINO
Model Server rerank servable. This provider speaks the OpenAI-style REST
rerank endpoint, not gRPC: the OVMS rerank servable's graph expects an HTTP
payload packet, so the gRPC ModelInfer path answers "ovms::HttpPayload was
requested" on these servables. The client is a plain `java.net.http`
`HttpClient` with a 10 second connect timeout and a 30 second request
timeout.

| Property | Environment variable | Example |
|---|---|---|
| `protomolt.rerank.ovms.url` | `PROTOMOLT_RERANK_OVMS_URL` | `http://localhost:8003` |
| `protomolt.rerank.ovms.model` | `PROTOMOLT_RERANK_OVMS_MODEL` | `bge-reranker` |

Each call POSTs `{base}/v3/rerank` with a JSON body of `model`, `query`, and
`documents`, and reads the `results` array of index/relevance_score objects
back into input order. A non-2xx status fails the call naming the URL, the
model, the status, and the body; results that miss or repeat an index fail
the call too. An empty candidate list short-circuits to an empty result
without a request.

## Certification

Two providers serving the same reranker must produce the same ranking before
a runtime can mix them, for example reranking with a different runtime in a
fallback path. Because score scales differ across providers, certification
is on ranking plus top-1 agreement, never on raw score values:
`RerankEquivalence.compare(a, b, cases, threshold)` in
`protomolt-rerank-harness` scores every case's documents with both providers
and reduces the per-query Kendall tau-b correlations and the argmax agreement
count to a `RerankEquivalenceReport`. The pair is certified when the worst
query's tau clears the threshold.

`compare` also takes a `scoreEpsilon`: score differences at or below the
epsilon are treated as ties, modeling the noise floor of a provider's scoring
scale. Set it around 1e-3 for sigmoid-scaled providers, where two runtimes
can round the same tiny probability differently; sub-floor jitter then counts
as the same relevance instead of dragging tau down through the tie
correction. The default 0 is strict: only exactly equal scores tie.

`TeiOvmsRerankEquivalenceLiveIntegrationTest` runs this check at threshold
0.9 with a 1e-3 score epsilon against live servers when
`PROTOMOLT_RERANK_TEI_TARGET`, `PROTOMOLT_RERANK_OVMS_URL`, and
`PROTOMOLT_RERANK_OVMS_MODEL` are all set, and skips otherwise.
