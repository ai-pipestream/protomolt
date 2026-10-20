# search/

Index mappings, chunking, embeddings, rerank and the search service. The
mapping SPI turns descriptor-declared indexing hints into engine artifacts;
engine plugins register through ServiceLoader; the embedding and rerank SPIs
front local and remote model backends; the Lucene-backed search service
serves the query and indexing gRPC contract.

| Gradle project | Directory | What it is |
|---|---|---|
| `:protomolt-search-index-spi` | `search/index/spi` | The search-index SPI, indexing plans and the `index.hints.v1` dialect |
| `:protomolt-search-index-lucene` | `search/index/lucene` | Lucene document mapper plugin |
| `:protomolt-search-index-opensearch` | `search/index/opensearch` | OpenSearch document mapper, mapping generator and a thin HTTP sink |
| `:protomolt-search-index-solr` | `search/index/solr` | Solr document mapper and managed-schema generator |
| `:protomolt-search-index-qdrant` | `search/index/qdrant` | Qdrant point mapper and a thin gRPC sink |
| `:protomolt-search-index-ndjson` | `search/index/ndjson` | Engine-agnostic protobuf to NDJSON encoding |
| `:protomolt-search-index-protobuf` | `search/index/protobuf` | The descriptor-driven indexer and its `Any` payload gate |
| `:protomolt-search-chunk` | `search/chunk` | The deterministic sentence-packed chunker over pinned boundary rules |
| `:protomolt-search-embedding` | `search/embedding/core` | Embedding provider SPI and the mapping-driven document embedder |
| `:protomolt-search-embedding-model2vec` | `search/embedding/model2vec` | Model2Vec static embeddings (OpenNLP embeddings module) |
| `:protomolt-search-embedding-tei` | `search/embedding/tei` | Hugging Face Text Embeddings Inference provider over gRPC |
| `:protomolt-search-embedding-ovms` | `search/embedding/ovms` | OpenVINO Model Server provider over the KServe v2 protocol |
| `:protomolt-search-embedding-harness` | `search/embedding/harness` | Pairwise cosine-equivalence certification for providers of one model |
| `:protomolt-search-rerank` | `search/rerank/core` | Rerank provider SPI |
| `:protomolt-search-rerank-tei` | `search/rerank/tei` | TEI rerank provider |
| `:protomolt-search-rerank-ovms` | `search/rerank/ovms` | OVMS rerank provider (REST) |
| `:protomolt-search-rerank-harness` | `search/rerank/harness` | Ranked-list equivalence certification for rerank providers |
| `:protomolt-search-proto` | `search/proto` | The search contract: query surface and the indexing RPC |
| `:protomolt-search-service` | `search/service` | The Lucene-backed query and indexing gRPC service over mapping subjects |
| `:protomolt-search-snapshot-s3` | `search/snapshot-s3` | S3 snapshot store: commit-point index snapshots in a bucket |
| `:protomolt-search-console` | `search/console` | The browser search console over the search service |

Docs: [indexing](../docs/search/indexing.md),
[chunking](../docs/search/chunking.md),
[embeddings](../docs/search/embeddings.md),
[the search service](../docs/search/service.md),
[reranking](../docs/search/rerank.md).
