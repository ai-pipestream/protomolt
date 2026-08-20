# Court Documents → NDJSON → Lucene

End-to-end path over real court opinions: CourtListener JSONL → the platform's
generic `ai.pipestream.proto.repo.v1.Document` → projected NDJSON → Lucene
(text fields + HNSW float vectors), with one demo kNN query and one demo text
query at the end.

## Run

```bash
./gradlew :samples:runCourtDocIndex
# optional knobs:
./gradlew :samples:runCourtDocIndex -Plimit=100 -Pout=/tmp/court-out \
    -Pfixtures=/path/to/opinions_1000.jsonl \
    -Pmodel2vec=/path/to/model2vec-model-dir
```

Outputs (under `build/court-index-out` by default):

- `documents.ndjson` — one projected JSON object per line (`doc_id`, `title`,
  `body`, `language`, `source_uri`, `document_type`, `author`, `embedding`,
  `embedding_dims`)
- `lucene/` — Lucene index: keyword ids/classifications, analyzed `title`/`body`,
  `embedding` as `KnnFloatVectorField` (COSINE / HNSW)
- `model2vec/` — the sample embedding model (only when no real model is configured)

## Corpus

The bundled fixture `samples/src/main/resources/fixtures/court/opinions_sample.jsonl`
is the first 25 opinions of the CourtListener seed corpus
(`sample-documents/courtlistener-seed/.../opinions_1000.jsonl`, the same JSONL used
across the platform's seed data). One JSON object per line; the fields the sample
reads are `opinion_id`, `cluster_id`, `case_name`, `date_filed`, `judges`,
`author`, `precedential_status`, `opinion_type`, `docket_id`, `nature_of_suit`,
`plain_text`. `-Pfixtures=` accepts the full 1000-opinion file (or any superset).

## JSONL → Document mapping

`CourtDocuments.toDocument(JsonNode)` (pure, unit-tested in `CourtDocumentsTest`):

| Document field | Source |
|---|---|
| `doc_id` | UUIDv5 (`UUID.nameUUIDFromBytes`) over `"courtlistener\|<opinion_id>"` |
| `search_metadata.title` | `case_name` |
| `search_metadata.body` | `plain_text` |
| `search_metadata.document_type` | `"court-opinion"` |
| `search_metadata.source_uri` | `https://www.courtlistener.com/opinion/<cluster_id>/<slug>/` (slug: lowercased `case_name`, non-alphanumerics → dashes) |
| `search_metadata.author` | `author`, falling back to `judges` |
| `search_metadata.language` | `"en"` |
| `search_metadata.creation_date` | `date_filed` (LocalDate → Timestamp, UTC midnight) |
| `search_metadata.metadata` | `precedential_status`, `opinion_type`, `docket_id`, `nature_of_suit` (blanks skipped) |
| `ownership` | account `samples`, datasource `courtlistener` |

## Embeddings

Each document is embedded as `title + "\n" + first 2000 chars of body` with the
Model2Vec provider (`:protomolt-search-embedding-model2vec`) and attached as one
`SemanticProcessingResult` (`result_id` `court-sample-doc-embedding`) with a single
`SemanticChunk` carrying a `ChunkEmbedding`.

Model resolution mirrors the provider module's own tests: when neither
`protomolt.embeddings.model2vec.path` (also `-Pmodel2vec=`) nor
`PROTOMOLT_MODEL2VEC_PATH` names a real Model2Vec directory, the sample writes a
small deterministic WordPiece model itself (`CourtSampleModel`, same four-file
`safetensors` layout the provider tests use) into `<out>/model2vec`. Its vectors
are seeded random unit vectors over a stopword + legal-term vocabulary, so the
demo runs offline and reproducibly; nearest neighbours reflect shared vocabulary,
not semantics. Point `-Pmodel2vec=` at a real Model2Vec release for meaningful
similarity.

## Indexing

Indexing **hints** come from a catalog (`CourtDocumentIndexSample.documentCatalog()`):
`doc_id` KEYWORD; `title`/`body` TEXT; `language`/`source_uri`/`document_type`/`author`
KEYWORD; everything else skipped (`search_metadata` carries a TEXT hint so the mapping
factory expands it into dotted paths). The document-level vector lives under the
repeated `semantic_results`, which catalog paths cannot index into yet, so the sample
attaches the `KnnFloatVectorField` explicitly — the same vector the NDJSON projection
carries.

After indexing, the demo runs two queries and prints the top 5 `doc_id` + title
for each: a kNN query (nearest neighbours of the first document's vector — rank 1
is the document itself at score 1.0) and a text query (`body` contains "habeas",
present in 2 of the 25 bundled opinions).
