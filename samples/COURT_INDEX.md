# Court PipeDoc → NDJSON → Lucene

End-to-end path using real `opensearch-sink-input-court` fixtures
(`PipeDoc` with search metadata + document-level embedding vectors).

## Layout (worktree)

```text
/work/worktrees/proto-nd-json-worktree/
  pipestream-proto-tools/          # this repo (branch feature/proto-ndjson-lucene)
  opensearch-sink-input-court/     # symlink → 1000 gzipped PipeDoc fixtures
  pipestream-protos/               # symlink → PipeDoc schema source
```

## Run

```bash
./gradlew :samples:runCourtPipeDocToLucene
# args: <fixturesDir> <outDir> <limit>
./gradlew :samples:runCourtPipeDocToLucene --args='\
  /work/worktrees/proto-nd-json-worktree/opensearch-sink-input-court/src/main/resources/fixtures/court/opensearch-sink \
  /tmp/court-out \
  1000'
```

Outputs:
- `pipedocs.ndjson` — projected docs (`doc_id`, `title`, `body`, `embedding`, …)
- `lucene/` — Lucene index with text fields + `KnnFloatVectorField` (`embedding`, COSINE / HNSW)

## Notes

- Indexing **hints** for PipeDoc today come from a **catalog** (we do not fork
  `pipestream-protos` to attach FieldOptions yet).
- Document vectors live under repeated `semantic_results`; the sample attaches
  the first document-level embedding explicitly until path indexing supports
  repeated/indexed segments.
- Shared HNSW knobs (M, efConstruction) across Lucene/Solr/OpenSearch are the
  next metadata layer on `INDEX_FIELD_TYPE_VECTOR` — not wired yet.
