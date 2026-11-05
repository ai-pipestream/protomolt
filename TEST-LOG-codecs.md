# TEST-LOG-codecs — area C: wire codecs + search index mappers

Branch: `test/protobuf-codecs-coverage` (worktree: `/work/worktrees/protomolt/test-proto-codecs`,
base: `origin/main`).

Scope: pure unit tests only (no testcontainers / live-engine tests). No production code changed.
Recon found the serde (`SerdeMapper`, `Subjects`, config) and all three document mappers already
densely covered (700–1400-line test classes); new tests target the verified-uncovered edges below.

## Tests added

### `sink/kafka/wire` — `ConfluentWireFormatTruncationTest` (6 tests)
- `payloadOffsetRejectsANegativeIndexCount` — count byte `0x01` (zigzag −1) rejected on the offset path
- `messageIndexRejectsATruncatedIndexArray` — count=2 with one index present → `Malformed varint`
- `payloadOffsetRejectsATruncatedIndexArray` — same shape via `payloadOffset`
- `payloadRejectsATruncatedIndexArray` — same shape via `payload`
- `carriesTheZeroSchemaId` — schema id 0 round trip
- `rejectsAnEmptyByteArray` — zero-length input rejected as not-a-frame

### `search/index/ndjson` — `ProtoNdjsonWriterEdgeCasesTest` (10 tests)
- `bulkCreateRejectsNonObjectSource` — `Timestamp` source rejected, stream untouched
- `blankIdsAreOmittedLikeMissingIds` — blank `_id` omitted from action line
- `bulkDeleteRejectsANullIdWithoutWriting` — NPE, stream untouched
- `bulkWritesRejectNullsWithoutWriting` — null out/index/document guards
- `toJsonLineRejectsNull`, `constructorRejectsNullOptions`
- `descriptorRegistryIsEmptyByDefaultAndPresentWhenSupplied`
- `includingDefaultValueFieldsPrintsUnsetScalars` — `page_count:0`/`display_name:""` vs `{}` default
- `writeLinesWithNoMessagesWritesNothing`, `writerOverloadWritesOneLine`

### Mapper guard rails (3 tests each, 9 total)
- `SolrDocumentMapperGuardsTest`, `OpenSearchDocumentMapperGuardsTest`,
  `ProtoLuceneMapperGuardsTest`: `FieldProjection` null path/field-name → NPE naming the parameter;
  `map(message, (IndexMapping) null)` → NPE naming `mapping`.

## Fixes
None — no genuine error-handling bug found; no production code touched (all green on first run).

## Verification
`./gradlew :protomolt-kafka-wire:test :protomolt-search-index-ndjson:test \
:protomolt-search-index-solr:test :protomolt-search-index-opensearch:test \
:protomolt-search-index-lucene:test --offline` → BUILD SUCCESSFUL.
288 tests total across the five modules, 0 failures, 0 errors, 0 skipped (25 new).

## Commits
- (see `git log test/protobuf-codecs-coverage` — small commits, one per area + log)
