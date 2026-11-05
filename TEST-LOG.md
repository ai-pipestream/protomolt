# Test Log — protobuf unit-test coverage sweep

Branch: `test/protobuf-coverage-sweep` (merged from `test/protobuf-helpers-coverage`,
`test/protobuf-validation-coverage`, `test/protobuf-codecs-coverage`, all based on
`origin/main`). Tests only — no production code changed, no functionality changed.
No genuine error-handling bugs were found; two red tests during development encoded
wrong assumptions in the new tests and were corrected to the documented contract
(details per area below).

## Area A — core/helpers (`:protomolt-helpers`)

Branch commits: `182e8b832` (PayloadCodec edge tests), `2267b3c99` (MessageDiff edge
tests), `28ce9e75c` (validator/conflict/TypeConverter/AnyHandler edge tests + log).
Suite: `./gradlew :protomolt-helpers:test` — BUILD SUCCESSFUL (124 tests, 0 failures).

- `PayloadCodecEdgeCasesTest` (6): empty-Struct round-trip; unpack-unknown throws
  `InvalidProtocolBufferException`; unpackSafe null on empty Any; unpackToStruct shape;
  toStruct-empty; accessors non-null.
- `MessageDiffEdgeCasesTest` (7): both-empty, one-sided field, repeated-order sensitivity,
  map-order insensitivity, map-value change, mismatched-type throws, Struct equality
  (runtime-built `edgecase.Person` descriptor).
- `HelpersEdgeCasesTest` (5): validator null/empty/garbage semantics; real-descriptor
  base64 round-trip; conflict-detector empty map; conflicting-FQN throws
  (`ProtoSchemaValidationException`; identical FQNs are benign duplicates per contract);
  TypeConverter boundaries/invalid inputs; AnyHandler type-name/`is`/unpackSafe.
- Fixes: none. Three first-run failures were wrong test expectations (null-proto no-op,
  empty-bytes no-op, identical-FQN duplicates, generic Struct view) — tests corrected.

## Area B — protobuf validation + descriptors

Branch commits: `011a59a17` (both test files) + `05e2a7877` (log).
Suites: `:protomolt-protobuf-validation:test` (261), `:protomolt-protobuf-metadata:test`
(20), `:protomolt-protobuf-validation-protovalidate:test` (40) — 0 failures/errors.

- `validate/CelErrorPathsTest.java` (6): unknown CEL function → `RuleCompilationException`
  (field set and unset); `isNan()` receiver type mismatch on a string field → observed
  red as compilation error, revealing overload resolution is dynamic — corrected to the
  real contract `RuleEvaluationException` with rule id preserved; malformed `format()`
  template and trailing-`%` template → `RuleEvaluationException`; `isInf(0)`
  either-infinity branch (previously untested).
- `meta/MaterializeJsonNamesTest.java` (4): restores annotated JSON name when missing;
  keeps existing; ignores fields without the option and empty annotated names
  (function had zero direct coverage).
- Fixes: none — the one red test encoded a wrong assumption; test corrected.

## Area C — wire codecs + search index mappers

Branch commits: `6e0f2d2d9` (wire), `b13eae63a` (ndjson), `9b0d35b61` (mapper guards),
`dd3538b9f` (log). Pure unit tests only (no testcontainers/live-engine tests).
Suite: `:protomolt-kafka-wire`, `:protomolt-search-index-ndjson`,
`:protomolt-search-index-solr`, `:protomolt-search-index-opensearch`,
`:protomolt-search-index-lucene` — BUILD SUCCESSFUL, 288 tests total, 0 failures.

- `ConfluentWireFormatTruncationTest` (6): negative-count rejection on `payloadOffset`;
  truncated index arrays via `messageIndex`/`payloadOffset`/`payload` → `Malformed varint`;
  schema-id 0 round trip; empty-array rejection.
- `ProtoNdjsonWriterEdgeCasesTest` (10): `bulkCreate` non-object source rejection;
  blank-id omission; null guards (out/index/document/id/options/message); registry
  presence; `includingDefaultValueFields` output; empty `writeLines`; `Writer` overload.
- `SolrDocumentMapperGuardsTest`, `OpenSearchDocumentMapperGuardsTest`,
  `ProtoLuceneMapperGuardsTest` (3 each): `FieldProjection` null components and null
  `IndexMapping` fail fast naming the parameter.
- Fixes: none — one candidate (unreachable CEL parse branches in `SerdeMapper`) was
  deliberately left untested rather than enshrined.

## Totals

53 new tests across 9 new test files, all passing. Zero production files touched.
