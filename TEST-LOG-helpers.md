# Test Log — protobuf helpers coverage (area A: core/helpers)

Branch: `test/protobuf-helpers-coverage`
Module: `:protomolt-helpers` (`core/helpers`)
Command: `./gradlew :protomolt-helpers:test` — BUILD SUCCESSFUL (124 tests, 0 failures).

## Tests added

### PayloadCodecEdgeCasesTest (new, 6 tests)
- `emptyStructRoundTrip` — empty Struct pack/unpack/unpackSafe round-trip.
- `unpackUnknownTypeThrows` — `unpack` on unknown type throws `InvalidProtocolBufferException`.
- `unpackSafeReturnsNullForEmptyAny` — `unpackSafe` on default Any returns null.
- `unpackToStructRoundTrip` — `unpackToStruct` on packed Struct returns non-null Struct with `fields` entry.
- `toStructOfEmptyMessageIsEmpty` — `toStruct` of default Struct is empty.
- `accessorsAreNonNull` — `anyHandler()` / `typeConverter()` non-null.

### MessageDiffEdgeCasesTest (new, 7 tests)
- `bothEmptyProduceEmptyDiff`, `oneSidedFieldIsReported` (left value + null right),
  `repeatedOrderMatters`, `mapOrderIsIrrelevant`, `mapValueChangeIsReported`,
  `mismatchedTypesThrow` (Struct vs Person), `structWithSameContentHasNoDiff`.
- Uses a runtime-built `edgecase.Person` descriptor (scalar + repeated + map fields).

### HelpersEdgeCasesTest (new, 5 tests)
- `validatorRejectsNullAndEmptyAndGarbage` — null proto is a documented no-op;
  empty bytes parse to default proto (no-op); garbage bytes throw; null bytes throw NPE;
  non-base64/empty strings return null from `tryParseBase64Descriptor`.
- `validatorAcceptsRealDescriptorProto` — base64 round-trip + `validate`/`validateBytes` accept.
- `conflictDetectorEmptyMapPasses` — empty sources pass both entry points.
- `conflictDetectorDetectsSameMessageInTwoFiles` — conflicting `dup.Dup` definitions throw
  `ProtoSchemaValidationException` (identical definitions are benign duplicates per contract).
- `typeConverterBoundaryAndInvalid` — `Long.MAX_VALUE` toValue, null-Value fromValue → null,
  invalid `convertToFieldType` inputs throw, empty Struct round-trip.
- `anyHandlerTypeNameAndMismatch` — type name/URL, `is(...)` positive/negative, `unpackSafe` null.

## Production fixes
None — no genuine error-handling bugs found. Three initial test failures were wrong
expectations in the new tests (corrected to match documented contract):
- `validate(source, null)` is a documented no-op; `validateBytes` of empty bytes parses
  to the default proto (no-op); identical FQNs across sources are benign duplicates;
  `unpackToStruct` returns a generic Struct view, not the original Struct.

## Commits
- (see `git log origin/main..test/protobuf-helpers-coverage --oneline`)
