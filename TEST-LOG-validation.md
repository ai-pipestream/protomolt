# TEST-LOG — protobuf validation (CEL) + descriptors (agent B)

Worktree: `/work/worktrees/protomolt/test-proto-validation`
Branch: `test/protobuf-validation-coverage`
Base: `origin/main` @ `44d61b9ba` (merge PR #687)

## Recon (read-only)

- `protobuf/validation/.../cel/ValidationCelFunctions.java` — CEL member functions
  (`isHostname`, `isEmail`, `isIp`, `isIpPrefix`, `isUri`, `isUriRef`, `isHostAndPort`,
  `isNan`, `isInf`); strings-extension `format()` covered by existing
  `cel/FormatFunctionTest`.
- `protobuf/validation/.../CelValues.java` — failure classification:
  compile error → `RuleCompilationException`, runtime error → `RuleEvaluationException`.
- `protobuf/validation/.../ValueChecks.java` — bytes-pattern non-UTF-8 already covered
  (`BytesRulesExtendedTest`); existing `RuleClassificationTest` covers bad syntax,
  static non-bool CEL, runtime div-by-zero, unknown oneof names, nesting limit.
- `protobuf/metadata/.../DescriptorMetadata.java` — `materializeJsonNames` had **no**
  direct test (`DescriptorMetadataTest` covers `asBag`/`field`/`message` only).

## Gaps found → tests added (tests only, no production change)

### 1. `protobuf/validation/.../validate/CelErrorPathsTest.java` (6 tests, new file)
- `unknownFunctionIsACompilationError`
- `unknownFunctionIsACompilationErrorEvenWhenTheFieldIsUnset`
- `receiverTypeMismatchIsARuntimeEvaluationError` — `this.isNan()` on a string field
  **compiles** (dynamic overload resolution) and fails at evaluation; first written
  expecting `RuleCompilationException` (red), corrected to the real contract
  `RuleEvaluationException` with `ruleId == "bad.recv"`.
- `malformedFormatTemplateIsARuntimeEvaluationError` — `'%d'.format(['abc'])`.
- `trailingPercentFormatTemplateIsARuntimeEvaluationError` — `'oops%'.format([this])`.
- `isInfWithZeroSignMatchesEitherInfinity` — `isInf(0)` true for ±∞, false for
  finite/NaN (previously untested branch of `ValidationCelFunctions.isInf`).

### 2. `protobuf/metadata/.../meta/MaterializeJsonNamesTest.java` (4 tests, new file)
- `restoresAnnotatedJsonNameWhenMissing`
- `keepsAnExistingJsonName`
- `ignoresFieldsWithoutTheMetadataOption`
- `ignoresAnEmptyAnnotatedJsonName`

## Fixes
None — no production bug found; the one red test encoded a wrong assumption and the
test was corrected (no production change).

## Verification
- `./gradlew :protomolt-protobuf-validation:test` — 261 tests, 0 failures.
- `./gradlew :protomolt-protobuf-metadata:test` — 20 tests, 0 failures.
- `./gradlew :protomolt-protobuf-validation-protovalidate:test` — 40 tests, 0 failures.

## Commits
- `011a59a17` test(validation): cover CEL error paths (unknown fn, type mismatch,
  bad format, isInf(0)) — CelErrorPathsTest (6) + MaterializeJsonNamesTest (4) + this log
