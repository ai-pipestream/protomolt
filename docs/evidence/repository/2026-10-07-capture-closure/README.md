# Capture admission closure qualification

Base `4a5916c370952a2438e1451b66a6072d4a3566b8`, branch
`refactor/repository-composition`, plus the files identified in `source-sha256.txt`.

`red/` records a real fresh capture accepted after committed abandonment. The
single regression failed because the expected exception was not raised.
V108 adds AFTER INSERT checks to the batch and capture-owner tables. Existing
claim fences serialize those checks with abandonment and terminal writers.

`initial/` records the first broader run: the four new cases passed, but two old
V84 migration fixtures called a current writer requiring V103 tables. The fixture
now inserts the original V81 columns under the actual claim fence, with a schema
version guard. No production compatibility fallback was added.

`focused/` records exit 0, 51 seconds, 38 tests, no failures/errors/skips:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentCaptureAdmissionClosureIT' \
  --tests '*DocumentPublicationAbandonmentIT' \
  --tests '*DocumentPreparationCaptureDrainIT' \
  --tests '*DocumentHistoricalMultiRevisionPublicationIT' \
  --max-workers=2 --console=plain
```

The four closure cases cover open-capture positive controls, closure with existing
captures migrated from V107, exact batch confirmation after abandonment, refusal
of an owner inserted after a real abandonment marker within the same transaction,
and a capture observed waiting on the exact abandonment transaction through
`pg_blocking_pids`. The interposed marker and failed capture roll back together.
Tests use real PostgreSQL; source provider observations in the seed fixture are
synthetic, not storage-performance evidence.

Sol reviewed the migration, transaction ordering and fixture correction without a
blocker. Separate terminal success/rejection capture cases remain outstanding.
This run does not qualify root release, enable public historical execution, or
repeat the full storage suite. JUnit XML trailing whitespace is normalized in
the retained evidence; test results and failure contents are otherwise preserved.
