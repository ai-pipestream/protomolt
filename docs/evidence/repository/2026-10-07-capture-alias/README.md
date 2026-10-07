# Cross-revision capture alias qualification

Base `69a9e92341e4cea614749f48254a1388dd711938`, branch
`refactor/repository-composition`, plus the tested source in `source-sha256.txt`.

The new case publishes revision 2 through the existing unclaimed historical
assessment/admission/publication path, reusing revision 1's object. It asserts
the two historical selectors name the same physical object. A separate private
preparation selects both revisions into distinct destinations and records two
real pins: one object ID, two revision IDs. Actual source closure and capture
completion produce the drain receipt; capture coverage then qualifies the batch.

This proves canonical `(node,revision,object)` identity is not collapsed to object
ID. No SQL guard is bypassed and no terminal/drain row is fabricated. The historical
seed uses explicitly synthetic provider observations; revision publication, schema
retention, registration, pin cleanup and drain checks run against real PostgreSQL.
It does not enable claimed historical execution or qualify real provider effects.

Final command: exit 0, 35 seconds, 38 tests, zero failures/errors/skips.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentCaptureAdmissionClosureIT' \
  --tests '*DocumentPreparationCaptureDrainIT' \
  --tests '*DocumentHistoricalMultiRevisionPublicationIT' \
  --max-workers=2 --console=plain
```

Sol reviewed the alias identity and lifecycle cleanup without a blocker. Multiple
execution-owner epochs, historical legacy initial-capture gaps and atomic release
remain separate acceptance requirements. No production code or wire contract is
changed. Retained JUnit XML trailing whitespace is normalized.
