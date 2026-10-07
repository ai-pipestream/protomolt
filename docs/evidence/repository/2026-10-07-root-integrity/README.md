# Actual history-root integrity

Base `23d69f0b575998e17486c9d35aee925a21f923f9`, branch
`refactor/repository-composition`, plus tested files in `source-sha256.txt`.

The old coverage query validated the V103 header against the command but did not
inspect its child rows. `red/` records two failing regression cases: after deleting
a child root, the query still returned EXACT instead of refusing damaged data.
`initial/compile.log` is an earlier ambiguous Java lambda overload, not the red
behavioral baseline; the test callback now explicitly selects the Consumer overload.

The production query now reads the header and an aggregate over its exact child
scope in one SQL snapshot. Both stored and actual count/digest must agree with the
canonical command. The query adds no locks. Future release still needs its own
claim, preparation, header and capture locks and all ownership/drain proofs.

Final command: exit 0, 34 seconds, 28 tests, zero failures/errors/skips.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentHistoricalMultiRevisionPublicationIT' \
  --tests '*DocumentPreparationHistoryRootsIT' \
  --tests '*DocumentCaptureAdmissionClosureIT' \
  --tests '*DocumentPreparationCaptureDrainIT' \
  --max-workers=2 --console=plain
```

The multi-revision tests inject missing roots, an extra real revision, and a
same-count substitution with a real revision. They disable only the root mutation
guard in an isolated PostgreSQL transaction, then deliberately roll back DDL and
data together and verify original coverage. Foreign keys remain active. This is
administrative corruption injection, not a production bypass. Existing tests
verify sealed empty sets, absent legacy headers, migration and canonical mismatch.
Historical seed provider observations are synthetic; PostgreSQL operations are real.

Sol reviewed the query and corruption fixtures without a blocker. This checkpoint
does not enable root release or public historical execution and does not claim a
new performance benchmark or full storage qualification. Retained JUnit XML has
trailing whitespace normalized.
