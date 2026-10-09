# Private capture coverage qualification

Base `5a83a6a44e103d59553a2c5451ea0ce87fc51566`, branch
`refactor/repository-composition`, plus files in `source-sha256.txt`.

New private helper `DocumentPreparationCaptureCoverage` verifies a historical
preparation's capture evidence in one caller-owned transaction. It locks the
claim, preparation, root header and digest-ordered batches, without renewing the
claim. It requires READ COMMITTED, actual canonical roots, exact selected revision
slots/objects, one same-creation initial capture, each batch's actual count/digest,
matching original owner/drain identities, and absence of exact pins/mirrors.
It reads at most 17 batch headers (rejecting more than 16) and 10,001 children per
batch (rejecting more than 10,000), processing batches separately.

Final command: exit 0, 35 seconds, 36 tests, no failures/errors/skips.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentPreparationCaptureDrainIT' \
  --tests '*DocumentPreparationHistoryRootsIT' \
  --tests '*DocumentHistoricalMultiRevisionPublicationIT' \
  --tests '*DocumentCaptureAdmissionClosureIT' \
  --max-workers=2 --console=plain
```

Eight new cases in the drain suite cover incomplete drain refusal, successful
completion without root deletion/claim renewal, and corrupt or missing pin,
publication, owner, drain, initial flag and batch evidence. Mutations deliberately
bypass only named guards in an isolated PostgreSQL transaction and roll back DDL
and data together; afterward the original evidence qualifies again. Existing
expiry/guarded SQL epoch-transfer cases now also qualify their original capture.
REPEATABLE READ is explicitly refused. Historical seed provider observations are
synthetic; database and lifecycle behavior are real. The other suites are regression
coverage; they do not constitute multi-revision tests of this new helper.

Sol reviewed the helper and requested refusal of duplicate source/revision/object
tuples, now implemented. Before release wiring, still qualify cross-revision reuse
of one physical object, repeated selectors, internally consistent incomplete or
duplicate batches, several captures with an earlier undrained owner, and historical
legacy sets missing their initial capture. Missing evidence already refuses in
the helper, but those full acceptance scenarios remain unproven.

This is new private functionality, not a fix to a preexisting public API, so there
is no pre-implementation behavioral red test. It is not called by a release API
and returns only a count for the current transaction. No terminality, root-release,
public execution, full storage or performance claim is made. JUnit XML trailing
whitespace is normalized in retained evidence.
