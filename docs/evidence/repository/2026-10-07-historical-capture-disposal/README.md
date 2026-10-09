# Historical capture disposal qualification

Base: `5f2a0a42c2d5b869457001d54cbd45b8484d7af9`. `source.sha256` binds the changed
production and test sources. No protobuf contracts or public entrypoints changed.

`RepositoryHistoricalSuccessorActivation.disposeCapture` permanently closes this
local activation attempt. Under the activation monitor, its classifier takes the
exact command claim lock at READ COMMITTED and distinguishes consistently absent
capture state from matching V94/V109/V104/V105 records. It verifies the persisted
pin tuple digest, sealed count, creation transaction and capture owner. Partial,
mismatched or unavailable evidence fails without releasing the local resources.

Both the Java monitor and SQL lock are released before waiting for actual Work/Uses.
Committed captures use existing V107 LOCAL evidence after read-pin release. An
absent activation releases only its local resources and writes no V107 record.
NO_CAPTURE leaves preactivation resources with the caller. This is local disposal,
not preparation-root release, reader-wide fencing or remote quiescence.

The focused real PostgreSQL command was:

```
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalCaptureDisposalIT' \
  --tests '*RepositoryHistoricalSuccessorActivationIT' \
  --tests '*DocumentPreparationCaptureDrainIT' --max-workers=2 --console=plain
```

It passed 47 tests with zero failures/errors/skips: 9 new disposal tests, 12 existing
successor activation tests, and 26 capture-drain tests. The new tests cover real
rollback/lost acknowledgment, held Work, exact repeat disposal, failed confirmation,
no tentative capture, process authority, later claim takeover without lease renewal,
owner/pin corruption, and classification waiting on the actual PostgreSQL claim lock
before deciding commit versus rollback. Source publication in these focused fixtures
uses synthetic provider observations; these tests make no object-provider durability claim.

The concurrent observer has a separate 64 MiB host budget. An earlier fixture shared
that budget with the held activation and correctly failed capacity admission before
reaching SQL; the fix changes fixture ownership, not production capacity. PostgreSQL
blocking evidence, rather than a timing delay alone, establishes the race ordering.

Sol reviewed the classifier, lifetime split and idempotent disposal retries. The
future managed owner must still drain its accepted calls before disposing historical
state. This private primitive does not implement the owner, expose historical
publication, or qualify every recovery/pruning scenario in the larger goal.

The full packaged regression command
`./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain`
passed in 8m32s with one aggregate test and zero failures/errors/skips. It uses real
PostgreSQL and LocalStack and includes restart, forced-crash recovery and final
lease-expiry cleanup. `host.log.gz` is the complete initial host log saved before
successful temporary-directory cleanup; the full Gradle/JUnit results establish
completion of the later phases. This is regression coverage of the shared drain
helper; the focused SQL suites above exercise the new disposal API directly.

Local test success does not establish hosted CI or a merge into main.
