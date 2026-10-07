# Historical activation confirmation and scoped recovery checks

Base `ec94c916d830cff6134e3ebf4109bf13c005f231`, plus the source fingerprints
retained here, on `refactor/repository-composition`.

`RepositoryHistoricalActivationEvidence` reads the exact immutable activation
identity without constructing a source handle, execution session or local drain
capability. It checks the V94/V109 transaction association, epoch/token/incarnation,
command, successor preparation, original retained preparation and capture digest.
An absent activation returns empty; an ordinary V94 activation without the
historical sidecar is refused. Same-attempt lost-ack confirmation reuses this check.

Actual PostgreSQL 18 cases cover:

- A separate entity-manager factory confirms the activation before and after
  capture completion with unchanged leases and batch counts. Historical session
  attachment remains refused. This is same-JVM connection independence, not a
  deployment restart test.
- Changed retained preparation, wrong principal and non-process callers refuse
  confirmation. A bare ordinary activation does not count as historical evidence.
- A lost commit reply leaves exact evidence readable independently of the old
  activation object and its local cleanup capability.
- Real key and creation-grant provisioning precedes the historical claim. After
  capturing sources, revoking the creation grant or source READ returns NOT_FOUND;
  revoking the key returns UNAUTHENTICATED. No V94 execution, V109 sidecar, epoch-2
  binding or extra capture survives refusal. The positive case commits normally.
- Revocation after commit permits only immutable confirmation; a fresh historical
  read is denied. The coordinating process's privileges do not replace the scoped
  execution caller's authority.
- Epoch 2 uses one-second claim and owner leases. Actual database expiry permits
  V97 reservation and V93 installation of a distinct epoch-3 incarnation. Its
  V109 ancestry traverses two preparation edges back to the original retention
  set. Draining epoch 3, then epoch 2, cannot replace epoch 1's unfinished capture.
  Actual original pin release and reader quiescence permit the final drain receipt;
  all three captures then qualify without current-lease changes.

Source document publication uses explicitly synthetic provider observations.
These tests qualify SQL authorization, immutable identity and reader lifetime,
not provider publication or a restored public historical execution session.
Concurrent fresh activation, ancestry-limit failure, deployed restart/attachment,
terminal root release and public historical execution remain unfinished.

Final command: exit 0 in 51 seconds; 56 tests, zero failures/errors/skips.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalActivationEvidenceIT' \
  --tests '*ScopedHistoricalSuccessorActivationIT' \
  --tests '*RepositoryHistoricalSuccessorActivationIT' \
  --tests '*DocumentCaptureAdmissionClosureIT' \
  --tests '*DocumentPreparationCaptureDrainIT' \
  --max-workers=2 --console=plain
```

`focused-results.tar.gz` retains the five JUnit XML reports. Earlier runs exposed
ambiguous static test imports, then a fixture mismatch: creation grants require
an absent destination, whereas the reused fixture targeted an existing document.
The fixture now explicitly creates the new destination before grant provisioning.
No failing case was skipped. Sol reviewed the implementation, expected denial
codes, scoped setup and three-epoch sequence with no blocker.
