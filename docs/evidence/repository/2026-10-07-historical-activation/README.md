# Private historical successor activation

Base `2906175faaea25ad28645523299fea7762117fa2`, plus the sources fingerprinted
in `source-sha256.txt`, on `refactor/repository-composition`.

V109 retains an immutable association between the successor execution, original
historical preparation and exact new reader capture. Its trigger verifies up to
64 immutable V93 ancestry links and exact same-transaction capture ownership. A
deferred constraint rejects a required historical binding absent at commit.
The private activation helper checks current caller, source and placement state,
and retains its tentative capture before commit so a lost reply does not discard
cleanup identity. No protobuf contract changes.

The new tests use PostgreSQL 18 and actual registration, expiration, successor
reservation, installation, activation, pin cleanup and capture-drain transactions:

- Epoch 2 drains while epoch 1 retains its original reader. Coverage refuses until
  epoch 1 actually closes, releases and quiesces. Both batch receipts then qualify,
  with claim and owner leases unchanged throughout activation and drain.
- Omitting the required sidecar fails at commit and rolls back execution/binding.
- A sidecar insertion fault, wrong retention digest, wrong capture digest and
  wrong claim token each roll back all new state. Actual production guards remain
  active; an injected BEFORE trigger alters only the attempted insert. Removing
  the injected trigger permits a successful retry.
- A fault after the actual database commit leaves one sidecar and the exact
  tentative capture available. Retry confirms it without adding a batch or lease.
- Cancellation before commit rolls back all new state; cancellation after commit
  remains cancellation to the caller but preserves the committed result for retry.

The source publication fixture explicitly uses synthetic provider observations.
This is SQL, authorization-boundary and local reader-lifetime evidence, not object
store or provider publication qualification. Scoped revocation, concurrent fresh
attempts, cold attachment, multi-hop ancestry and the traversal limit remain open.
Historical execution, publication and root release remain gated. Future historical
use must require V109 evidence; a bare legacy V94 row is insufficient.

Final command: exit 0 in 1 minute 48 seconds; 71 tests, zero failures/errors/skips.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalSuccessorActivationIT' \
  --tests '*RepositorySuccessorActivationIT' \
  --tests '*RepositorySuccessorInstallIT' \
  --tests '*DocumentCaptureAdmissionClosureIT' \
  --tests '*DocumentPreparationCaptureDrainIT' \
  --max-workers=2 --console=plain
```

`focused-results.tar.gz` contains the five JUnit XML reports. The first broader
run exposed two legacy migration fixtures invoking a post-V103 writer against
V95/V96. Those now use the existing, explicitly version-bounded legacy fixture.
All ordinary activation and installation regressions pass, including migrations.

Sol reviewed the implementation and final failure cases with no blocker. The
review emphasized that V94's historical-required flag defaults false for legacy
rows: SQL does not parse historical selectors from protobuf. The private helper
requests the flag; future historical attachment/release must independently require
its exact sidecar. No deployment, main merge or public historical API is claimed.
