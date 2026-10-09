# Historical source authorization lockset

`DocumentAdmissionAuthorization.Prepared` now separates historical source addresses
from current-source revision conditions. A single deterministic lock acquisition
covers destinations, current sources and historical sources. Historical identity,
current READ and availability are checked before destination WRITE and before any
revision-conflict detail. Historical revision numbers are never compared to the
current head. Existing current-source and destination revision checks remain.

Preparation accepts exact pinned historical reference preparations, checks their
Uses are live, bounds aggregate selector count and encoded bytes, requires the same
account as the command, and compares the exact selector set with all historical
content arms in the plan. An empty default list cannot silently authorize a future
historical command. Repeated identical selections may share source authorization;
the eventual writer still must bind every destination member/ordinal separately.

Validation:

```
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentHistoricalAuthorizationIT' \
  --tests '*DocumentHistoricalSelectionIT' \
  --tests '*DocumentOperationUploadAdmissionIT' \
  --tests '*DocumentAtomicPublicationIT' --console=plain
```

All 172 tests passed with no failures, errors or skips in 31 seconds. The new boundary tests cover an advanced
historical source head, denied source or destination masking a stale destination,
authorized stale-destination refusal, extraneous pinned selectors, and closed Uses.
Positive authorization tests explicitly construct the internal lockset to exercise
its SQL boundary while the public command guard remains. They do not establish
acceptance of an executable historical command or a successful restore publication.
Fixtures use explicit synthetic provider observations, not object-store operations.
Sol found no material blocker in this staged change.

Receipt replay/rejection must also reauthorize historical sources before activation.
Full command-to-target correspondence, retained input capture, assessment retention,
policy fencing and native publication integration remain required. No hosted CI,
merge, deployment or public restore availability is claimed.
