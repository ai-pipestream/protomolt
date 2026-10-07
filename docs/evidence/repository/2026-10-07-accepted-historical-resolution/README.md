# Accepted historical schema resolution

Baseline: `d36acabdb74df0a89c745412a536f6867d14cdbd`. Final tested Java files
are fingerprinted in `source-sha256.txt`; this evidence is committed with them.

An accepted historical Work now starts schema resolution through a child lifetime
permit. External admission still closes immediately. Forking atomically refuses
an ended parent, but permits a continuation of an active parent after admission
closes. Closing the parent does not release source Uses while its resolver child
remains active. Resolver cleanup closes composite/loaders before the child permit.
Construction failures release the child and preserve the primary error if cleanup
also fails. Historical assessment preparation uses its accepted Work consistently
for references, opaque checks, resolution and final authorization.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentHistoricalSourceLifetimeTest' \
  --tests '*DocumentHistoricalSourceDrainIT' \
  --tests '*DocumentHistoricalRestoreAssessmentIT' \
  --tests '*DocumentHistoricalPublicationIT' \
  --max-workers=2 --console=plain
```

Both runs executed 58 tests: six lifetime, five drain, 42 restore assessment and
five publication; zero failures, errors or skips. The initial run took 53 seconds.
`initial/` predates the final cleanup exception-preservation adjustment. `final/`
contains the complete rerun after that change. Neither run is reused evidence
from another source state.

New checks cover a child outliving its parent, refusal to fork an ended parent,
100 concurrent parent-close/fork races, and real PostgreSQL 18 retained descriptor
resolution starting after admission closes. A held resolver remains usable after
the parent closes, then source Uses and byte reservations drain on resolver exit.
A foreign-member construction failure releases the child without closing the
parent. Existing cleanup-failure and cancelled-worker tests remain enabled.

Initial source provider observations in the SQL fixtures are synthetic; the
descriptor reads and retention/authorization transactions are real. The held
worker retains an actual resolver, not an in-flight object-store request. This
does not qualify claimed historical CREATE, publication, restart or transport.
Those gates remain closed. A future full claimed-assessment test must also hold
validation work through close and verify failure after resolver construction.

Sol reviewed the six-file implementation and tests with no blocker. Local
validation does not establish hosted CI, merge or deployment.
