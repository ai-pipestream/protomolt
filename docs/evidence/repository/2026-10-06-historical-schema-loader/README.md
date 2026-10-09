# Historical schema loader ownership

The package-private loader owns retained SQL bytes and schema resolution separately
from a validation verdict. It borrows an exact live source Use, validates selected
physical entries and ordinal mappings, and loads SQL assets using the caller bound
to the historical capture. It accepts no independent caller or transaction source.
Resolver cleanup precedes SQL reservation cleanup. The caller retains its own Use.

Tests cover foreign entries, mismatched mappings, a different capture of the same
revision, closing the borrowed Use, cancellation after SQL reservation and exhausted
capacity. Refusals release reservations; loader close leaves a live borrowed Use
available. Existing PostgreSQL restore, materialization, selection and delivery
authorization tests remain green, alongside whole-command assessment regressions.

```sh
./gradlew :protomolt-repo-container:test \
 --tests '*DocumentHistoricalRestoreAssessmentIT' \
 --tests '*DocumentHistoricalMaterializationIT' \
 --tests '*DocumentHistoricalSelectionIT' \
 --tests '*DocumentHistoricalDeliveryAuthorizationIT' \
 --tests '*DocumentPublicationAssessmentTest' --console=plain
```

Result: 69 tests, zero failures/errors/skips, 46 seconds. Local log:
`/tmp/protomolt-historical-loader-bound.log`. `git diff --check` passed.

The existing historical fixture uses real PostgreSQL and retained descriptors but
synthetic provider observations. This checkpoint provides no provider performance
or public historical execution qualification. Whole-command assessment, actual
runtime observation, CREATE and atomic publication remain separate work.
