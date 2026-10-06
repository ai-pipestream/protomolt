# Historical assessment provenance migration

V86 adds an immutable `source_node` and distinct `HISTORICAL_REUSE` declaration
to assessment slots. The historical guard requires an exact sealed native revision,
matching successful commit transaction, full ordinal/object/physical slot,
same-account available source, live physical retention and DOCUMENT_HISTORY
reference. It does not require that revision to be current. Existing REUSE keeps
its current-pointer and DOCUMENT_CURRENT checks.

Real PostgreSQL tests cover staging r1 after two later publications; refusal for
wrong node, revision, full ordinal, object, missing node, oversized ordinal and
non-native legacy history; and refusal when the same old source is declared current
REUSE. Failed transactions leave no assessment owner or reference. A V85-to-V86
upgrade fixture preserves all existing NEW_CONTENT/REUSE slot fields and references,
then stages another current-reuse assessment under the new function.

Command used:

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentHistoricalSelectionIT' --tests '*DocumentAssessment*IT' --console=plain
```

Result: 96 tests, zero failures/errors/skips, 46 seconds. Local log:
`/tmp/protomolt-historical-staging-qualified.log`. Sol reviewed the migration and
tests with no material blocker. `git diff --check` passed.

The fixtures use real SQL invariants with synthetic provider observations and
direct staging declarations. They do not prove historical command-to-slot equality,
provider bytes, or restore execution. The SQL guard does not parse the protobuf
command. Execution remains gated until Java binding, versioned replay, current
authorization, schema retention and atomic publication are integrated. Add isolated
cross-account, missing-history-reference and retiring-object negatives before that
activation. No hosted CI, merge or deployment is claimed.
