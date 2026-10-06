# Composite schema resolution

The admission library can now combine exact retained sources with ordinary resolver
selections for one member. Routes are disjoint and cover every historical ordinal.
All sources, including an ordinary container when needed, must agree on the complete
container definition. The resolver never substitutes a current definition for a
historical occurrence and never converts a validation failure into opaque mode.

Each retained scope checks its selected target ordinals, including rootless parts,
and compares exact evidence, references and artifact bytes. Strict single-source
checking still requires equality with the complete assessment. The composite checks
the global union and binds each ordinary occurrence to the definition it returned.
The host retains source scopes and bytes; only metadata scratch is owned here.

Sol identified a bypass where an ordinary occurrence could use assets already in
the retained union without going through the composite resolver. The regression
`sharedAssetsCannotHideBypassedOrdinaryOccurrence` failed before the fix because no
exception was raised. Completion now decodes canonical ordinary evidence with byte
reservations and checks exact occurrence-to-definition bindings. A second regression
swaps two definitions with the same type URL while preserving the artifact union;
the swapped assignment is refused. Sol reviewed the correction with no blocking issue.

Other cases cover accepted and rejected semantic verdicts across two retained scopes,
mixed ordinary/historical parts, unused source scopes, a rootless ordinal gaining an
Any root, missing and overlapping routes, an incompatible container, cancellation,
asset limits, closure and reservation cleanup. Ordinary and historical regression
suites remain green.

```sh
./gradlew :protomolt-repo-admission:test \
 --tests '*DocumentSchemaAssessmentReplayTest' --tests '*DocumentSchemaPreparationTest' \
 :protomolt-repo-container:test --tests '*DocumentHistoricalRestoreAssessmentIT' \
 --tests '*DocumentHistoricalMaterializationIT' --tests '*DocumentPublicationAssessmentTest' --console=plain
```

Result: 94 tests, zero failures/errors/skips, 25 seconds. Local logs:
`/tmp/protomolt-composite-bypass-red.log` and `/tmp/protomolt-composite-qualified.log`.
`git diff --check` passed. Admission tests execute real descriptor validation with
synthetic document fixtures. Container cases use real PostgreSQL; their provider
observations are explicitly synthetic, not a provider qualification.

Whole-command host integration, current authorization/policy fencing, observed-runtime
CREATE and atomic reference publication remain unfinished. No public restoration,
decoded-heap bound, performance result or deployment is established here.
