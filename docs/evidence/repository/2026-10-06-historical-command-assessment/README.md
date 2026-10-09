# Historical command assessment

The internal assessment owner accepts complete pinned historical sources and
ordinary members in one command. It binds each capture to the exact caller and
account, checks all fragment hashes before schema resolution, and assesses every
member under one supplied policy and evaluation time. Historical-only members
retain their own container definitions. Mixed historical and ordinary parts must
agree on the complete container definition; only ordinary occurrences reach the
ordinary resolver.

The owner retains source Uses through inspection and replay. Closing an outer
history handle does not release those Uses. Inspection rechecks current READ and
returns immutable summaries plus the caller-supplied command and policy. Its facade
expires when the callback returns. No payload, borrowed schema view, publication
candidate or terminal decision escapes this API. Previously returned value
summaries remain values; this is not revocable memory.

Sol reviewed the owner and authorization changes with no correctness blocker.
The review identified an inaccurate summary-only description because inspection
also returns the immutable input command and policy; the API comment and remaining
work document now describe that explicitly. The API remains internal.

Tests cover multiple typed members, ordinary typed and opaque members, distinct
ordinary and historical container metadata, mixed historical CORE and uploaded
PARSED roots, missing/incompatible mixed containers, pin lifetime, mismatched
caller, duplicate/missing captures, capacity, cancellation, revoked source access,
reentrant closure, expired inspection and deterministic schema replay. All byte
reservations return to zero. The mixed-part test confirms exactly one ordinary
lookup and no registry lookup for the historical root.

```sh
./gradlew :protomolt-repo-container:test \
 --tests '*DocumentHistoricalRestoreAssessmentIT' \
 --tests '*DocumentPublicationAssessmentTest' \
 --tests '*DocumentHistoricalSelectionIT' --console=plain
```

Result: 81 tests, zero failures/errors/skips, 40 seconds. Log:
`/tmp/protomolt-historical-whole-assessment-qualified.log`.
The fixtures use real PostgreSQL and descriptor validation. Provider observations
are explicitly synthetic; this run establishes neither provider durability nor
performance.

Historical opaque-source classification, a SQL host case combining multiple
historical sources within one member, authoritative destination/current-policy
fences, observed-runtime CREATE and atomic reference publication remain open.
Public restore and automatic claim transfer remain disabled. This checkpoint
does not establish hosted CI, merge or deployment.

## Two-source SQL integration follow-up

`oneMemberCombinesTwoSqlHistoricalSourcesAndRechecksBoth` combines historical CORE
from one sealed revision and PARSED from another document at a distinct graph
address. Both use the same logical document ID so the combined fragments retain
their document identity. The second source has an unselected CORE root, exercising
selected-root checking rather than accidentally validating a whole second source.
Captures arrive in reverse order, and the ordinary resolver throws if invoked.
Both source Uses remain live after the outer handles close. Replay succeeds with
current READ, fails when the second source loses READ, and releases all reservations.

The first run failed because the test expected PERMISSION_DENIED for revocation.
The repository deliberately returns NOT_FOUND for inaccessible historical documents;
inspection of `DocumentAdmissionAuthorization` confirmed that behavior. The corrected
test asserts NOT_FOUND. No production behavior was changed. Sol reviewed the fixture
and routing assertions with no blocker.

```sh
./gradlew :protomolt-repo-container:test \
 --tests '*DocumentHistoricalRestoreAssessmentIT' \
 --tests '*DocumentSchemaRetentionIT' --console=plain
```

Result: 40 tests, zero failures/errors/skips, 33 seconds. Log:
`/tmp/protomolt-multiple-historical-sources-qualified.log`.
This closes the multiple-source SQL assessment coverage gap listed above. Provider
observations remain synthetic, and the publication and opaque-classification gaps
remain open.
