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
