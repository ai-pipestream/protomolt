# Exact-revision retention inventory

`DocumentRevisionRetentionInventory` is an internal process-authority diagnostic.
One PostgreSQL statement observes an exact sealed revision at its exact node address.
It returns only bounded metadata: up to 10,000 distinct physical object identities
and 64 artifact identities, with an overflow row that triggers explicit refusal.
No descriptor, content or provider bytes are loaded.

The snapshot contains:

- Current-head status and source-revision read-pin/assessment-slot counts. Pins count
  physical pin rows, not unique reader sessions.
- Per-object native history/current/archive/read/assessment references, mirror totals,
  and retiring/reclaiming flags.
- Per-artifact committed revision, operation claim and assessment references.
- Explicit unresolved preparation-journal selectors and future content-repository
  references. There is no complete-liveness or deletable flag.

The result can become stale immediately. It grants no read pin, row lock, pruning
capability or resource ownership. Counts retain expired and terminal claims until
an authoritative release removes them. It does not discover every pre-registration
in-flight operation, inspect command bytes, or prove writer quiescence. Control
checks are cooperative at query/iteration boundaries, not JDBC query cancellation.
Bounded result size and indexed lookups are not a measured latency guarantee.

V88 adds source-revision indexes for read pins and assessment slots. No existing
immutability guard, protobuf definition or public endpoint changes.

Tests use real PostgreSQL, schema artifacts and retention logic. Provider observations
and low-level assessment declarations are explicitly synthetic SQL fixtures; these
tests do not establish object-store behavior. Coverage includes typed and opaque
revisions, same objects shared by three revisions, a held historical Use through
capture closure, explicit pin release, staged reuse source slots, normalized schema
ownership by a pending assessment, and actual assessment expiry/release. Releasing
the assessment removes its artifact hold while its unreleased operation claim remains
counted. V87-to-V88 migration preserves an existing published revision. Authority,
wrong-address/missing revision, cancellation, immutable results and unchanged
publication/schema deletion guards are also checked.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentRevisionRetentionInventoryIT' \
  --tests '*DocumentHistoricalPublicationIT' \
  --tests '*DocumentAssessmentSlotsIT' \
  --tests '*DocumentAssessmentArtifactsIT' --console=plain
```

All four suites passed in 23 seconds. Local log:
`/tmp/protomolt-revision-retention-inventory-qualified.log`.
Sol reviewed the query, indexes, boundary and tests without finding a blocker.
Pruning itself, pending-journal reachability, atomic reference/deletion races,
backup/restore qualification and optional JCR semantics remain open. Hosted CI,
merge and deployment are separate from local verification.
