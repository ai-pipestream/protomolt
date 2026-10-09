# Unavailable historical source generation

A managed host now reports `RepositoryException.Code.FAILED_PRECONDITION` when the
original document backend generation/profile is not configured. gRPC preserves the
same code and message. Previously the shared backend guard threw an unclassified
`IllegalStateException`, which the transport mapped to `INTERNAL`.

The qualification uses real PostgreSQL, Redis and Git. One bounded host publishes a
typed source. Another host mounts the same reachable Redis endpoint and storage realm
under a different generation. A new historical publication must refuse that source;
silently substituting its current backend would otherwise reach the same bytes.

Both library-first and authenticated in-process gRPC-first calls refuse with the
exact expected status/message. No schema resolution, assessment owner, revision
commit or success receipt is produced. The negative host drains its reader and is
fenced. The original correctly configured host then publishes an independent operation
using the same source, verifies bytes and retained object identity, and replays its
receipt. Existing enabled/disabled and bounded ordinary publication checks remain green.

```sh
./gradlew :protomolt-repo-service:test \
  --tests '*ManagedSchemaHostIT' \
  --tests '*ManagedPublicationOptionsTest' \
  --tests '*ManagedSchemaOwnershipTest' \
  :protomolt-repo-container:admissionStorageTest \
  --tests '*DocumentAssessmentStorageRuntimeTest.managedHistoricalHostBinding' \
  --tests '*DocumentAssessmentStorageRuntimeTest.boundedPublicHostBinding' \
  --max-workers=2 --console=plain
```

Passed in 29 seconds: 14 cases, zero failures, errors or skips. Two cases execute
aggregate production-JAR host probes. Archived XML includes the earlier red case
showing the unclassified exception. Sol reviewed the fixture and boundary change;
the suggested explicit absence of a success receipt is asserted.

This proves missing source-generation refusal for a new historical operation. It
does not prove missing upload placement during cold recovery, configuration rotation,
or resuming that failed operation after mounting another backend. Matching-backend
cold recovery is covered separately. No multi-backend provider discovery, fallback,
network listener qualification or deployment is claimed.
