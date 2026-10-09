# Historical and fresh parts in one member

`DocumentHistoricalMixedMemberPublicationIT` exercises PostgreSQL, the actual
descriptor/admission implementation and atomic publication. One typed member
combines historical CORE content with a newly uploaded PARSED fragment.
Provider observations are synthetic fixture input; this test does not read an
object store or qualify provider durability.

An unverified upload refuses publication, leaves the destination absent and replay
pending, and creates no schema admission. After verification, publication preserves
the historical manifest entries and physical UUIDs, binds the fresh part to its
selected attempt/provider version, and checks its size, hash and content type.
Replay returns the exact result. Only the fresh occurrence invokes the ordinary
schema resolver. A separate historical schema reader reconstructs both values from
retained artifacts and checks the exact fragments and two roots. The source head
does not change, and owned pins and reservations drain.

Sol reviewed the assertions and ownership behavior without finding a blocker.
The five affected suites passed: 55 tests, no failures or skips, in 45 seconds.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentHistoricalMixedMemberPublicationIT' \
  --tests '*DocumentHistoricalMultiRevisionPublicationIT' \
  --tests '*DocumentHistoricalPublicationIT' \
  --tests '*DocumentHistoricalRestoreAssessmentIT' \
  --tests '*DocumentHistoricalSchemasIT' --console=plain
```

Local log: `/tmp/protomolt-historical-mixed-member-qualified.log`.
An initial compile caught an assertion against a content-type accessor absent from
the manifest; the final test checks the persisted physical-object content type.
Actual-provider same-member qualification is now recorded in the
[production-JAR follow-up](../2026-10-06-historical-mixed-member-provider/README.md).
Public/claimed restore, hosted CI, merge and deployment remain separate gates.
