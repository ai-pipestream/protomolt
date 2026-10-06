# Internal historical publication

The historical assessment owner can now promote accepted content, stage its
schema artifacts and publish a new revision in one owned scope. Promoted content
does not escape its source Uses. The transaction checks current policy and
READ/WRITE rights, drive identity, independent source and retention locks, exact
historical physical bindings and schema claims before writing. Ordinary execution
and claimed historical sessions remain gated.

Promotion consumes the assessment. A failure after promotion requires rebuilding
that scope if a retry is authorized; an uncertain commit requires outcome replay
first. The owner does not perform a cancellation check after a returned durable
success. Source Uses remain live until the synchronous transaction completes.

Four real-PostgreSQL integration cases cover typed and explicit opaque restoration,
restoring the original revision again after current advances, current-policy
change, and a fault at the final terminal-success insert. The fault rolls back
the document pointer, schema admission and terminal result together. These tests
use explicitly synthetic initial provider observations. They passed in 12 seconds
with zero failures, errors or skips:

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentHistoricalPublicationIT' --console=plain
```

Log: `/tmp/protomolt-historical-publication-faults.log`. The first draft assertion
used the wrong schema-admission table name; correcting that test query did not
require changing publication behavior.

The production-JAR harness adds real bounded GETs of original provider versions,
strict admission without current-registry access, normal publication and an
acknowledgment failure after the actual PostgreSQL commit. Only the commit uses
the fault datasource; schema staging uses the ordinary transaction service.
Authorized replay must recover the durable result. Checks include new revision
identity, original manifest/provenance, exact original physical UUIDs in ordinal
order, no upload attempts, and validated decoding through retained descriptors.
It does not instrument or count every provider method invocation.

That harness exposed a reader gap: `DocumentHistoricalSchemas.replay` derived its
expected fragment hash only from upload and current-reuse declarations. A newly
published historical-reuse revision therefore failed validated rereading. The
reader now also takes the hash from the historical declaration; the same real
provider case is its regression. The neighboring occurrence resolver already
handled historical declarations.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```

The complete production-JAR storage harness passed in 2 minutes 38 seconds, with
zero failures, errors or skips. The outer test requires both
`HISTORICAL_PUBLICATION_OK` and `HISTORICAL_PUBLICATION_LOST_ACK_OK`, alongside the
existing assessment, restart and crash markers. Log:
`/tmp/protomolt-historical-publication-runtime-final.log`. The backend was versioned
LocalStack S3 with PostgreSQL; this is correctness evidence, not RustFS performance.
The preceding failed run established the restored-read hash regression described
above. An earlier harness compile failure was a missing import in the new probe.

The affected historical publication, selection, ordinary assessment, historical
schema and schema-boundary suites also passed: 62 tests, zero failures, errors or
skips, 42 seconds. Log: `/tmp/protomolt-historical-publication-regression.log`.

Sol reviewed transaction order, scope ownership, native SQL compatibility, fault
targeting and test boundaries. Mixed upload/current/historical commands, two
revisions of the same source node/slot, and READ revocation during lock waits
remain dedicated qualification work. Public restore, claimed-session activation,
automatic claim transfer, hosted CI and deployment are not established here.
