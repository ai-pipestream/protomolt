# Historical publication authorization and revision selection

The authorization test uses real PostgreSQL blocking, not a timed assumption.
After creating a scoped caller with READ and WRITE access, it blocks publication
at the schema-policy pointer row. `pg_blocking_pids` and the waiting query establish
that promotion and staging finished and publication reached that lock. Another
transaction removes READ while retaining WRITE. After the blocker releases,
publication must return NOT_FOUND, leave the published revision pointer unchanged,
and create no revision commit, schema admission or terminal-success row. A matched
case without revocation must publish successfully.

The first assertion incorrectly expected the document mutation counter to stay at
one. ACL updates intentionally advance that counter. The corrected test compares
it with the value after the ACL update and separately checks the immutable
publication pointer and operation rows. It therefore distinguishes the authorized
ACL change from a forbidden publication.

The revision-selection test creates two real SQL revisions of the same source
node and slot, with different bytes, hashes, physical UUIDs, producer metadata and
drives. The second source revision uses the ordinary production commit path. One
command then publishes the old and new content to two separate destinations.
One variant uses two historical captures supplied in reverse order; another uses
the old historical capture alongside current-revision reuse. Only the latter may
resolve the current member's schema, exactly once.

Assertions compare canonical member order, exact destination addresses, selected
drive names, producer metadata, content hashes and physical UUIDs per result.
The source head remains unchanged and authorized replay returns the same result.
The initial strengthened order assertion used insertion order; it now follows the
contract's canonical member ordering.

These tests use real PostgreSQL, descriptor validation and commits, with explicitly
synthetic initial provider observations. They do not establish real-provider
throughput or mixed fresh-upload behavior. Sol reviewed both test designs and
identified the mutation-counter and ordering assertion corrections above.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentHistoricalMultiRevisionPublicationIT' \
  --tests '*DocumentHistoricalPublicationAuthorizationIT' \
  --tests '*DocumentHistoricalPublicationIT' \
  --tests '*DocumentSchemaRetentionIT' --console=plain
```

All 14 tests passed with zero failures, errors or skips in 17 seconds. Log:
`/tmp/protomolt-historical-mixed-publication-qualified.log`. This checkpoint adds
qualification and a test-fixture update precondition; it required no production
behavior change. Hosted CI, public activation and deployment remain separate.
