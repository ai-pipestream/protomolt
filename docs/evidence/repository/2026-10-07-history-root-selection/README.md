# Historical source selection and retention projection

Base `66cb29a2d5d531c8a2043b0d7c385a29ebb8007c`, plus the test changes in this
commit. Local PostgreSQL 18 container; physical provider observations in this
existing fixture are synthetic. This is SQL, schema, selector and pin-lifetime
evidence, not an object-provider integration test.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentHistoricalMultiRevisionPublicationIT' \
  --tests '*DocumentPreparationHistoryRootsIT' \
  --max-workers=2 --console=plain
```

Exit 0 in 11 seconds: 5 tests, no failures, errors or skips. XML trailing
whitespace normalized; content otherwise unchanged.
Sol reviewed the test and its evidence scope with no blocker.

Two committed revisions of the same source node supply actual historical
selectors. The scenarios distinguish two historical revisions from historical
plus current reuse. A repeated selector in another destination does not add a
retention root; different revisions of the same node remain distinct. Java's
ordered digest matches PostgreSQL aggregation over those stored revision rows.
The actual source owner prepares complete references, rejects a missing source,
and invalidates the borrowed references when closed. Existing publication and
exact replay assertions still pass afterward.

This does not qualify nonempty preparation registration, claimed historical
execution, retention release or pruning. Those paths remain disabled or pending.
