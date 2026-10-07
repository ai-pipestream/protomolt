# Damaged retention evidence cannot become a recovery-limit receipt

Base: `abbe115690796f6f3d244467916ba79afe10dd5f`. This checkpoint adds test
coverage only; no production Java, migration or protobuf changes.

Five isolated PostgreSQL 18 fixtures reach the actual 16-capture bound, then
introduce one kind of administrative corruption:

- Incorrect retained-root digest.
- Missing retained root.
- Changed source-pin publication revision, invalidating its batch digest.
- Changed capture-owner token, invalidating its coordinator binding.
- Removed initial-capture designation.

Only the relevant table's immutable guard is temporarily disabled, within the
mutation transaction, and restored before that transaction commits. The test
checks `pg_trigger` before invoking the ordinary production handler. No relaxed
guard participates in the decision attempt. This setup models damaged persisted
evidence; it does not claim ordinary callers can perform these mutations.

Each handler call reports the expected canonical-root, capture-content or
original-ownership error. None creates a decision, rejection, success, execution,
historical activation or capture drain for the target operation. Sixteen batches
remain; leases and the fixture's existing source-publication success count are
unchanged, and reserved payload memory returns to zero.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalLimitCorruptionIT' \
  --max-workers=2 --console=plain
```

The first run correctly reached every expected runtime error but failed its
postcondition: it expected zero global success rows, ignoring the fixture's
already-published source document. The corrected assertion preserves that baseline
and independently checks zero successes for the target operation. The initial
failure log is retained; this is a test correction, not a production defect fix.

The final run passed five tests with zero failures/errors/skips. Its XML in
`results.tar.gz`, `gradle.log` and source fingerprints accompany this evidence.
Sol reviewed the corruption method and corrected assertions with no blocker.
Initial provider observations are synthetic, while the SQL mutations and runtime
checks are real. These are representative corruption cases, not exhaustive
coverage of every retention field. Ancestry corruption, retained-root release,
public recovery and deployment remain separate work.
