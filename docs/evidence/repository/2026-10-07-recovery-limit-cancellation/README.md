# Request cancellation at the recovery-limit commit boundary

Base: `2f2a9f9dd2daca597910f4c12f393e2f75ba634b`. This checkpoint changes tests
and documentation only. `source-sha256.txt` fingerprints the tested handler and
test source. The SQL implementation and migrations are unchanged.

The two new cases use real PostgreSQL 18 and the existing JDBC delegation hooks:

- An already-cancelled request is refused with CANCELLED before either decision
  row exists. For an active request, the before-commit hook observes its tentative
  decision and injects cancellation before delegating JDBC commit. The exception
  cause chain contains CANCELLED and both rows roll back.
- The after-commit hook observes the committed decision and sets the cancellation
  flag without throwing. The handler returns its durable receipt; authorized
  replay returns that same receipt. A still-cancelled retry is refused, and a
  fresh uncancelled retry succeeds without duplicating the result.

Both cases assert exactly one pair after clean retry, no execution or capture
drain, unchanged claim/owner leases, and zero reserved payload budget.

This qualifies controlled cancellation at the JDBC boundary. The before-commit
hook itself calls `control.check()`; it does not establish that the production
handler can interrupt a JDBC commit already in progress. The late signal cannot
roll back a committed decision. Lost response handling remains exact replay,
with current authorization, rather than inference from the exception alone.

Command:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalLimitDecisionsIT' \
  --max-workers=2 --console=plain
```

Final execution passed all five tests with zero failures, errors or skips in
1m1s. `results.tar.gz` contains the JUnit XML and `gradle.log` the build output.

Sol reviewed the new cases with no blocker and suggested checking the CANCELLED
code in the cause chain; that assertion is included. The archive and build log
record the final execution, including the three existing cases. Provider
observations in these fixtures are synthetic; no provider cancellation,
performance, host transport or deployment claim follows from this check.

Concurrent decision/activation, corrupt retained evidence, expired-lease replay,
source-root release and an explicit terminal cancellation operation for a
non-exhausted unactivated successor remain separate work. This private handler
is still unmounted.
