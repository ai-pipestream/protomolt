# Private preparation terminal evidence

This change adds a transaction-local inspector, not a root-release operation.
It locks the claim and exact retained preparation, checks canonical terminal
identity and SQL projections, and returns private immutable evidence. It does
not renew leases, drain captures, delete roots, or touch provider storage.

Tested as changes on base `d1f922166e27b325c11a8bbe9fd955d374f207ca`.
`source-sha256.txt` identifies the exact final executable sources; this README
is committed alongside them. No public contract or migration changes.

## Qualification

Final command, exit 0, Gradle wall time 29 seconds:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentPreparationTerminalEvidenceIT' \
  --tests '*DocumentPublicationRejectionIT' \
  --tests '*DocumentPublicationCommitIT.scopedJournaledPublicationUsesRealProviderAndKeepsReceiptAfterGrantRevocation' \
  --max-workers=2 --console=plain
```

`qualified-results.tar.gz` contains 17 tests, zero failures/errors/skips:
two terminal-inspector tests, 13 existing rejection tests, and two real-provider
journaled publication cases (typed and opaque). `qualified-gradle.log` retains
the invocation output. The scoped journaled publication cases use PostgreSQL
and versioned LocalStack through existing real adapters.

The inspector tests prove pending operations cannot qualify, non-process callers
cannot use the private inspector, exact initial abandonment qualifies, a changed
preparation fails, and canonical cancellation qualifies without removing roots
or changing leases. Test-admin corruption of a rejection timestamp and success
member count is detected as DATA_LOSS. Each corruption and its trigger change
are rolled back; successful qualification repeats after the success rollback.

`bound-results.tar.gz` retains the preceding PostgreSQL run of all three
`RepositoryHistoricalRecoveryBoundIT` cases (106.333 seconds, zero
failures/errors/skips). At the actual 16-capture and 65-edge limits, the tests
qualify the original preparation against the real reason-4 decision/rejection
pair. Ending retention does not require a complete execution ancestry walk.
These are terminal-inspection checks, not evidence that roots can be released.

## Failed fixture attempts retained

`initial-unsupported-historical-fixture.log` records an attempted success fixture
using claimed historical publication. That path explicitly remains unimplemented;
the gate was preserved. The bound tests passed in that same invocation, whose
overall exit was nonzero because of the unsupported fixture.

`initial-unjournaled-fixture.log` records the next attempted success assertion in
the host runtime convenience path. Both cases failed with NoResultException
because that path did not save V81 preparations. The assertion was moved to the
existing scoped journaled path; no production fallback or fabricated journal
row was introduced. Neither failed invocation is counted as a passing run.

## Review and remaining work

Sol reviewed the inspector and shared rejection-projection extraction. Its
initial blocker (missing SQL projection correspondence) was fixed; the rereview
found no remaining blocker. Success member-count corruption was then added to
qualify that check explicitly.

V111 release receipts, SQL deletion guards, all-capture drainage integration,
released-coverage detection, pruning-safe retries and release races are still
unfinished. The public claimed historical path remains gated. This evidence
does not establish root-release completion, full repository-goal completion,
hosted CI, merge, deployment, or performance.
