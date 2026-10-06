# Atomic initial registration

Source base: `aa4ac666c072efa01739d24f0aeeaecce806ad0d`. Run on October 5, 2026,
America/New_York (October 6 UTC).

The initial private registration path previously committed its claim before its
recovery preparation. Two real PostgreSQL regressions fail against that code:
refusing the preparation commit leaves an orphan claim, and losing the first
commit acknowledgment leaves no durable preparation. `red.xml` and `red.log.gz`
retain those failures; they are assertion failures, not compilation failures.

The path now encodes bounded preparation before SQL, then acquires the claim and
inserts preparation in one transaction. Exact retries re-establish the SQL fence
and preserve token, seeds and lease. No provider call occurs under those locks.

Focused command:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentPublicationSessionIT' \
  --tests '*DocumentPublicationPreparationJournalIT' \
  --tests '*RepositoryExecutionClaimLedgerIT' \
  --tests '*RepositoryClaimMutationFenceIT' --console=plain
```

Result: 55 tests passed, zero skipped. The four `green-*.xml` files and
`green.log.gz` retain this result. Cancellation cases exercise entry, pre-SQL,
post-claim/pre-preparation, pre-commit and post-commit boundaries; they assert
neither row or both rows, exact retry and released byte reservations. A controlled
JDBC commit fault surrounds the real database. The broader fence test now reflects
the existing atomic owner/claim heartbeat behavior and still proves stale handles
cannot bypass a transferred claim with an unchanged owner nonce.

The production-JAR gate also passes:
`./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`.
`provider-runtime.xml` and `provider.log.gz` retain the PostgreSQL/LocalStack
integration result. It exercises the existing real-provider publication and
rejection variants, including journaled registration and assessment recovery.
`source.sha256` records the final source files; the focused preparation test had
only a redundant lexical block removed after its green run and was recompiled
by the production gate.

Ordinary runtime activation, scoped journal capability, interrupted registration
in a fresh process, and claim transfer remain separate requirements. Modes and
operation-owner admission still commit after the initial pair. These tests do not
claim forced-process crash recovery, new throughput results, or public RPC parity.
