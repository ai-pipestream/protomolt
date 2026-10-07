# Claim expiry before publication finalization

Base: 5316e95ee6222394978109090f930f70e4c4cafa. Sol reviewed the fixture and
aggregate wiring. Production code and deadlines are unchanged.

```
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.claimExpiresBeforeFinalization' --max-workers=2 --console=plain
```

Final: 1 test, 0 failures/errors/skips; Gradle 1m05s, JUnit 63.129s,
timestamp 2026-10-07T22:38:40.354Z. Earlier corrected run: 1m07s.
Real PostgreSQL and LocalStack; source hashes and reports are attached.

Backend PIDs establish origin-to-publisher-to-takeover contention. After claim
expiry, assessment and upload deadlines remain live. Capture revalidation throws
RepositoryExecutionClaimLedger.Fenced. Publication rolls back; takeover commits
with exact reservation confirmation. No result or revision is committed. Local
cleanup returns the retained bytes.

The initial fixture expected a later SQL-trigger failure. The archived red report
records the earlier rejection. An aggregate-marker compilation error was also
corrected before this final run.

The aggregate requires this isolated host with the existing 90s cap and all prior
cases. Aggregate execution remains pending. Successor publication after this race
and the before-claim ordering remain open; the repository goal is incomplete.
