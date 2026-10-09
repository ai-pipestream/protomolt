# Bounded abandonment confirmation

Base `3a4e3da33fc1455f8ccb82d417a3fa03fb13b594`, branch
`refactor/repository-composition`, plus files in `source-sha256.txt`.

`red/` records four lost-response cases: both ordinary cases passed and both
bounded cases failed at `Tx.readOnly`, before the abandonment write. The fix
uses `inTransaction` for the same exact evidence query so transaction-local SQL
timeouts apply. It adds no claim mutation or identity fallback.
`initial/compile.log` records an intermediate ambiguous Java lambda overload,
corrected with an explicit return block; it is not the regression baseline.

Final qualification: exit 0, 39 seconds, 23 tests, no failures/errors/skips.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentPublicationAbandonmentIT' \
  --tests '*DocumentCaptureAdmissionClosureIT' \
  --max-workers=2 --console=plain
```

The 18 abandonment cases include ordinary/bounded lost-commit-acknowledgement
recovery with and without fixed modes, retry after expiry, exact confirmation
after claim transfer, wrong-token and public-caller rejection, unchanged lease,
and a real ACCESS EXCLUSIVE table lock causing the configured lock timeout.
The timeout releases the payload budget and confirmation succeeds after rollback
of the blocking transaction. No provider work is simulated for confirmation.

The five capture cases include a genuine generation-1 historical registration
with fixed modes and a real V64 cancellation receipt, followed by V108 refusal of
a new capture. The historical source fixture uses synthetic provider observations
and real PostgreSQL. It does not prove provider performance or source drain.

Sol reviewed the production change and real terminal fixture without a blocker.
V52 success capture coverage remains pending: claimed historical commit is still
gated, and this checkpoint does not bypass that gate or fabricate success rows.
No root release, public execution, full storage rerun or wire-contract change is
claimed. JUnit XML trailing whitespace is normalized in retained evidence.
