# Private execution claim prerequisite

Command:

```
./gradlew :protomolt-repo-container:test --tests '*RepositoryExecutionClaimLedgerIT' --tests '*DocumentPublicationSessionIT' --tests '*DocumentOperationUploadAdmissionIT' --console=plain
```

The PostgreSQL tests cover exact acquisition and transfer replay without renewal,
command/token conflict, explicit expired takeover, stale renewal, four competing
transfers with one winner, rollback-only after a caught failed fence, expiry after
a verified row-lock wait, direct SQL guard violations, caller-key separation and
rejection of snapshot isolation. The lock-wait case uses exact backend PIDs and
`pg_blocking_pids`, not timing alone. Existing session/upload cases qualify that
the additive migration leaves their behavior intact.

Sol reviewed the implementation and found no blocker. Epoch-only predecessor CAS
is valid under the guarded, nondeletable monotonic row. Takeover authentication is
still a host obligation. This private primitive is not wired into publication
mutations and does not establish cross-host recovery, provider safety, horizontal
capacity or a public API. SQL-visible claim stamps and dependent mutation guards
remain to be implemented before enabling shared session recovery.

Final run: BUILD SUCCESSFUL, 106 tests (7 claim, 8 session, 91 upload), zero
failures/errors/skips. XML results are retained here. An initial test compilation
error from an ambiguous Tx lambda overload was corrected before execution.
