# Host termination receipts

Base: `e0e4b97ce14f75a6c59505d453611b7c5092e9c5`.
Command for the accompanying V113, termination service and test:

```sh
./gradlew :protomolt-repo-container:test --tests '*ReaderHostTerminationIT' --tests '*ReaderHostExecutionIT' --tests '*ReaderRegistrationIT' --max-workers=2 --console=plain
```

33 tests passed without failures, errors or skips in 24 seconds. Archived XML
records that final run. Sol reviewed the production code without a blocker.

Termination cases use actual child JVMs and PostgreSQL. They reject a live child,
missing verifier, mismatched identity, unsupported proof format, and conflicting
proof. They verify atomic SQL receipt/state recording, receipt immutability,
uncertain commit replay, concurrent exact retries and rejected attestation reuse.
Host termination does not change reader state or permit recovery.

The managed children have no database sessions or subprocess workers. This is not
remote-host or orphan-reclamation qualification. Child startup and exit waits are
bounded. Production verification has no default; host configuration and restart
driver integration remain required. Per-reader external quiescence is next.
