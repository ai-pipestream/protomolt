# Recovery preparation metadata ownership

Base: 1ac6f9af6. Preparation can start without a supplied retention record.
Reservation precedes predecessor and anchor loading. Installation requires
verified retention. Preparation retains the loader lease, digest and successor
plan through retries. Supersession and close release metadata leases.

PostgreSQL tests cover installation, mode mismatch before reservation, and a
missing anchor before installation. Assertions cover stable identities, memory
cleanup, no activation and no recovery publication. Source fixtures use synthetic
provider observations. Missing-anchor corruption is injected with SQL triggers
disabled; application writes cannot perform that change.

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryHistoricalPreparationIT' --max-workers=2 --console=plain
```

Exit 0 in 1m30s: 21 tests, zero failures, errors or skips. Test development corrected
a global revision count that included the source fixture and an AssertJ inference
error. These failures originated in the test assertions.

Sol found no blocker. Registry integration, lost-reply qualification, recovery
across multiple successors and separate-process provider execution remain open.
Public historical routing is disabled.
