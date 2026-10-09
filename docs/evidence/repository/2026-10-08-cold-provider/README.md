# Cold registry publication with a real object provider

Base: 90f86e0e8, with the cold provider qualification and retention-budget fix.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.coldOwner' --max-workers=2 --console=plain
```

Exit 0 in 35s: one aggregate test, zero failures or skips. The harness runs a
production-JAR host against PostgreSQL and LocalStack. It asserts missing/corrupt
resubmitted payload rejection before reservation, cold installation across calls,
fresh historical provider reads, upload, assessment CREATE, one publication,
receipt replay, normal retirement, and return of retained memory. The same mode
is included in the mandatory storage host loop. That full aggregate has not been
rerun for this change.

The first run failed with `PayloadBudget.CapacityExceededException` at activation.
The anchor loader kept its 48 MiB decoding scratch reservation after returning a
small record. The fix retains the actual encoded record size, releases scratch
after transfer, and separately reserves scratch during preparation digest
encoding. The provider fixture's 128,000,000-byte budget is unchanged.

Sol reviewed the routing, assertions and memory ownership without blockers. This
host creates the predecessor and cold entry in one JVM. It does not prove recovery
after a process crash, transport parity or performance. Public historical routing
remains disabled.

The subsequent SQL regression passed in 2m22s: 33 tests, zero failures or skips
(31 preparation cases and two loader cases). It checks exact retained record-size
accounting, availability of the remaining budget, insufficient memory and caller
rejection, plus the full preparation suite after the accounting change.

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryHistoricalPreparationIT' --tests '*RepositoryHistoricalRetentionLoaderIT.loadsInitialAnchorBeforeAnySuccessorInstall' --tests '*RepositoryHistoricalRetentionLoaderIT.rejectsWrongCallerAndInsufficientMemory' --max-workers=2 --console=plain
```
