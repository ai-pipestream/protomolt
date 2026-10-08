# Combined candidate gates

These gates ran on clean candidate commit `a98a3d15c78d41e9394867eba67dce5e89ecf1d5`, including root checkpoints `9189b5bd0` and `9f83399cb`, regression repair `bc93c7f39`, the reviewed migration assertion, and the repository side-task changes. The initial source hashes, worktree status, and exact command appear at the top of `gradle.log`.

Command:

```text
./gradlew :protomolt-repo-container:test :protomolt-repo-container:admissionStorageTest :protomolt-repo-service:test --rerun-tasks --continue --max-workers=2 --console=plain
```

Result: `BUILD SUCCESSFUL` in 36m48s. JUnit totals from this run:

- Container: 220 XML reports, 1,955 tests, 0 failures, 0 errors, 1 skipped benchmark.
- Service: 55 XML reports, 490 tests, 0 failures, 0 errors, 3 skipped benchmarks.
- Production-JAR admission and PostgreSQL: 1 test, 0 failures, 0 errors, 0 skips; elapsed test time 468.299s.

The full Gradle output is in `gradle.log`. Container and service XML bundles and the admission test XML are included in this directory.
