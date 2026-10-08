# Combined candidate container test gate

This full repository-container test run used candidate commit `6c32eba7263ba603a5c103961244fad442131119`, which includes the scoped transport qualification, provider qualification, regression fixture repair, reviewed migration assertion, and root checkpoint `66e33b7b127b26ae46eb52f3492431769ecf5acb`. The worktree was clean before the command. Source hashes and the exact command are recorded at the beginning of `gradle.log`.

Command:

```text
./gradlew :protomolt-repo-container:test --rerun-tasks --continue --max-workers=2 --console=plain
```

Result: `BUILD SUCCESSFUL` in 21m33s. The 220 JUnit XML reports contain 1,952 tests, 0 failures, 0 errors, and 1 skipped benchmark. The full Gradle output is in `gradle.log`; the reports are in `test-results-xml.tar.gz`.

The later candidate commit `3de26f2919001e92282aafc486ea02223e53529c` changes only the CI diagnostic path parser. The service and admission-storage gates are recorded separately after they complete.
