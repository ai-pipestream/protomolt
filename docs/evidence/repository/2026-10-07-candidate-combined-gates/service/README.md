# Combined candidate service test gate

This full repository-service test run used candidate commit `91baba458f535244fcf5a1c484de13d4d09e43af`, including the scoped transport qualification, provider qualification, regression fixture repair, reviewed migration assertion, root checkpoint `66e33b7b127b26ae46eb52f3492431769ecf5acb`, and CI diagnostic path-parser fix. The worktree was clean before the command. Source hashes and the exact command appear at the beginning of `gradle.log`.

Command:

```text
./gradlew :protomolt-repo-service:test --rerun-tasks --continue --max-workers=2 --console=plain
```

Result: `BUILD SUCCESSFUL` in 12m54s. The 55 JUnit XML reports contain 490 tests, 0 failures, 0 errors, and 3 skipped benchmarks. The full Gradle output is in `gradle.log`; the reports are in `test-results-xml.tar.gz`.

This run was completed before root follow-up commit `9189b5bd0` and provides evidence for this exact candidate source state. Final qualification must run again after incorporating that commit.
