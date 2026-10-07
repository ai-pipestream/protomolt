# Candidate admission storage gate

This run used clean candidate commit `137a8f2593277db79c8c67e01671cc76b61ff3a9`, containing root checkpoint `9189b5bd0`. Input hashes and the exact command appear at the beginning of `gradle.log`.

Command:

```text
./gradlew :protomolt-repo-container:admissionStorageTest --rerun-tasks --max-workers=2 --console=plain
```

Result: `BUILD SUCCESSFUL` in 8m02s. The production-JAR admission and PostgreSQL runtime test passed (1 test, 0 failures, 0 errors, 0 skips); the test XML is included alongside the full Gradle output.

Root checkpoint `9f83399cb` was pushed after this run. This is evidence for the listed commit only; full candidate qualification will run again after that checkpoint is integrated.
