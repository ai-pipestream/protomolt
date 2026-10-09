# Historical recovery storage gate

Tested code: `be318c04b4eef9984b0c1676b7313da88673eed6`.
Checkout HEAD at completion: `1eb7baea71149bf28d3116b7221c0b36238ea40c`.
Later commits only changed documentation. No source changed during execution.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain
```

Result: BUILD SUCCESSFUL in 18m14s. The production-JAR driver reports one aggregate
test, no failures, errors or skips, with 1091.417 seconds of test execution.
The archived XML and Gradle log record that run, including recovery after the
writer exits at initial START, reservation and installation.

V112 and reader-host lifecycle changes were not part of this execution. External
termination, orphan reclamation, public routing and load testing remain unqualified.
