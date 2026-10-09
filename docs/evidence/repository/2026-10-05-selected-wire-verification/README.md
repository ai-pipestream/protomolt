# Selected historical wire verification

Local run on 2026-10-05, based on `39022d1b` plus the accompanying test changes.

```sh
./gradlew :protomolt-repo-admission:test --tests '*DocumentRetainedPathMaterializationTest' --console=plain
```

Passed 23 cases, with no failures, errors or skips. The retained JUnit XML includes
two added wire-verifier cases: seven altered-response variants, and cancellation
at every observed control checkpoint. Both verify reservation release. The
existing path tests continue exercising real protobuf descriptors and retained
schema artifacts, including different definitions under one type URL.

These are local Java boundary tests. They do not qualify remote clients, managed
host mounting, current authorization over a network, or performance.
