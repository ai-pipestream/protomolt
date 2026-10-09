# Reader recovery supervisor qualification

Code checkpoint: `9f4f0236c` on `agent/reader-recovery-supervisor`.

```sh
./gradlew :protomolt-repo-container:test --tests '*ReaderHostRecoveryIT' --tests '*DocumentAssessmentReadSessionIT' --tests '*ArchiveExternalQuiescenceIT' --max-workers=2 --console=plain
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.coldProcessRestart' --max-workers=2 --console=plain
```

SQL: 16 tests passed, no failures, errors or skips; 22-second build. These cover
page failure with progress on another reader, cancellation preserving an earlier
error, cancellation after committed cleanup, exact receipt binding, traversal end
without recovery completion, foreign retention, LOCAL_DRAIN provenance, and a
shared limit of one across archive, document and assessment resources. Fixtures
use real PostgreSQL with synthetic retained objects and no provider workers.

Crash restart: 3 cases passed, no failures, errors or skips; 1m39s build and
96.085-second test suite. Each production-JAR writer terminates with code 23 in a
different preparation phase. A fresh process restores the operation, executes
bounded supervisor cleanup, requires a zero-work pass, then uses separate operation
authority for capture and root release. The harness verifies the actual child exit
and absence of the writer's database sessions. The original publication receipt
remains replayable.

Sol reviewed the supervisor and crash-harness changes without a blocker. Suggested
coverage for cancellation preserving an earlier error was added before the final
SQL run. This is a callable supervisor step. Deployed scheduling, remote termination
verification, fairness and load qualification remain separate work. No protobuf
contract changed.
