# Host registration through reader constructors

Base: `d756eca9621e609c844ed0db234e54efdd15ca40`.
The accompanying constructor, host utility and test changes were exercised by:

```sh
./gradlew :protomolt-repo-container:test --tests '*ReaderHostExecutionIT' --tests '*ReaderRegistrationIT' --tests '*ReaderHostPinFenceIT' --tests '*ArchiveReaderIncarnationIT' --tests '*DocumentReaderPinsIT' --tests '*DocumentHistoricalReaderPinsIT' --max-workers=2 --console=plain
```

58 tests passed with no failures, errors or skips. Build duration: 38 seconds.
The XML files record the final run. An earlier narrower run passed 27 tests.

Document and archive constructors register under an ACTIVE host execution before
returning. Null host arguments fail. Missing host identity fails startup and
leaves cleanup PENDING rather than reporting success. Existing local constructors
remain available. Tests exercise rollback and lost commit acknowledgment through
both new constructors, plus local shutdown after host fencing.

ReaderHostExecutions exposes registration and fencing for trusted Java host
composition. It is not a transport endpoint and grants no permission to attest
termination. Sol reviewed the implementation without a blocker.

External termination verification, orphan cleanup, production host wiring and
public historical routing remain unfinished.
