# External reader quiescence

Base: `6389c5248263db7030efeba06b0bea081e10ab2d`.
Command for the accompanying V114, Java operation and tests:

```sh
./gradlew :protomolt-repo-container:test --tests '*ReaderExternalQuiescenceIT' --tests '*ReaderHostTerminationIT' --tests '*ReaderHostExecutionIT' --tests '*ReaderHostPinFenceIT' --tests '*ReaderRegistrationIT' --tests '*ArchiveReaderIncarnationIT' --tests '*DocumentReaderPinsIT' --tests '*DocumentHistoricalReaderPinsIT' --max-workers=2 --console=plain
```

68 tests passed without failures, errors or skips in 38 seconds. The new test XML,
suite counts and Gradle output are archived here. Sol reviewed the implementation
without a blocker.

PostgreSQL and a managed child JVM exercise exact registration matching, immutable
provenance, lost acknowledgment, atomic receipt/state commit and separate bounded
pin recovery. Object fixtures are synthetic, without provider I/O qualification.

Competing cleanup attempts wait on an actual reader lock and return one receipt.
Another reader completes during that wait. The host remains independently lockable.
The test needs a pool of 4 connections for the blocker, waiters and observer;
the initial pool of 3 exhausted capacity. Recursive inspection includes indirect
lock waiters. These fixture corrections preceded the successful run.

Archive recovery with external evidence, crash-harness integration, capture cleanup,
root release and production host verification remain acceptance work.
Public historical routing remains disabled.
