# Accepted historical call lifecycle

Base: `b76169eae87a685156f4137d783ec5f3a74063ca`. `source.sha256` identifies the
changed production and probe sources. No protobuf or public entrypoint changed.

The real PostgreSQL/LocalStack production-JAR fixture first failed against unchanged
production when historical attachment entered a closed publication admission barrier.
The caller had already acquired an outer call. `red-host.log.gz`, `red-gradle.log.gz`
and `red-source.sha256` record that failure.

`RepositoryHistoricalSuccessorActivation.openAcceptedExecution` now forks that live
call on the expected barrier. Foreign and ended permits fail before activation.
Activation/attachment failure closes the child; successful attachment transfers it
to the execution handle. The original new-admission method is preserved.

The scoped mixed successor fixture proves:

- A foreign barrier and an ended call are refused without adding permits.
- Cancellation during attachment propagates and refunds the child permit.
- Closed admission continues refusing new calls.
- An already accepted call can attach and publish after admission closes.
- Closing the outer call leaves the execution child counted; closing the execution
  makes the barrier idle before capture drain.
- Existing scoped authorization, receipt, provider bytes, predecessor fencing and
  cleanup assertions still pass.

Sol reviewed implementation and ownership. The full command
`./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain`
passed in 8m34s with one aggregate test and zero failures/errors/skips. The complete
Gradle/JUnit outputs include restart, forced-crash recovery and final lease-expiry
cleanup. `host.log.gz` is the complete initial host phase saved before test cleanup.

This qualifies the private attachment prerequisite. It does not establish managed
historical orchestration, public historical publication, or disposal of unconfirmed
activation. Those remain separate work; hosted CI and merge status are separate.
