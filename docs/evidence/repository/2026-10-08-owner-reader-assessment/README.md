# Retained assessment provider preparation

Base: `6832fa88b`. Source hashes accompany this report. Sol reviewed the ownership changes.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.initialOwner' --max-workers=2 --console=plain
./gradlew :protomolt-repo-container:test --tests '*DocumentPublicationAssessmentTest' --tests '*DocumentHistoricalRestoreAssessmentIT' --tests '*RepositoryInitialHistoricalAttemptsIT' --tests '*DocumentPublicationCommitIT.preparesSharedHistoricalFragmentsOnceAndCleansUpOnCancellation' --max-workers=2 --console=plain
```

Both commands exited 0. The packaged PostgreSQL/LocalStack probe passed in 27s (1 aggregate case), exercising initial publication and lost CREATE acknowledgement with provider-fetched historical fragments. Assessment regressions passed in 47s: 67 tests across 4 suites, zero failures, errors or skips.

The probe initially called awaitIdle before closing reader admission; correcting that fixture order resolved the failure. An earlier invocation used a nonexistent task name and executed no tests.

The assessment owns the helper's snapshot directly. Mode checks precede provider access; the retained Work and registration child cover preparation and cleanup. Public routing and managed host shutdown remain unfinished.
