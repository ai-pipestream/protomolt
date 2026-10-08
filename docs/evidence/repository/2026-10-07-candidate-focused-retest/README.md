# Candidate focused regression retest

These focused reruns cover the repository regression fixture repairs and the expanded V95 preparation-preservation assertion. They ran on candidate source commit `805bc682a88e6ec9729ce34899ffe7bed11284c4`, before the later root checkpoint merge at `66e33b7b127b26ae46eb52f3492431769ecf5acb`. The checkout was clean for both runs. The exact source hashes and commands are recorded at the start of each Gradle log.

## Container tests

Command:

```text
./gradlew :protomolt-repo-container:test --tests ai.protomolt.proto.repo.container.ledger.RepositoryCoordinatorHandoffIT --tests ai.protomolt.proto.repo.container.ledger.DocumentOperationCommandsIT --tests ai.protomolt.proto.repo.container.ledger.DocumentRevisionSchemaArtifactsIT --tests ai.protomolt.proto.repo.container.ledger.DocumentRevisionSchemaAssetsIT --tests ai.protomolt.proto.repo.container.ledger.DocumentRevisionSchemaEvidenceIT --tests ai.protomolt.proto.repo.container.ledger.RepositorySuccessorInstallIT --rerun-tasks --max-workers=2 --console=plain
```

Result: `BUILD SUCCESSFUL`; 58 tests, 0 failures, 0 errors, 0 skipped across six classes. The Gradle output and individual JUnit XML reports are in `container/`.

## Service test

Command:

```text
./gradlew :protomolt-repo-service:test --tests ai.protomolt.proto.repo.service.ArchiveDeletionFailureIT --rerun-tasks --max-workers=2 --console=plain
```

Result: `BUILD SUCCESSFUL`; 9 tests, 0 failures, 0 errors, 0 skipped. The Gradle output and JUnit XML report are in `service/`.

These are focused results only; the combined container/service gates must be rerun against the merged candidate head.
