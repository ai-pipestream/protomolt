# Historical provider read interface

Base: `3a4b491a1dcd91feb59742034088d47ed23a2f43`; tested sources are hashed alongside this note.

DocumentHistoricalRetainedReader exposes the engine's existing exact-ordinal historical read. The focused test uses PostgreSQL and LocalStack, overwrites the current provider key, and verifies the retained version's bytes through the interface. It checks released byte reservations and retains the existing batch lifetime assertions.

Command:

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentPublicationCommitIT.readsExactHistoricalProviderVersionAndKeepsPinsWithReturnedBatch' --max-workers=2 --console=plain
```

Exit 0; one test, zero failures, errors or skips. Gradle: 33 seconds. JUnit: 10.537 seconds. Logs and XML are archived here.

This qualifies the interface and existing engine implementation. It does not enable public historical publication or qualify managed runtime integration.
