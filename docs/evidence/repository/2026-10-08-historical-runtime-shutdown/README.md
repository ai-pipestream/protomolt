# Historical runtime shutdown

Base: 06a0ff609. The packaged PostgreSQL/LocalStack aggregate passed in 34 seconds,
exit 0, one aggregate test with zero failures/errors/skips. Runtime configuration
and historical-owner regression tests also passed, exit 0, in 22 seconds. XML
reports and tested source hashes are adjacent.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.initialOwner' --max-workers=2 --console=plain
./gradlew :protomolt-repo-container:test --tests '*DocumentPublicationRuntimeConfigTest' --tests '*RepositoryInstalledHistoricalAttemptsIT' --max-workers=2 --console=plain
```

The new runtime probe registers two initial historical owners with real SQL
captures. A batch read through DocumentPartReader retains actual LocalStack bytes.
After outer admission closes, a controlled failure in the host authority lookup
leaves both owners and the reader retained. Clearing that failure and retrying
shutdown disposes the ready generation while the earlier held batch keeps its
generation alive. Releasing the batch permits a later shutdown pass to dispose
the remaining owner, close the reader, and return all capture and byte accounting.

The reader wrapper delegates every read and lifecycle call to DocumentPartReader;
it only records when close is called. This fixture has no external workers and
performs no uploads. It proves returned-batch lifetime, not an in-flight provider
worker or public request dispatch. Those have separate acceptance requirements.

Sol reviewed the changes with no blocking findings. Public historical routing
remains disabled. The private registry accessor still requires accepted-scope
enforcement in the future driver. Cold recovery and historical rejection remain
unfinished. No full storage-suite result is claimed for this checkpoint.
