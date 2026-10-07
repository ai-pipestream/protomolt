# Scoped publication parity: 2026-10-07 repair of PR #411

**Current tested source: `ef9fdbecb8895a7137b2ba17127661febf000fc7`.** It is
target `refactor/repository-composition` at
`2d60befbdcdb16ef761638e55b293af9a2353be7` (identical on Forgejo and GitHub when
merged, merge commit `85419e1ac`) plus the BOM repair `ef9fdbecb`. The repair
code itself is `5b1dd08be`. The evidence for this state is in
[ef9fdbecb/](ef9fdbecb/) and the section "Requalification at `ef9fdbecb`" below.
It is added by a later evidence-only commit; `git diff ef9fdbecb <final PR head>`
changes files under `docs/evidence/repository/2026-10-07-scoped-transport-conformance/` only.

The earlier qualification at `c01ca4e117d81c71b7c4de4d5c37c6b10cd8296e` (target
`d91db10b4`) is kept below with its archives in this directory; those archives
are results for `c01ca4e11`, not for the current head. Earlier results in the
parent directory belong to earlier sources: see [../README.md](../README.md)
(coordinator integration, `1b9cdd8a8`) and
[../HISTORICAL-AGENT-RUN.md](../HISTORICAL-AGENT-RUN.md) (original agent run).

## Requalification at `ef9fdbecb`

Hosted `build (25)` on the previous head `5b31ac3cd` failed in
`:bom:checkBomCompleteness` (`bom/build.gradle:234`) before any tests ran
(https://github.com/ai-pipestream/protomolt/actions/runs/37594308528/job/112703043363).
The same failure reproduced locally on the target itself: four published
repository modules (`protomolt-repo-admission`, `protomolt-repo-history-grpc`,
`protomolt-repo-publication-grpc`, `protomolt-repo-schema-registry`) were not
constrained. Commit `ef9fdbecb` adds them to `supportedModules` next to the other
repository modules, so they appear in both the Maven BOM constraints and the
consumer catalog. No exemption was added and no check was changed.

| Run | Command | Exit | Wall | Result |
| --- | --- | --- | --- | --- |
| parity-1 | `./gradlew :protomolt-repo-container:scopedPublicationTest --max-workers=2 --console=plain --rerun-tasks` | 0 | 78 s | 1 test, 0 fail/err/skip, 66.2 s; 44/44 markers; run `02c61dcd-…`; 174 tasks executed |
| parity-2 | same | 0 | 78 s | 1 test, 0 fail/err/skip, 66.5 s; 44/44 markers; run `5944ab01-…`; 174 tasks executed |
| regressions | focused `:protomolt-repo-container:test` (five suites, command as below) | 0 | 63 s | 67 tests (7 + 16 + 7 + 5 + 32), 0 fail/err/skip |
| admission-storage | `./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain` | 0 | 7 m 44 s | 1 test, 0 fail/err/skip |
| bom | `./gradlew :bom:checkBomCompleteness :bom:checkConsumerCatalog --max-workers=2 --console=plain --rerun-tasks` | 0 | 2 s | both tasks executed |
| consumer-stage | `./gradlew -I gradle/toolkit-consumer.init.gradle -PtoolkitRepository=<tmp> -PpublishVersion=0.1.0-consumer-candidate-SNAPSHOT stageToolkitConsumer` | 0 | 1 s | 48 tasks executed |
| consumer-run | `./gradlew -p examples/protobuf-toolkit -PtoolkitRepository=<tmp> -PprotomoltVersion=0.1.0-consumer-candidate-SNAPSHOT run --refresh-dependencies` | 0 | 2 s | consumer ran |

Each run was archived before the next started, as `ef9fdbecb/<run>.tar.gz` with
command, `result.txt` (exit, duration, source SHA), Gradle output, JUnit XML and,
for parity, the persisted log, plus a `SHA256SUMS` manifest. Probe log SHA-256:
parity-1 `6d401b910411d9b8073653c1d60aef40facede445981afa2803724fe638aced9`
(XML `78e04ce03007c48e26fc2139ddba93a89a812a00c513dc22519a4642e3e1c50e`), parity-2
`dbdc9218e699fd92851ef6e0c7c360e02a336e09bedb81026387a49a889a641c`
(XML `d3692e694758f98bf235c778c78d196aaeebae64efc9195737ad5a69499b98c3`).
`ef9fdbecb/source-sha256.txt` fingerprints the four changed files. The
`DocumentHistoricalTransportProbe.java:287` failure described below did not recur
in this run. The `c01ca4e11` archives were re-extracted and their `SHA256SUMS`
and `source-sha256.txt` entries verified unchanged.

## Earlier qualification at `c01ca4e11`

### What the repair changes

All changes are test-side: `ScopedPublicationProbe.java`,
`ScopedPublicationParityIT.java` and one system property on the
`scopedPublicationTest` task. The coordinator's `ObservedStore` adapter-call
observations, positive controls, refusal and exact-retry snapshots, the narrow
`**/ScopedPublicationParityIT*` default-test exclusion, the `check` dependency and
the JUnit marker printing are unchanged.

1. **Exact identity at the repository boundary.** Previously
   `requireScopedCallersOnly` only checked that a binding was present. Now every
   `Host.publish` invocation, on both paths, requires that exactly one repository
   call for that operation arrived after the invocation started, carrying the
   expected principal, no process authority, and the full binding
   (issuer, credential ID, generation) of the key provisioned or rotated for THAT
   invocation. The expectation is the `ScopedKey` returned by
   `RepositoryCredentialAuthorities.register`/`rotate`, never the transport mapper
   output. The deliberate unbound case expects no binding; same-principal
   different-key and rotation cases expect their own incoming key, not the
   original grant's key. Refused invocations are checked too. `requireScopedCallersOnly`
   now also requires every observed call to have been matched exactly.
2. **Substitution negative (`IDENTITY_SUBSTITUTION`, both paths).** Key A's
   invocation is made to present key B's binding (the library host builds the
   wrong caller; over gRPC the token resolves to key B). Key B holds the grant, so
   the repository accepts and commits: the call carries a binding, so a
   presence-only check would pass. The exact check raises `IdentityMismatch`. Against
   an actually observed caller, a wrong issuer, credential ID, generation,
   principal, process authority or unbound expectation each fails. Over gRPC, a
   host mapper that replaces the authenticated binding is refused
   `PERMISSION_DENIED` by the production adapter before the repository, with no
   adapter write calls.
3. **Persisted probe log.** The log lives at
   `repo/container/build/test-results/scopedPublicationTest/scoped-publication.log`
   (`protomolt.test.scopedPublicationLog`, set only on this task). The driver
   deletes it before each run, passes a fresh run identifier that the probe must
   echo (`SCOPED_PUBLICATION_RUN <uuid>`), and writes compilation or launch
   failures there. Process timeout, forced termination, exit code, the 1 MiB size
   check and every marker assertion are unchanged.
4. **Labels.** Assertion messages about `recordedVersions` now say "recorded
   versions", not provider effects. The raw historical content read after READ
   revocation is labelled as a library read on both paths.

## Results at `c01ca4e11` (local host `krick`, superseded by the requalification above)

| Run | Command | Exit | Wall | JUnit | Probe |
| --- | --- | --- | --- | --- | --- |
| parity-1 | `./gradlew :protomolt-repo-container:scopedPublicationTest --max-workers=2 --console=plain --rerun-tasks` | 0 | 78 s | 1 test, 0 fail/err/skip, 65.6 s | 44/44 markers, run `ed7bec2c-…` |
| parity-2 | same | 0 | 80 s | 1 test, 0 fail/err/skip, 67.3 s | 44/44 markers, run `5b14c630-…` |
| regressions | `./gradlew :protomolt-repo-container:test --max-workers=2 --console=plain --tests '*RepositoryCredentialAuthoritiesIT' --tests '*RepositoryCreationGrantsIT' --tests '*DocumentScopedRegistrationIT' --tests '*ScopedRepositorySuccessorIT' --tests '*DocumentPublicationCommitIT'` | 0 | 62 s | 67 tests (7 + 16 + 7 + 5 + 32), 0 fail/err/skip | n/a |
| admission-storage | `./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain` | 0 | 7 m 45 s | 1 test, 0 fail/err/skip | n/a |

Each run was archived before the next one started: `parity-1.tar.gz`,
`parity-2.tar.gz`, `regressions.tar.gz` and `admission-storage.tar.gz` hold the
command, `result.txt` (exit, duration, source SHA), the Gradle output, the JUnit
XML and, for parity, the probe log, each with a `SHA256SUMS` manifest. Both parity
runs executed all 174 tasks; the regression and storage tasks executed fresh
(new XML timestamps). Probe log SHA-256: parity-1
`50b307638bc975a9924bc013482b02c341e68e49a244d6b95345ee283942ddc2`, parity-2
`1ea8f5776cefe326fe4f388677fa1f8258061afb2916840ac99c0c40cb21ad99`.
`source-sha256.txt` fingerprints the three changed files at the tested commit.

`pre-merge-5b1dd08be.tar.gz` keeps the first qualification at `5b1dd08be`
(before target `d91db10b4` was merged): both parity runs and admissionStorageTest
passed, and the first focused-regression run had one failure in an untouched test:
`DocumentPublicationCommitIT.hostRuntimePublishesAndReplaysThenDrainsRealProviderResources[2]`
at `DocumentHistoricalTransportProbe.java:287` (`responseBudget.reservedBytes()`
was 2340, not 0, right after gRPC channel/server shutdown). An immediate fresh
rerun of the same command passed 67/67, as did the run at `c01ca4e11`. This
looks like an intermittent release-timing race in the existing historical-read
transport probe, which this repair does not touch; it is reported, not fixed.

## Markers and what they mean

44 scenario markers are printed and checked against one JUnit harness test: the
original 42 (17 shared scenarios and 3 final-check orderings per path, transport
fail-closed, parity sentinel) plus `IDENTITY_SUBSTITUTION_LIBRARY` and
`IDENTITY_SUBSTITUTION_GRPC`. A marker prints only after its scenario's assertions
return; the count alone does not show what each scenario proves.

## Limits

- gRPC here means an authenticated **in-process** server and channel with the
  production `ApiTokenServerInterceptor` and `DocumentPublicationGrpcService`.
  This suite does not exercise Netty or the network; loopback Netty evidence for
  this adapter is in `../../2026-10-06-publication-transport`.
- Tokens are synthetic and the resolver is fixture code. Production
  authentication provisioning and external identity-provider revocation are out
  of scope.
- The held-upload gate pauses before delegating to the SDK. It is not proof of
  an already in-flight network PUT, and its timings are not a benchmark.
- Receipt replay is exercised over each path. The post-commit raw historical
  content refusal is checked through the library on both paths; no
  content-delivery transport is claimed.
- `ObservedStore` counts adapter calls and normal returns, not HTTP retries, and
  does not prove durability on its own. Recorded SQL versions are a separate
  measurement.
- Local validation only, on one host. Hosted CI, review, merge and deployment
  are reported separately in the PR, not here.
