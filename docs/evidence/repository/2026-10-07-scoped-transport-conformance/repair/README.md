# Scoped publication parity: 2026-10-07 repair of PR #411

**Current tested source: `9f7d5a3557b6e19d866fe8bf0de374b78ad194c6`.** It is
target `refactor/repository-composition` at
`775e2814a1751f2667598095856fd6a99bf668b0` (identical on Forgejo and GitHub when
merged, merge commit `8f215a8a1`) plus the BOM repair `ef9fdbecb`, the
document-platform fixture repair `e26b6b1f9` and the intake credential repair
`9f7d5a355`. The scoped publication repair code is `5b1dd08be`. Evidence for this
state is in [9f7d5a355/](9f7d5a355/) and the next section. It is added by a later
evidence-only commit; `git diff 9f7d5a355 <final PR head>` changes files under
`docs/evidence/repository/2026-10-07-scoped-transport-conformance/` only.

Earlier qualifications are kept below with their archives: `ef9fdbecb` in
[ef9fdbecb/](ef9fdbecb/) and `c01ca4e11` in this directory. Those archives are
results for those commits, not for the current head. Earlier results in the
parent directory belong to earlier sources: see [../README.md](../README.md)
(coordinator integration, `1b9cdd8a8`) and
[../HISTORICAL-AGENT-RUN.md](../HISTORICAL-AGENT-RUN.md) (original agent run).

## Qualification at `9f7d5a355`

### Document-platform failures and repairs

Hosted `build (25)` on `570025073`
(https://github.com/ai-pipestream/protomolt/actions/runs/37599544882/job/112720264651)
passed the BOM check and then failed `:protomolt-document-platform:test`, 58 tests
with 4 failures. Locally, at merge `8f215a8a1` before any change,
`./gradlew :protomolt-document-platform:test --max-workers=2 --console=plain --rerun-tasks`
gave the same result in 48 s (`9f7d5a355/docplat-before.tar.gz`). The four failures
were DocumentPlatformSmokeIT, PlatformRoleNodeIT and RoleNodeIT at initialization,
and PlatformSnapshotIT.aFreshIndexDirectoryRestoresFromTheBucketOnBoot. Each one
failed with `ComposerException: node boot failed: Repository network transport
requires a nonblank API token (PROTOMOLT_API_TOKEN)` from `RepoServiceModule.wire`,
which target commit `6bc32d582` made mandatory.

1. **Fixtures (`e26b6b1f9`, test sources only).** Every node that mounts the
   repository role now gets an explicit synthetic operator token through its node
   environment, and each client presents it as `api_token`. That covers the smoke
   node, both snapshot writer boots and the reader, the PlatformRoleNodeIT repo
   and intake nodes, and the RoleNodeIT composer repo node and intake opener. Once
   the token is set the smoke node's search console is guarded, so the fixture
   adds an access-policy principal (search-query, schema-read, service-invoke),
   logs it in through `POST /session`, and sends the session cookie. No assertion
   was removed. The token-configuration and access-policy tests are unchanged and
   pass. With only this change the module ran 69 tests with 2 failures
   (`9f7d5a355/docplat-after-fixtures.tar.gz`, run on the `e26b6b1f9` tree before
   it was committed).
2. **Intake credential (`9f7d5a355`, production; Kristian approved the change).**
   Both remaining failures were `UNAUTHENTICATED: Missing API token 'api_token'` on
   cross-node ingest. `IntakeModule` passed only the repo target string, and
   `IntakeServices` opened its own plaintext channel, so a remote intake node
   presented no credential. `IntakeModule` now passes
   `context.channels().to("repo")`, which is opened by the node's remote opener
   with the node's token. `IntakeServices` borrows that channel and does not close
   it. The standalone `IntakeServiceMain` path is unchanged.

### Results

| Run | Command | Exit | Wall | Result |
| --- | --- | --- | --- | --- |
| parity-1 | `./gradlew :protomolt-repo-container:scopedPublicationTest --max-workers=2 --console=plain --rerun-tasks` | 0 | 80 s | 1 test, 0 fail/err/skip; 44/44 markers; run `dc94528c-…`; 174 tasks executed |
| parity-2 | same | 0 | 79 s | 1 test, 0 fail/err/skip; 44/44 markers; run `549dd51f-…`; 174 tasks executed |
| regressions | focused `:protomolt-repo-container:test` (five suites) | 0 | 71 s | 67 tests, 0 fail/err/skip |
| admission-storage | `./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain` | 0 | 7 m 49 s | 1 test, 0 fail/err/skip |
| bom | `./gradlew :bom:checkBomCompleteness :bom:checkConsumerCatalog --max-workers=2 --console=plain --rerun-tasks` | 0 | 2 s | both executed |
| consumer-stage / consumer-run | candidate toolkit consumer, as in CI | 0 / 0 | 2 s / 1 s | consumer ran |
| document-platform | `./gradlew :protomolt-document-platform:test --max-workers=2 --console=plain --rerun-tasks` | 0 | 53 s | 69 tests in 17 suites, 0 fail/err/skip; 360 tasks executed |
| intake-service | `./gradlew :protomolt-intake-service:test --max-workers=2 --console=plain --rerun-tasks` | 0 | 27 s | 61 tests in 8 suites, 0 fail/err/skip |

Each run was archived before the next one started, as `9f7d5a355/<run>.tar.gz`
with a `SHA256SUMS` manifest. The parity runs include the persisted log: parity-1
`8b3f9d3634d5c67a644bf91ad3f0c6dfb4674435b44b861214a79819360bd1ed` (XML
`723f19395eeee45bb09bcfcc4dec76aa1de7e2a157333361898b58c2f8198b79`), parity-2
`f26df6c92d62ee25b7843d60d590cfdaaf78537afa374fc5564812ed86e6a8b8` (XML
`78a9d091aac77ff831df87c123959f9ede6601bfbcbfd66ea77fdcd9647d06f3`).
`9f7d5a355/source-sha256.txt` fingerprints every non-evidence file this PR changes
relative to the target. The `DocumentHistoricalTransportProbe.java:287` failure
described below did not recur.

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
