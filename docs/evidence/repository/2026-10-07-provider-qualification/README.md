# Repository provider qualification evidence, 2026-10-07

Tested revision: branch `agent/repository-provider-qualification`, commits
`21ab678f3` (consumer verification) and `abba0c2b9` (provider qualification),
base `4db6d6c35bc227658e2341c750e9248a8aee3041`. The final runs below executed
at `abba0c2b9` with `git status --porcelain` clean except the untracked
qualification documents being written at the time; every tested source file
was committed. No uncommitted file influenced any result.

## Commands and exit statuses

Staging and published-consumer verification (unique staging directory and
candidate version for this run):

```sh
./gradlew -I gradle/repository-consumer.init.gradle \
  -PrepositoryConsumerRepository=/tmp/protomolt-provider-qualification-maven-abba0c2b9 \
  -PpublishVersion=0.1.0-provider-qualification-abba0c2b9 stageRepositoryConsumer \
  --max-workers=2 --console=plain        # exit 0
./gradlew -p verification/repository-consumer \
  -PrepositoryConsumerRepository=/tmp/protomolt-provider-qualification-maven-abba0c2b9 \
  -PcandidateVersion=0.1.0-provider-qualification-abba0c2b9 check \
  --max-workers=2 --console=plain        # exit 0 (Gradle module metadata mode)
./gradlew -p verification/repository-consumer \
  -PrepositoryConsumerRepository=/tmp/protomolt-provider-qualification-maven-abba0c2b9 \
  -PcandidateVersion=0.1.0-provider-qualification-abba0c2b9 -PpomOnly check \
  --max-workers=2 --console=plain        # exit 0 (Maven-POM-only mode)
```

Full log: `final-consumer.log`. Both modes compiled and resolved independently
(`build-gradle-module`, `build-pom-only`); every probe printed its OK line in
both modes, and both negative fixtures were detected as expected in both modes.

Owned-module test gate (all tests re-executed at the tested revision):

```sh
./gradlew :protomolt-repo-blob-spi:test :protomolt-repo-blob-s3:test \
  :protomolt-repo-blob-redis:test :protomolt-repo-blob-cache:test \
  :protomolt-repo-blob-grpc:test :protomolt-repo-spi:test \
  :protomolt-repo-codec:test :protomolt-repo-publication-grpc:test \
  --rerun-tasks --max-workers=2 --console=plain   # exit 0
```

Log: `final-module-tests.log`. JUnit totals in `test-totals.txt`: 241 tests,
0 failures, 0 errors, 0 skipped across the seven tested modules;
`repo/publication/grpc` has no test sources (its `test` task is a no-op).

## Recorded artifacts

- `consumer-matrix/gradle-module/` and `consumer-matrix/pom-only/`: declared
  and resolved group:artifact:version inventories for all fifteen consumer
  rows, recorded by the `recordConsumerMatrix` task. The two modes resolve
  identical graphs for every row (verified by diffing the inventories after
  removing the `metadata-mode` header line; no differences).
- `published-metadata/pom-dependencies.txt`: staged POM dependency sets for
  the eight consumer modules; the byte SPI POM has zero dependencies and every
  module carries the Gradle metadata marker plus a `.module` file.
- `staged-artifacts/sha256.txt`: SHA-256 of every staged jar, POM and module
  file (sources/javadoc jars excluded).
- `toolchain.txt`: JDK 25.0.3 (GraalVM CE 25.1.3), Gradle 9.6.1, Docker
  29.8.1, and the digests of the container images used:
  `redis:7-alpine` (`sha256:e7723ff7…`),
  `localstack/localstack:3.8` (`sha256:b279c01f…`),
  `rustfs/rustfs:1.0.0-beta.11-preview.1` (`sha256:ea50257b…`).

## Key measured findings

- LocalStack 3.8 enforces `If-None-Match: *` (duplicate creates conflict;
  eight-way create race has exactly one winner) but does **not** enforce
  `If-Match` (a stale precondition succeeds). Matching conditional writes are
  not qualified on LocalStack 3.8; the qualification stays on the
  deployment-pinned RustFS image. The behavior is pinned by the
  `localstack38DoesNotEnforceMatchingPreconditions` probe as an upgrade
  detector.
- A non-S3 published consumer (byte SPI + Redis provider JARs only) starts and
  executes real conditional operations against a real Redis container with
  AWS and Azure SDK classes absent from the classpath.
- The boundary classifier detects an injected forbidden runtime dependency in
  a disposable negative publication, and discovery fails explicitly for
  deliberately corrupted service registrations. Fixtures live under the
  mode-specific build directory and never touch the staged candidate
  repository.

See `docs/design/repository-provider-qualification.md` for the full matrix,
entry points, lifecycle findings and requirement status. Historical evidence
under `docs/evidence/repository/2026-10-06-publication-consumer/` retains its
original scope and was superseded by re-execution at this revision.

A later independent review ran these gates again and fixed several workarounds;
see [`review-followup/`](review-followup/README.md).
