# Published repository library consumer

Run from the repository root. These commands publish only into a filesystem
repository; they do not release artifacts remotely.

```sh
./gradlew -I gradle/repository-consumer.init.gradle \
  -PrepositoryConsumerRepository=/tmp/protomolt-repository-maven \
  -PpublishVersion=0.1.0-repository-boundary-proof stageRepositoryConsumer \
  --max-workers=2 --console=plain
./gradlew -p verification/repository-consumer \
  -PrepositoryConsumerRepository=/tmp/protomolt-repository-maven \
  -PcandidateVersion=0.1.0-repository-boundary-proof check \
  --max-workers=2 --console=plain
./gradlew -p verification/repository-consumer \
  -PrepositoryConsumerRepository=/tmp/protomolt-repository-maven \
  -PcandidateVersion=0.1.0-repository-boundary-proof -PpomOnly check \
  --max-workers=2 --console=plain
```

Use a unique candidate version and destination when comparing different
revisions, and never point two runs at the same staging directory. The second
consumer invocation resolves Maven POM metadata with Gradle metadata
redirection disabled. Neither run substitutes project dependencies.

## Metadata mode isolation

Each metadata mode builds into its own directory (`build-gradle-module` or
`build-pom-only`), so compilation and resolution outputs from one mode can
never be reused as an up-to-date success for the other. `--rerun-tasks` is not
required to switch modes; same-mode up-to-date reuse remains legitimate.

## What `check` gates

- `verifyConsumerBoundaries` classifies every consumer row by group:artifact
  identity and rejects forbidden dependency families: SQL/ORM/pooling/migration,
  Kafka, repository server/engine modules, and AWS, Azure or Redis SDKs. Rows:
  byte SPI alone (additionally pinned JDK-only), repository SPI alone, codec
  alone, SPI+codec, S3 provider alone (AWS SDK allowed), Redis provider alone
  (Jedis allowed), cache alone, cache with an explicitly selected Redis
  provider, blob gRPC client alone, publication gRPC client alone, and each
  probe source set. Legitimate protobuf/gRPC client libraries are not flagged.
- `verifyConsumer`, `verifyProviderDiscovery` and `verifyRedisPublishedConsumer`
  launch probes from the resolved published classpaths. The discovery probe
  asserts ServiceLoader finds exactly the S3 and Redis factories from the
  staged JARs: packaged service entries, no-I/O backend identities, explicit
  capability sets, unknown-provider and unsupported-capability refusals, and
  close semantics. The Redis probe proves a non-S3 consumer starts and executes
  real conditional operations against a real Redis container (started through
  the docker CLI, not a classpath dependency) with AWS and Azure SDK classes
  absent from the classpath.
- `verifyForbiddenDependencyDetection` stages a disposable negative publication
  (group `ai.pipestream.negative`, under the mode-specific build directory)
  with an injected forbidden runtime dependency and asserts the boundary
  classifier detects it. The staged candidate repository is never contaminated.
- `verifyBrokenServiceMissing` and `verifyBrokenServiceWrongClass` repackage the
  published Redis provider JAR into deliberately corrupted fixtures (service
  registration removed; registration naming a class that does not exist) and
  assert discovery fails explicitly. Fixtures carry `DELIBERATE-TEST-FIXTURE`
  markers and are test inputs only.
- `inspectPublishedMetadata` parses the staged POMs, asserts their exact
  dependency sets and scopes, and requires the Gradle module metadata marker.
- `recordConsumerMatrix` and `recordStagedArtifactHashes` write the declared
  and resolved module inventories and staged artifact hashes under
  `build-<mode>/reports/` for evidence capture.

The independent build also compiles migrated Java imports, checks typed
assembly and conditional-write alternatives, and rejects storage/database
implementations in the resolved production graph.

The separate `publicationClient` source set depends only on the published
`protomolt-repo-publication-grpc` artifact. It compiles client construction and the
shared publication method without dependencies from the other consumer source
sets. Its resolved runtime rejects repository server/engine code, SQL, Kafka and
AWS, Azure or Redis storage SDKs. Both metadata modes apply that check. This is a
packaging check; real transport behavior is tested in the packaged storage suite.
