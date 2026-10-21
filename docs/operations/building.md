# Building and testing

## Prerequisites

This is the authoritative prerequisites list; README and CONTRIBUTING defer
to it.

- **JDK 25, the only supported runtime.** The build pins a Java 25 toolchain
  (`JavaLanguageVersion.of(25)` in `build.gradle`) and compiles with
  `--release 25`; the Foojay resolver downloads a 25 toolchain when the
  launching JDK is something else. The Gradle 9.6.1 wrapper itself needs
  JDK 17 or newer to start.
- **[buf](https://buf.build/docs/installation) 1.70.0** on the `PATH` for
  proto linting. CI pins 1.70.0 because the bundled well-known descriptors
  vary by CLI version. `buf lint` runs standalone or through the root
  `./gradlew check` task; `./gradlew build` does not run it.
- **Node 22 or newer**, only for the browser console. `apps/console` is a
  standalone Vite app; `apps/serve` bundles `apps/console/dist` only when
  it exists, so `/console` serves nothing until `npm ci && npm run build`
  has run in `apps/console`. The JVM build does not need Node.
- **Docker**, for the Testcontainers integration suites (Redpanda,
  Apicurio, OpenSearch, Iceberg, LocalStack). Test classes named `*IT` or
  `*IntegrationTest` run in each module's `integrationTest` task, not in
  `test`; `check`, and with it `build`, depends on `integrationTest`. A
  suite that needs Docker fails without it rather than skipping.

## Building from a clone

```shell
git clone https://github.com/ai-pipestream/protomolt.git
cd protomolt
./gradlew build
```

`build` compiles all modules, runs every unit test and every integration
suite (`integrationTest`, the `*IT` and `*IntegrationTest` classes), and runs the in-build
protovalidate conformance gate (which fails the build on any drift from the
expected 100% pass rate). It does not run `buf lint`; that hangs off the
root `check` task (see [Proto linting](#proto-linting)).

The catalog inventory is generated from the built-in, standalone MCP, and full
catalog constructors. Refresh the checked-in machine-readable document after
changing action registration:

```shell
./gradlew :protomolt-system-tests:generateActionInventory
```

The system-test inventory check compares that document with the running
catalogs and requires the catalog documentation to link it, so stale names or
manual count claims fail the build.

To consume the artifacts locally:

```shell
./gradlew publishToMavenLocal
```

Versions are derived from `v*` git tags via Axion; an untagged clone
publishes a snapshot version. A specific version can be forced with
`-PpublishVersion=…`. Artifact signing activates only when
`GPG_PRIVATE_KEY` is set, so local publishing needs no signing setup.

## Build cost and the inner loop

`settings.gradle` includes 181 modules, `build` runs the Docker-backed
integration suites, and the conformance gate runs in-build, so
a full `./gradlew build` is a long operation: expect tens of minutes on a
developer laptop, dominated by the test and container time rather than
compilation. The Gradle daemon is capped at `-Xmx2g`
(`gradle.properties`), and each module's tests fork up to half the
machine's cores (`maxParallelForks`).

Day-to-day work targets one module by its Gradle project path (the
`include` name in `settings.gradle`, which differs from the directory
name):

```shell
# Unit tests and javadoc for one module:
./gradlew :protomolt-mapper:test :protomolt-mapper:javadoc --max-workers=2

# Its integration suites (*IT and *IntegrationTest classes; Docker for Testcontainers):
./gradlew :protomolt-repo-service:integrationTest --max-workers=2

# Compile without running tests:
./gradlew :protomolt-mapper:classes --max-workers=2
```

Gradle builds the module's dependencies as needed, so the first run of a
deep module compiles more than later ones. On a small machine keep
`--max-workers` low (2 is a safe default); the module-level test forking
already uses the cores.

## Snapshot dependencies

Everything resolves from Maven Central, including stock
`org.apache.lucene` (version `lucene` in `gradle/libs.versions.toml`), with
one exception: the experimental OpenNLP fork (`ai.pipestream:opennlp-*`,
a `-SNAPSHOT` version in the catalog). `build.gradle` resolves it from
`mavenLocal()` first, then Maven Central's snapshot repository, then the
ai-pipestream Forgejo package registry
(`https://git.rokkon.com/api/packages/ai-pipestream/maven`) as a fallback.
Both remote repositories are content-filtered to exactly the fork, so no
other dependency is ever looked up there.

Two failure modes to know about:

- Central's snapshot storage expires artifacts, so a clean clone can fail
  with `Could not find ai.pipestream:opennlp-...`. That means a fresh fork
  snapshot needs publishing (or the Forgejo fallback is unreachable); it is
  not a fault in your checkout.
- A stale copy in `~/.m2/repository/ai/pipestream` shadows the published
  one, because `mavenLocal()` is consulted first. Clear it and re-resolve:

```shell
rm -rf ~/.m2/repository/ai/pipestream
./gradlew build --refresh-dependencies
```

## Proto linting

All ProtoMolt standard `.proto` files (and their test fixtures) are linted
with buf's `STANDARD` rule set to stay consistent with the wider protobuf
ecosystem: packages are version-suffixed (`…v1`) and enums use prefixed
value names (`INDEX_FIELD_TYPE_*`). Configuration lives in
[`buf.yaml`](../../buf.yaml). Code generation itself remains plain `protoc` /
protobuf-gradle-plugin; buf is a lint-time tool only, not a dependency.

```shell
buf lint            # or: ./gradlew bufLint / check
```

CI additionally runs `buf breaking` against the base branch on pull
requests.

## Integration tests

The schema-registry loader modules include integration tests (JUnit
`@Tag("integration")`) that exercise real registries: Apicurio Registry's
native v3 API, and the Confluent subjects API against both Apicurio's
ccompat facade and a Redpanda container.

The Confluent lanes (the `:protomolt-schema-confluent` Confluent suites and
the `:protomolt-kafka-serde` registry suite) provision their own registry: a
Testcontainers Redpanda, which serves the Confluent Schema Registry API.
They need Docker; without it they fail rather than skip.

The OpenSearch index lane (`:protomolt-search-index-opensearch`) also provisions
its own engine: a Testcontainers container running the
`opensearchproject/opensearch` image whose tag comes from `opensearch-image`
in `gradle/libs.versions.toml` (the compose stack pins the same version),
started through the OpenSearch project's `opensearch-testcontainers`
module. It likewise needs Docker and fails without it, so the compose stack's
`opensearch` service is not needed for tests.

The Iceberg lanes self-provision the same way. The `:protomolt-iceberg`
suites each boot an `apache/iceberg-rest-fixture` catalog (one suite uses
`apache/gravitino-iceberg-rest` instead), the `:protomolt-iceberg-s3` suite
pairs a fixture catalog with a Testcontainers LocalStack S3 store, and the
`:protomolt-kafka-connect-iceberg` suite boots its own fixture catalog. They need
Docker and fail without it, so the compose stack's
`iceberg-rest`, `iceberg-rest-s3`, `rustfs`, and `gravitino-iceberg-rest`
services are not needed for tests.

The S3 store convention is three lanes by environment: LocalStack for
tests, the compose stack's `rustfs` for live runs on a laptop or server,
and real S3 on AWS. The Apicurio lanes self-provision the same way: the
`:protomolt-schema-apicurio` suites and the confluent module's two ccompat
suites each boot an `apicurio/apicurio-registry` container whose tag comes
from the `apicurio` version in `gradle/libs.versions.toml` (the ccompat
suites hit its `/apis/ccompat/v7` facade). They need Docker and fail
without it, so the compose stack's `apicurio` service is not needed for tests:

```shell
./gradlew :protomolt-schema-apicurio:integrationTest \
          :protomolt-schema-confluent:integrationTest
```

Test artifacts and subjects use unique per-run names, so reruns never
collide.

The Apicurio lanes can be retargeted at an external registry, such as the
compose stack's, with a system property or environment variable:

| Property | Environment variable | Default |
|---|---|---|
| `protomolt.it.apicurio.url` | `PROTOMOLT_IT_APICURIO_URL` | the suite's Testcontainers registry |

```shell
./gradlew :protomolt-schema-apicurio:integrationTest \
          -Dprotomolt.it.apicurio.url=http://my-registry:8080
```

An unreachable override endpoint fails the suite rather than skipping, so
a typo in the URL cannot silently hand the run back to the Testcontainers
registry. Without the override the suites provision their own containers.

## ACP protocol round trips

The `acp-protocol`-tagged tests in `protomolt-acp-agent` drive the catalog agent
through a full ACP exchange: two over in-memory pipes, two across a real child
process over stdio. They are excluded from the default `test` task and run as
their own lane:

```bash
./gradlew :protomolt-acp-agent:acpProtocolTest
```

The exclusion is about cost, not flakiness: the process suite forks an agent
JVM per test and both suites shell out to protoc, which is slower than
anything the default test task runs. The agent speaks newline-delimited
JSON-RPC over the first-party, virtual-thread transport in
`protomolt-acp`; there is no SDK and no reactive runtime involved, so the
tests have no timing machinery to trip over. The transport's own tests
(connection behavior and the golden wire-transcript replay) live in
`protomolt-acp` and run in its default `test` task because nothing in
them is slow. CI runs the lane once, serially
(`maxParallelForks = 1`), after the main build. They are kept as a separate
lane rather than deleted because the coverage is real: they are the only
tests that exercise the agent as an IDE actually drives it.

## Conformance against buf's own runner

Beyond the in-build gate, the conformance executor can be scored by buf's
`protovalidate-conformance` binary; see
[Validation](../transform/validation.md#conformance-harness). CI runs this full-suite
scoring on every push, so the pass rate is enforced, not just documented.

## Continuous integration

GitHub Actions (`.github/workflows/ci.yml`) runs four jobs on every push
to `main` and on pull requests:

- **build**: compiles all modules, runs the unit and integration suites and
  the in-build conformance gate (`./gradlew build`), then the ACP protocol
  lane, `buf lint` (buf pinned at 1.70.0), the deployment and starter
  static checks, and (for pull requests) `buf breaking` against the target
  branch. Tests run on JDK 25 (`-PtestJdk=25`), the only supported
  runtime; the matrix form stays so a newer JDK can be added beside it.
- **console**: installs the console's npm dependencies and runs its tests,
  its TypeScript typecheck, and its production build. The console is a
  standalone Vite app that the JVM build does not need, so a broken bundle
  would otherwise reach `main` unnoticed.
- **conformance**: builds the conformance executor and scores it with
  buf's own `protovalidate-conformance` binary against the complete suite;
  any failing case fails the job.
- **integration**: starts `docker-compose.integration.yml` (Apicurio, a
  Confluent Schema Registry backed by Redpanda, Solr, OpenSearch, the Iceberg
  REST and Gravitino catalogs, and rustfs) and runs the integration suites
  against it: `:protomolt-schema-apicurio`, `:protomolt-schema-confluent`,
  `:protomolt-search-index-solr`, `:protomolt-search-index-opensearch`,
  `:protomolt-iceberg`, `:protomolt-iceberg-s3`,
  `:protomolt-kafka-connect-iceberg`, and `:protomolt-kafka-serde`.

## Releasing to Maven Central

Releases are cut from the `Release and Publish` workflow
(`.github/workflows/release-and-publish.yml`), triggered manually with a
`patch`/`minor`/`major` bump. The workflow tags the release with Axion,
runs the full build and test suite against the tagged version, then signs
every module's publication and uploads them to Maven Central as one portal
deployment (the `nmcp` aggregation). Versions are never hand-typed.

Publication requires four credentials, provided as org-level secrets:
`MAVEN_CENTRAL_USERNAME`, `MAVEN_CENTRAL_PASSWORD`, `GPG_PRIVATE_KEY`
(ASCII-armored), and `GPG_PASSPHRASE`. Signing is guarded on their
presence, so local builds and `publishToMavenLocal` never require them.

The deployment bundle can be built and inspected locally without
publishing:

```shell
./gradlew nmcpZipAggregation
unzip -l build/nmcp/zip/aggregation.zip
```

A snapshot variant (`publishAggregationToCentralPortalSnapshots`) exists
but is not wired into CI; Central's snapshot storage is quota-constrained,
so snapshots are published deliberately, not on every merge.
