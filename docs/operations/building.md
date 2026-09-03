# Building and testing

## Prerequisites

- JDK 25 or newer. The build uses Gradle toolchains (with the Foojay
  resolver), so any recent JDK can run the wrapper; artifacts target 21.
- [buf](https://buf.build/docs/installation) on the `PATH` for proto
  linting (`buf lint` runs as part of `check`).
- Docker, only if you want to run the registry integration tests.

## Building from a clone

```shell
git clone https://github.com/ai-pipestream/protomolt.git
cd protomolt
./gradlew build
```

`build` compiles all modules, runs every unit test, runs `buf lint`, and
runs the in-build protovalidate conformance gate (which fails the build on
any drift from the expected 100% pass rate).

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
They run wherever Docker is available and skip otherwise.

The OpenSearch index lane (`:protomolt-search-index-opensearch`) also provisions
its own engine: a Testcontainers container running
`opensearchproject/opensearch:3.7.0` (the same image the compose stack
pins), started through the OpenSearch project's `opensearch-testcontainers`
module. It likewise runs wherever Docker is available and skips otherwise,
so the compose stack's `opensearch` service is not needed for tests.

The Iceberg lanes self-provision the same way. The `:protomolt-iceberg`
suites each boot an `apache/iceberg-rest-fixture` catalog (one suite uses
`apache/gravitino-iceberg-rest` instead), the `:protomolt-iceberg-s3` suite
pairs a fixture catalog with a Testcontainers LocalStack S3 store, and the
`:protomolt-kafka-connect-iceberg` suite boots its own fixture catalog. They run
wherever Docker is available and skip otherwise, so the compose stack's
`iceberg-rest`, `iceberg-rest-s3`, `rustfs`, and `gravitino-iceberg-rest`
services are not needed for tests.

The S3 store convention is three lanes by environment: LocalStack for
tests, the compose stack's `rustfs` for live runs on a laptop or server,
and real S3 on AWS. The Apicurio lanes self-provision the same way: the
`:protomolt-schema-apicurio` suites and the confluent module's two ccompat
suites each boot an `apicurio/apicurio-registry:3.3.0` container (the
ccompat suites hit its `/apis/ccompat/v7` facade). They run wherever
Docker is available and skip otherwise, so the compose stack's `apicurio`
service is not needed for tests:

```shell
./gradlew :protomolt-schema-apicurio:test \
          :protomolt-schema-confluent:test
```

Test artifacts and subjects use unique per-run names, so reruns never
collide.

The Apicurio lanes can be retargeted at an external registry, such as the
compose stack's, with a system property or environment variable:

| Property | Environment variable | Default |
|---|---|---|
| `protomolt.it.apicurio.url` | `PROTOMOLT_IT_APICURIO_URL` | the suite's Testcontainers registry |

```shell
./gradlew :protomolt-schema-apicurio:test \
          -Dpipestream.it.apicurio.url=http://my-registry:8080
```

An unreachable override endpoint still skips via a JUnit assumption (a
quick ~2 second probe), so a plain `./gradlew build` stays green without
containers.

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

- **build**: compiles all modules, runs the unit test suite and the
  in-build conformance gate, runs `buf lint`, and (for pull requests)
  `buf breaking` against the target branch. It runs as a matrix over test
  JDKs 21 and 25 (`-PtestJdk=…`), so both supported runtimes are exercised.
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
