# Building and testing

## Prerequisites

- JDK 21 or newer. The build uses Gradle toolchains (with the Foojay
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
ecosystem — packages are version-suffixed (`…v1`) and enums use prefixed
value names (`INDEX_FIELD_TYPE_*`). Configuration lives in
[`buf.yaml`](../buf.yaml). Code generation itself remains plain `protoc` /
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
ccompat facade and a genuine Confluent Schema Registry.

```shell
docker compose -f docker-compose.integration.yml up -d

./gradlew :protomolt-schema-apicurio:test \
          :protomolt-schema-confluent:test
```

When a registry is not reachable (a quick ~2 second probe), the tests skip
via JUnit assumptions, so a plain `./gradlew build` stays green without
containers. Test artifacts and subjects use unique per-run names, so reruns
against a long-lived registry never collide.

Default endpoints match `docker-compose.integration.yml` and can be
overridden with system properties or environment variables:

| Property | Environment variable | Default |
|---|---|---|
| `pipestream.it.apicurio.url` | `PIPESTREAM_IT_APICURIO_URL` | `http://localhost:18780` |
| `pipestream.it.confluent.url` | `PIPESTREAM_IT_CONFLUENT_URL` | `http://localhost:18781` |

```shell
./gradlew :protomolt-schema-apicurio:test \
          -Dpipestream.it.apicurio.url=http://my-registry:8080
```

## Conformance against buf's own runner

Beyond the in-build gate, the conformance executor can be scored by buf's
`protovalidate-conformance` binary; see
[Validation](validation.md#conformance-harness).

## Continuous integration

GitHub Actions (`.github/workflows/ci.yml`) builds and tests on every push
to `main` and on pull requests, runs `buf lint`, and runs `buf breaking`
against the target branch for pull requests.
