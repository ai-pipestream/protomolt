# host/

Embedding the toolkit into servers and frameworks: the role-composition
engine, distributed configuration, the JSON/REST server hosts, and the
Spring and Quarkus integrations.

| Gradle project | Directory | What it is |
|---|---|---|
| `:protomolt-composer` | `host/composer` | The role engine: the `ServiceModule` SPI, two-phase lifecycle, and the channel pivot between co-mounted and remote roles |
| `:protomolt-config` | `host/config` | Typed protobuf config documents behind a pluggable `ConfigSource`, applied verify-then-swap |
| `:protomolt-config-registry` | `host/config-registry` | Config documents read from the git-backed registry; the commit is the version |
| `:protomolt-config-kafka` | `host/config-kafka` | Config documents read from a compacted Kafka topic through the house serde |
| `:protomolt-server-common` | `host/server/common` | Shared host abstraction, config record and HTTP support |
| `:protomolt-server-jdk` | `host/server/jdk` | `com.sun.net.httpserver` host on virtual threads |
| `:protomolt-server-vertx` | `host/server/vertx` | Vert.x 5 host |
| `:protomolt-server-netty` | `host/server/netty` | Netty host with a virtual-thread invoker pool |
| `:protomolt-server-spring` | `host/server/spring` | Spring MVC facade over the gateway |
| `:protomolt-server-micronaut` | `host/server/micronaut` | Plain facade for Micronaut users to wire themselves |
| `:protomolt-server-quarkus` | `host/server/quarkus` | CDI bean fronted by JAX-RS resources |
| `:protomolt-integration-spring` | `host/integration/spring` | Spring Boot 3 auto-configuration for the toolkit beans |
| `:protomolt-integration-quarkus` | `host/integration/quarkus/runtime` | Quarkus extension runtime producing the same bean set |
| `:protomolt-integration-quarkus-deployment` | `host/integration/quarkus/deployment` | The Quarkus build-time half of that extension |

There is no `docs/host` chapter; the closest guides are
[role nodes](../docs/apps/role-nodes.md),
[distributed config](../docs/core/config.md),
[REST gateway](../docs/surface/rest-gateway.md) and
[framework integrations](../docs/surface/framework-integrations.md).
