# Framework integrations

The `host/integrations/` modules wire ProtoMolt into a dependency-injection
container. They provide beans, not HTTP endpoints — the HTTP hosts live in
`host/servers/` (see [REST gateway and servers](rest-gateway.md)). A Spring
application typically pairs `protomolt-spring` with
`protomolt-server-spring`; the same split applies to Quarkus.

## Spring Boot (`protomolt-spring`)

`ProtoToolsAutoConfiguration` is a standard auto-configuration (registered
via `AutoConfiguration.imports`). It provides, each guarded by
`@ConditionalOnMissingBean` so an application bean always wins:

- `DescriptorRegistry`, preloaded with the `GoogleDescriptorLoader`
  (classpath descriptor sets) and `ClasspathDescriptorLoader`, plus every
  application-defined `DescriptorLoader` bean — defining a
  `GatheringDescriptorLoader` or registry-loader bean is all it takes to add
  a descriptor source
- `ProtoFieldMapper`, and a `CelEvaluator` when CEL is on the classpath
- `ProtobufJsonTranscoder`
- `ProtoRestMethodRegistry`, `ProtoApiTokenValidator`, and
  `ProtoRestGateway`

The default token validator rejects all tokens; define a
`ProtoApiTokenValidator` bean to accept real credentials. The Spring MVC
host reads its mount point from
`pipestream.proto.rest.path-prefix` (default `/grpc-json`).

## Quarkus (`protomolt-quarkus`)

The Quarkus extension (feature name `protomolt`, runtime + deployment
modules) provides the same object graph as CDI `@DefaultBean` producers, so
any application-scoped bean of the same type overrides a default cleanly.
The producer also aggregates every `DescriptorLoader` bean in the container
into the registry — contributing a new descriptor source is just producing
a bean.

The Apicurio module ships Quarkus wiring of its own, configured under
`pipestream.proto.apicurio.*`:

```properties
pipestream.proto.apicurio.enabled=true
pipestream.proto.apicurio.registry-url=http://localhost:8080/apis/registry/v3
pipestream.proto.apicurio.group-id=default
pipestream.proto.apicurio.auto-load-on-startup=false
```

## Micronaut and others

Micronaut has a server facade (`protomolt-server-micronaut`) but no DI
module; construct the gateway objects directly (they are plain constructors
throughout) or wrap them in your own factories. The same approach works for
any container.
