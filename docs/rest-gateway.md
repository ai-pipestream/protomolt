# REST gateway and servers

The HTTP modules expose protobuf services as JSON/REST — including services
whose types are only known at runtime through a schema registry — with
OpenAPI generated from the same registrations. The gateway itself is
framework-agnostic; thin host modules bind it to an HTTP stack.

## JSON transcoding

`ProtobufJsonTranscoder` (`protomolt-json`) converts between protobuf
messages and JSON in both directions. It resolves types through a
`DescriptorRegistry`, so it handles `DynamicMessage`s for types loaded from
a registry as naturally as generated classes, and its type registry follows
the descriptor registry as it grows. Malformed input surfaces as
`MalformedProtobufJsonException`, distinct from internal
`ProtobufJsonException` failures.

## The gateway

`ProtoRestGateway` (`protomolt-rest`) dispatches
`POST /{service}/{method}` calls: it authenticates the request, transcodes
the JSON body to the request message, invokes the registered handler, and
transcodes the response back.

```java
var descriptors = DescriptorRegistry.create();
// descriptors.addLoader(apicurioLoader);  // or a Confluent loader

var methods = new ProtoRestMethodRegistry();
methods.register(ProtoRestMethod.builder("Greeter", "SayHello", req -> /* invoke */ resp)
    .requestType(HelloRequest.class)
    .apiToken(ApiTokenRequirement.apiKeyHeader("api_token"))
    .summary("Say hello")
    .build());

var gateway = new ProtoRestGateway(
    methods,
    new ProtobufJsonTranscoder(descriptors),
    ProtoApiTokenValidator.sharedSecret(System.getenv("API_TOKEN")));

String json = gateway.invoke("Greeter", "SayHello", "{\"name\":\"Ada\"}",
    Map.of("api_token", token), Map.of());
```

Failure modes are typed (`ServiceNotFoundException`,
`MethodNotFoundException`, `HttpMethodNotAllowedException`,
`RequestTooLargeException`, unauthorized variants), which the hosts map to
appropriate status codes.

### Authentication

`@ProtoApiToken` / `ApiTokenRequirement` declare per-method token
requirements (header or query, API-key or HTTP bearer);
`ProtoApiTokenValidator` checks presented tokens. The gateway fails closed:
without an explicitly supplied validator, tokens are rejected. Use
`ProtoApiTokenValidator.sharedSecret(...)` or provide your own
implementation.

### Annotation-driven registration

For services defined as plain classes, annotate the methods and register the
bean:

```java
@ProtoRestExposed(path = "/echo", summary = "Echo a message")
public EchoResponse echo(EchoRequest request) { ... }
```

```java
new ProtoRestAnnotationRegistrar(methods).register(new EchoService());
```

`@ProtoRestExposed` marks methods for exposure (path, HTTP verbs, summary);
a type-level annotation supplies defaults for the class.

## OpenAPI

`ProtoOpenApiGenerator` (`protomolt-openapi`) builds an OpenAPI 3.x
document from the method registry and descriptors — schemas, security
schemes from the token requirements, and summaries from the registrations:

```java
String openApi = new ProtoOpenApiGenerator().generateJson(methods);
```

Every server host serves this document at `GET /openapi.json`.

## Server hosts

All hosts implement `ProtoRestServerHost` (`protomolt-server-common`) and
share behavior and configuration: `POST {prefix}/{service}/{method}` (prefix
defaults to `/grpc-json`), `GET /openapi.json`, `GET /health`, a request
body limit (16 MiB by default, 413 above it), 405 with an `Allow` header on
wrong verbs, and generic 5xx bodies that never leak internal exception
detail. `ProtoToolsServerConfig` carries host, port, path prefixes, and
limits.

| Artifact | Stack | Notes |
|---|---|---|
| `protomolt-server-jdk` | JDK `HttpServer` | Default; no extra HTTP dependencies, virtual-thread dispatch |
| `protomolt-server-vertx` | Vert.x 5 | Standalone Vert.x (Quarkus 3.x is still on Vert.x 4) |
| `protomolt-server-netty` | Netty 4.2 | `HttpServerCodec` + aggregator pipeline |
| `protomolt-server-spring` | Spring MVC | Controller mounted at `${pipestream.proto.rest.path-prefix:/grpc-json}` |
| `protomolt-server-micronaut` | Micronaut | Facade; annotation wiring lives in the application |
| `protomolt-server-quarkus` | JAX-RS / RESTEasy | Until Quarkus moves to Vert.x 5 |

```java
var server = new JdkProtoRestServer(config, gateway);
server.start();
```

### Trying it from a clone

```shell
./gradlew :samples:runJsonRestServer
curl -H 'api_token: secret' -H 'content-type: application/json' \
  -d '{"name":"Ada"}' http://127.0.0.1:8080/grpc-json/Echo/echo
```

The sample (`samples/`) boots the JDK host with an annotated `EchoService`.

For dependency-injection wiring of the gateway, transcoder, and registries
in Spring and Quarkus applications, see
[Framework integrations](framework-integrations.md).
