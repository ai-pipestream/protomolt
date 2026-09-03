/**
 * Spring MVC host adapter for the protobuf JSON/REST gateway.
 *
 * <p>{@link ai.protomolt.proto.server.spring.SpringProtoRestController} is a
 * {@code @RestController} that maps the invoke, OpenAPI, and health routes onto an injected
 * {@link ai.protomolt.proto.http.rest.ProtoRestGateway}. Route paths are read from the
 * {@code protomolt.rest.*} properties, defaulting to the same values as
 * {@link ai.protomolt.proto.server.ProtoToolsServerConfig}, so the mount point can be changed
 * without recompiling.
 *
 * <p>Register the controller as a bean alongside a configured gateway; the beans it needs are
 * supplied by the {@code protomolt-integration-spring} auto-configuration. Body size checks and the mapping
 * from gateway exceptions to HTTP status codes come from
 * {@link ai.protomolt.proto.server.ProtoRestHttpSupport}, matching the standalone hosts.
 *
 * <p>See the
 * <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/surface/rest-gateway.md">REST gateway
 * and servers</a> guide.
 */
package ai.protomolt.proto.server.spring;
