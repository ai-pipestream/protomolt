/**
 * Shared configuration and HTTP helpers for the protobuf JSON/REST server hosts.
 *
 * <p>{@link ai.protomolt.proto.server.ProtoRestServerHost} is the contract every
 * {@code servers/*} adapter implements: start, report the bound port, and expose the
 * {@link ai.protomolt.proto.http.rest.ProtoRestGateway} it fronts. Request handling itself belongs to
 * the gateway; a host binds HTTP and nothing more.
 *
 * <p>{@link ai.protomolt.proto.server.ProtoToolsServerConfig} carries the bind address, the REST
 * path prefix, the OpenAPI and health paths, and the request body cap, normalizing each on
 * construction. {@link ai.protomolt.proto.server.ProtoRestHttpSupport} holds the logic the hosts
 * would otherwise duplicate — path parsing, query and header flattening, body size checks, and the
 * mapping from {@link ai.protomolt.proto.http.rest.ProtoRestException} subtypes to HTTP status codes —
 * so that all hosts answer identically for the same request.
 *
 * <p>The concrete hosts live in the sibling modules {@code servers/jdk}, {@code servers/netty},
 * {@code servers/vertx}, {@code servers/quarkus}, {@code servers/spring}, and
 * {@code servers/micronaut}, each depending on this package rather than on one another.
 *
 * <p>See the
 * <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/surface/rest-gateway.md">REST gateway
 * and servers</a> guide for the request contract these hosts share.
 */
package ai.protomolt.proto.server;
