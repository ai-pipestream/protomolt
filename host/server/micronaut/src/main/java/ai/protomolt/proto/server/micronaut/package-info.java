/**
 * Micronaut facade over the protobuf JSON/REST gateway.
 *
 * <p>{@link ai.protomolt.proto.server.micronaut.MicronautProtoRestFacade} produces the invoke,
 * OpenAPI, and health responses for a Micronaut application to serve. The controller annotations
 * and route declarations stay in the application, which keeps this module free of a compile-time
 * dependency on Micronaut; the facade returns each response as a
 * {@code MicronautProtoRestFacade.Result} carrying status, body, and headers.
 *
 * <p>Request handling is delegated to {@link ai.protomolt.proto.http.rest.ProtoRestGateway}, and body
 * size checks, header flattening, and status mapping come from
 * {@link ai.protomolt.proto.server.ProtoRestHttpSupport}, so responses match the standalone hosts.
 *
 * <p>See the
 * <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/surface/rest-gateway.md">REST gateway
 * and servers</a> guide.
 */
package ai.protomolt.proto.server.micronaut;
