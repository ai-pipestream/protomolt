/**
 * Quarkus facade over the protobuf JSON/REST gateway.
 *
 * <p>{@link ai.pipestream.proto.server.quarkus.QuarkusProtoRestFacade} is an application-scoped
 * CDI bean that JAX-RS resources call to serve the invoke, OpenAPI, and health responses. Unlike
 * the standalone hosts it does not bind a port: the application owns the JAX-RS resources and their
 * paths, and the facade supplies the status, body, and headers for each call as a
 * {@code QuarkusProtoRestFacade.Result}.
 *
 * <p>Request handling is delegated to {@link ai.pipestream.proto.rest.ProtoRestGateway}, with body
 * size checks, header flattening, and status mapping taken from
 * {@link ai.pipestream.proto.server.ProtoRestHttpSupport}, so responses match the standalone hosts.
 * Quarkus 3.x runs on Vert.x 4; the {@code servers/vertx} module targets Vert.x 5 and is the
 * preferred host once Quarkus moves to it.
 *
 * <p>See the
 * <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/rest-gateway.md">REST gateway
 * and servers</a> guide.
 */
package ai.pipestream.proto.server.quarkus;
