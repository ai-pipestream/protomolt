/**
 * Vert.x 5 host for the protobuf JSON/REST gateway.
 *
 * <p>{@link ai.pipestream.proto.server.vertx.VertxProtoRestServer} implements
 * {@link ai.pipestream.proto.server.ProtoRestServerHost} on a Vert.x
 * {@link io.vertx.core.http.HttpServer}, serving the invoke, OpenAPI, and health routes and
 * delegating request handling to
 * {@link ai.pipestream.proto.rest.ProtoRestGateway}. It either creates its own
 * {@link io.vertx.core.Vertx} instance or accepts one supplied by the application; only an instance
 * it created is closed with the server.
 *
 * <p>{@code VertxProtoRestServer.createRouter()} returns an {@link io.vertx.ext.web.Router} carrying
 * the same routes, for mounting under a router the application already owns. Quarkus 3.x runs on
 * Vert.x 4 and is served instead by the {@code servers/quarkus} module.
 *
 * <p>See the
 * <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/rest-gateway.md">REST gateway
 * and servers</a> guide.
 */
package ai.pipestream.proto.server.vertx;
