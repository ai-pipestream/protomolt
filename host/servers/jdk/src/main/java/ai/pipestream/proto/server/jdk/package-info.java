/**
 * Host for the protobuf JSON/REST gateway built on the JDK HTTP server.
 *
 * <p>{@link ai.pipestream.proto.server.jdk.JdkProtoRestServer} implements
 * {@link ai.pipestream.proto.server.ProtoRestServerHost} on
 * {@link com.sun.net.httpserver.HttpServer} with a virtual-thread executor, and adds no
 * dependencies beyond the JDK itself. Request handling is delegated to
 * {@link ai.pipestream.proto.rest.ProtoRestGateway}; the class binds the invoke, OpenAPI, and
 * health routes and applies the shared status mapping from
 * {@link ai.pipestream.proto.server.ProtoRestHttpSupport}.
 *
 * <p>Additional handlers can be mounted alongside the gateway with
 * {@code JdkProtoRestServer.withContext(String, com.sun.net.httpserver.HttpHandler)} before the
 * server starts. The {@code servers/netty} and {@code servers/vertx} modules serve the same routes
 * on their own stacks.
 *
 * <p>See the
 * <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/rest-gateway.md">REST gateway
 * and servers</a> guide.
 */
package ai.pipestream.proto.server.jdk;
