/**
 * Netty host for the protobuf JSON/REST gateway.
 *
 * <p>{@link ai.pipestream.proto.server.netty.NettyProtoRestServer} implements
 * {@link ai.pipestream.proto.server.ProtoRestServerHost} over a Netty HTTP pipeline. It serves the
 * three routes common to every host — the invoke route under the configured REST prefix, the
 * OpenAPI document, and the health endpoint — and delegates request handling to
 * {@link ai.pipestream.proto.rest.ProtoRestGateway}. Gateway invocations are dispatched to a
 * virtual-thread-per-task executor so a slow backend does not occupy an event loop.
 *
 * <p>Shared status mapping and path parsing come from
 * {@link ai.pipestream.proto.server.ProtoRestHttpSupport}; behavior therefore matches the JDK,
 * Vert.x, and framework-hosted adapters for the same request.
 *
 * <p>See the
 * <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/rest-gateway.md">REST gateway
 * and servers</a> guide.
 */
package ai.pipestream.proto.server.netty;
