/**
 * The one-process ProtoMolt server, mounting every surface over a single action catalog.
 *
 * <p>{@link ProtoMoltServe} is the entry point and the {@code protomolt-serve} launcher. It runs
 * {@code ProtoMoltService} over gRPC with server reflection through
 * {@link ai.pipestream.proto.grpc.service.ProtoMoltGrpcServer}, the same verbs over JSON/REST
 * through {@link ai.pipestream.proto.rest.ProtoRestGateway}, the MCP server on streamable HTTP,
 * and optionally the git-backed schema registry speaking the Confluent protocol.
 *
 * <p>The HTTP surfaces are assembled from thin handlers. {@link ProtoMoltRestMount} registers
 * every RPC as {@code POST /grpc-json/ProtoMoltService/{Method}} with the envelopes the gRPC
 * surface uses, described by the document
 * {@link ai.pipestream.proto.openapi.ProtoOpenApiGenerator} produces;
 * {@link SwaggerUiHandler} serves Swagger UI against that document; and
 * {@link McpHttpHandler} carries JSON-RPC messages for
 * {@link ai.pipestream.proto.mcp.McpServer} over one stateless endpoint, so an MCP client
 * connects with a URL alone.
 *
 * <p>This package composes existing modules rather than adding verbs: the catalog comes from
 * {@link ai.pipestream.proto.grpc.service.ProtoMoltCatalog}, and each surface reuses the module
 * that defines it. See the
 * <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/grpc-service.md">gRPC
 * service guide</a>.
 */
package ai.pipestream.proto.serve;
