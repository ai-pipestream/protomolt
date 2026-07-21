/**
 * The action catalog served as the {@code ai.pipestream.protomolt.v1.ProtoMoltService} gRPC
 * service.
 *
 * <p>Every verb is one typed RPC. {@link ProtoMoltCatalog} assembles the full catalog — the
 * built-in actions plus the gRPC, codegen, chain, and gather verbs — and
 * {@link ProtoMoltGrpcService} binds an
 * {@link ai.pipestream.proto.actions.ActionCatalog} as the service. Dispatch runs through
 * {@link CatalogBridge}: each request message's canonical proto3 JSON form is exactly the
 * action's input envelope and each action's output envelope parses as the response message, so
 * a call is one print, one dispatch, and one parse for every verb alike. That correspondence is
 * what keeps the gRPC, JSON/REST, and MCP surfaces byte-identical in their payloads.
 *
 * <p>The service is served descriptor-natively. {@link ProtoMoltServiceSchema} compiles
 * {@code protomolt_service.proto} at class load and the compiled file descriptor is attached to
 * the binding, so server reflection lists the service as it would a stub-generated one.
 * {@link ProtoMoltGrpcServer} is a ready-to-run server with reflection enabled, and
 * {@link ApiTokenServerInterceptor} applies a shared-secret credential check server-wide when
 * one is configured.
 *
 * <p>{@code ai.pipestream.proto.serve} mounts this service alongside the REST, OpenAPI, and MCP
 * surfaces in one process. See the
 * <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/grpc-service.md">gRPC
 * service guide</a>.
 */
package ai.pipestream.proto.grpc.service;
