/**
 * The action catalog served as the {@code ai.pipestream.proto.grpc.service.v1.ProtoMoltService} gRPC
 * service.
 *
 * <p>Every verb is one typed RPC. {@link ProtoMoltCatalog} assembles the full catalog — the
 * built-in actions plus the gRPC, service-workspace, codegen, workflow, jobs, inference, gather,
 * and emit verbs, and
 * {@link ProtoMoltGrpcService} binds an
 * {@link ai.protomolt.proto.actions.ActionCatalog} as the service. Dispatch runs through
 * {@link CatalogBridge}: each request message's canonical proto3 JSON form is exactly the
 * action's input envelope and each action's output envelope parses as the response message, so
 * a call is one print, one dispatch, and one parse for every verb alike. That correspondence is
 * what keeps the gRPC, JSON/REST, and MCP surfaces byte-identical in their payloads.
 *
 * <p>The service is served descriptor-natively. {@link ai.protomolt.proto.grpc.service.contract.ProtoMoltServiceSchema} compiles
 * {@code protomolt_service.proto} at class load and the compiled file descriptor is attached to
 * the binding, so server reflection lists the service as it would a stub-generated one.
 * {@link ProtoMoltGrpcServer} is a ready-to-run server with reflection enabled, and
 * {@link ai.protomolt.proto.authz.grpc.ApiTokenServerInterceptor} applies the credential
 * check server-wide when one is configured.
 *
 * <p>{@code ai.protomolt.proto.serve} mounts this service alongside the REST, OpenAPI, and MCP
 * surfaces in one process. See the
 * <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/surface/grpc-service.md">gRPC
 * service guide</a>.
 */
package ai.protomolt.proto.grpc.service;
