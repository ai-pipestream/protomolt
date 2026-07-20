/**
 * OpenAPI 3.x generation from protobuf service descriptors.
 *
 * <p>{@link ProtoOpenApiGenerator} builds the document from an
 * {@link ai.pipestream.proto.rest.ProtoRestMethodRegistry}, so the description of the HTTP
 * surface comes from the same registrations the gateway dispatches on rather than from a
 * separately maintained specification. Request and response schemas are derived from the
 * protobuf descriptors attached to each {@link ai.pipestream.proto.rest.ProtoRestMethod},
 * following proto3 JSON conventions; message types are emitted once under {@code components}
 * and referenced, which keeps recursive types finite.
 *
 * <p>Annotations read by the REST package are honored where present:
 * {@link ai.pipestream.proto.rest.ProtoRestExposed} supplies summaries and the declared HTTP
 * verbs, and {@link ai.pipestream.proto.rest.ProtoApiToken} becomes a security scheme. The
 * generated document is a plain map or JSON string, so a host can emit it at build time or
 * serve it from {@code /openapi.json} at startup.
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/rest-gateway.md">REST
 * gateway guide</a>.
 */
package ai.pipestream.proto.openapi;
