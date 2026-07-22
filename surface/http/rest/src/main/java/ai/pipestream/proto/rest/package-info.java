/**
 * A framework-agnostic JSON/REST gateway over protobuf services.
 *
 * <p>{@link ProtoRestGateway} dispatches {@code POST /{service}/{method}} calls: it applies the
 * API-token check, transcodes the JSON body into the request message, invokes the registered
 * handler, and transcodes the response back through
 * {@link ai.pipestream.proto.json.ProtobufJsonTranscoder}. Routes come from
 * {@link ProtoRestMethodRegistry}, which holds one {@link ProtoRestMethod} per exposed RPC and
 * rejects a duplicate {@code service/method} pair rather than letting registration order decide
 * the winner.
 *
 * <p>Two extension points keep the package independent of any HTTP stack. Handlers may be
 * discovered from the {@link ProtoRestExposed} and {@link ProtoApiToken} annotations through
 * {@link ProtoRestAnnotationRegistrar}, or registered directly by a host module; authentication
 * is delegated to a {@link ProtoApiTokenValidator} implementation, with the declared requirement
 * carried at runtime as an {@link ApiTokenRequirement}. Failures are reported as subtypes of
 * {@link ProtoRestException} so a host can map each one to a status code.
 *
 * <p>{@code ai.pipestream.proto.openapi} generates an OpenAPI document from the same registry,
 * and the server modules bind the gateway to a concrete HTTP stack. See the
 * <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/rest-gateway.md">REST gateway
 * guide</a>.
 */
package ai.pipestream.proto.rest;
