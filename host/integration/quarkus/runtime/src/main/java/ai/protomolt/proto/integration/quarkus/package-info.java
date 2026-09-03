/**
 * Runtime half of the Quarkus extension for the ProtoMolt protobuf tools.
 *
 * <p>{@link ai.protomolt.proto.integration.quarkus.ProtoToolsProducer} exposes the core object graph as CDI
 * producers: a {@link ai.protomolt.proto.descriptors.DescriptorRegistry}, a
 * {@link ai.protomolt.proto.mapper.ProtoFieldMapper}, a
 * {@link ai.protomolt.proto.http.json.ProtobufJsonTranscoder}, a
 * {@link ai.protomolt.proto.server.ProtoToolsServerConfig}, and the REST types
 * ({@link ai.protomolt.proto.http.rest.ProtoRestMethodRegistry},
 * {@link ai.protomolt.proto.http.rest.ProtoApiTokenValidator}, and
 * {@link ai.protomolt.proto.http.rest.ProtoRestGateway}). Each is a
 * {@link io.quarkus.arc.DefaultBean}, so an application bean of the same type overrides it without
 * an ambiguous resolution error.
 *
 * <p>The registry producer also folds in every available
 * {@link ai.protomolt.proto.descriptors.DescriptorLoader} bean in the container, so adding a
 * descriptor source means producing a bean. The default
 * {@link ai.protomolt.proto.http.rest.ProtoApiTokenValidator} rejects all tokens; applications produce
 * their own to accept credentials.
 *
 * <p>Extension jars are not bean archives, so this producer is registered at build time by the
 * companion {@code ai.pipestream.proto.integration.quarkus.deployment} package. HTTP endpoints are not provided
 * here — the Quarkus host facade lives in {@code servers/quarkus}.
 *
 * <p>See the
 * <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/surface/framework-integrations.md">Framework
 * integrations</a> guide.
 */
package ai.protomolt.proto.integration.quarkus;
