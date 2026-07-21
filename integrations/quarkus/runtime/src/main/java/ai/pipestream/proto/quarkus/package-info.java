/**
 * Runtime half of the Quarkus extension for the ProtoMolt protobuf tools.
 *
 * <p>{@link ai.pipestream.proto.quarkus.ProtoToolsProducer} exposes the core object graph as CDI
 * producers: a {@link ai.pipestream.proto.descriptors.DescriptorRegistry}, a
 * {@link ai.pipestream.proto.mapper.ProtoFieldMapper}, a
 * {@link ai.pipestream.proto.json.ProtobufJsonTranscoder}, a
 * {@link ai.pipestream.proto.server.ProtoToolsServerConfig}, and the REST types
 * ({@link ai.pipestream.proto.rest.ProtoRestMethodRegistry},
 * {@link ai.pipestream.proto.rest.ProtoApiTokenValidator}, and
 * {@link ai.pipestream.proto.rest.ProtoRestGateway}). Each is a
 * {@link io.quarkus.arc.DefaultBean}, so an application bean of the same type overrides it without
 * an ambiguous resolution error.
 *
 * <p>The registry producer also folds in every available
 * {@link ai.pipestream.proto.descriptors.DescriptorLoader} bean in the container, so adding a
 * descriptor source means producing a bean. The default
 * {@link ai.pipestream.proto.rest.ProtoApiTokenValidator} rejects all tokens; applications produce
 * their own to accept credentials.
 *
 * <p>Extension jars are not bean archives, so this producer is registered at build time by the
 * companion {@code ai.pipestream.proto.quarkus.deployment} package. HTTP endpoints are not provided
 * here — the Quarkus host facade lives in {@code servers/quarkus}.
 *
 * <p>See the
 * <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/framework-integrations.md">Framework
 * integrations</a> guide.
 */
package ai.pipestream.proto.quarkus;
