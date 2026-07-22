/**
 * Spring integration for the ProtoMolt protobuf tools.
 *
 * <p>{@link ai.pipestream.proto.spring.ProtoToolsAutoConfiguration} contributes the core object
 * graph as beans: a {@link ai.pipestream.proto.descriptors.DescriptorRegistry}, a
 * {@link ai.pipestream.proto.mapper.ProtoFieldMapper}, a
 * {@link ai.pipestream.proto.json.ProtobufJsonTranscoder}, and the REST types
 * ({@link ai.pipestream.proto.rest.ProtoRestMethodRegistry},
 * {@link ai.pipestream.proto.rest.ProtoApiTokenValidator}, and
 * {@link ai.pipestream.proto.rest.ProtoRestGateway}). It is registered through
 * {@code AutoConfiguration.imports} for Boot applications and can be imported directly by
 * plain-Spring ones. Every bean is conditional on a missing bean of the same type, so an
 * application definition always takes precedence.
 *
 * <p>The registry aggregates every application-defined
 * {@link ai.pipestream.proto.descriptors.DescriptorLoader} bean on top of the classpath loaders,
 * which makes contributing a descriptor source a matter of declaring a bean. The default
 * {@link ai.pipestream.proto.rest.ProtoApiTokenValidator} rejects all tokens; applications define
 * their own to accept credentials.
 *
 * <p>This package provides beans, not HTTP endpoints — the Spring MVC host lives in
 * {@code servers/spring}.
 *
 * <p>See the
 * <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/framework-integrations.md">Framework
 * integrations</a> guide.
 */
package ai.pipestream.proto.spring;
