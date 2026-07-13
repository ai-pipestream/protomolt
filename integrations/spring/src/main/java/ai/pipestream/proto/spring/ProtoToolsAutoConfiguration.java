package ai.pipestream.proto.spring;

import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.descriptors.ClasspathDescriptorLoader;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.descriptors.GoogleDescriptorLoader;
import ai.pipestream.proto.json.ProtobufJsonTranscoder;
import ai.pipestream.proto.mapper.ProtoFieldMapper;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import ai.pipestream.proto.rest.ProtoApiTokenValidator;
import ai.pipestream.proto.rest.ProtoRestGateway;
import ai.pipestream.proto.rest.ProtoRestMethodRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

import java.util.Optional;

/**
 * Spring Boot auto-configuration for the Pipestream protobuf tools.
 *
 * <p>Registered via {@code META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports},
 * so Boot applications get these beans automatically; every bean is
 * {@link ConditionalOnMissingBean @ConditionalOnMissingBean}, so an application-defined bean of
 * the same type always wins. Plain-Spring (non-Boot) applications can still
 * {@code @Import(ProtoToolsAutoConfiguration.class)}: the Boot annotations are an optional
 * (compileOnly) dependency and are simply ignored when Boot is absent at runtime.</p>
 */
@AutoConfiguration
public class ProtoToolsAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public DescriptorRegistry descriptorRegistry() {
        DescriptorRegistry registry = new DescriptorRegistry();
        registry.addLoader(new GoogleDescriptorLoader());
        registry.addLoader(new ClasspathDescriptorLoader());
        return registry;
    }

    @Bean
    @ConditionalOnMissingBean
    public ProtoFieldMapper protoFieldMapper(DescriptorRegistry descriptorRegistry) {
        return new ProtoFieldMapperImpl(descriptorRegistry);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnClass(CelEvaluator.class)
    public CelEvaluator celEvaluator() {
        return new CelEvaluator();
    }

    @Bean
    @ConditionalOnMissingBean
    public ProtobufJsonTranscoder protobufJsonTranscoder(DescriptorRegistry descriptorRegistry) {
        return new ProtobufJsonTranscoder(descriptorRegistry);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnClass(ProtoRestGateway.class)
    public ProtoRestMethodRegistry protoRestMethodRegistry() {
        return new ProtoRestMethodRegistry();
    }

    /**
     * Fail-closed default token validator: every token-protected method is rejected until the
     * application defines a real {@link ProtoApiTokenValidator} bean (for example
     * {@link ProtoApiTokenValidator#sharedSecret(String)}). This is deliberate; a default that
     * accepts any non-blank token would silently pass junk tokens.
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnClass(ProtoRestGateway.class)
    public ProtoApiTokenValidator protoApiTokenValidator() {
        return (tokenConfig, headers, queryParams) -> Optional.of(
                "No API token validator is configured; rejecting request. Define a "
                        + "ProtoApiTokenValidator bean to enable token-protected methods.");
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnClass(ProtoRestGateway.class)
    public ProtoRestGateway protoRestGateway(
            ProtoRestMethodRegistry protoRestMethodRegistry,
            ProtobufJsonTranscoder protobufJsonTranscoder,
            ProtoApiTokenValidator protoApiTokenValidator) {
        return new ProtoRestGateway(
                protoRestMethodRegistry,
                protobufJsonTranscoder,
                protoApiTokenValidator);
    }
}
