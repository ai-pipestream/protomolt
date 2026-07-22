package ai.pipestream.proto.quarkus;

import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.descriptors.ClasspathDescriptorLoader;
import ai.pipestream.proto.descriptors.DescriptorLoader;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.descriptors.GoogleDescriptorLoader;
import ai.pipestream.proto.json.ProtobufJsonTranscoder;
import ai.pipestream.proto.mapper.ProtoFieldMapper;
import ai.pipestream.proto.mapper.ProtoFieldMapperImpl;
import ai.pipestream.proto.rest.ProtoApiTokenValidator;
import ai.pipestream.proto.rest.ProtoRestGateway;
import ai.pipestream.proto.rest.ProtoRestMethodRegistry;
import ai.pipestream.proto.server.ProtoToolsServerConfig;
import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Optional;
import java.util.Set;

/**
 * CDI producers for the Pipestream protobuf tools runtime.
 *
 * <p>Every producer is a {@link DefaultBean}, so applications override any of them by
 * declaring their own bean of the same type without hitting an
 * {@code AmbiguousResolutionException}.</p>
 */
@Singleton
public final class ProtoToolsProducer {

    @Produces
    @DefaultBean
    @Singleton
    public DescriptorRegistry descriptorRegistry(Instance<DescriptorLoader> extraLoaders) {
        DescriptorRegistry registry = new LoaderDedupingDescriptorRegistry();
        registry.addLoader(new GoogleDescriptorLoader());
        registry.addLoader(new ClasspathDescriptorLoader());
        if (extraLoaders != null) {
            for (DescriptorLoader loader : extraLoaders) {
                if (loader != null && loader.isAvailable()) {
                    registry.addLoader(loader);
                }
            }
        }
        return registry;
    }

    @Produces
    @DefaultBean
    @Singleton
    public ProtoFieldMapper protoFieldMapper(DescriptorRegistry descriptorRegistry) {
        return new ProtoFieldMapperImpl(descriptorRegistry);
    }

    @Produces
    @DefaultBean
    @Singleton
    public CelEvaluator celEvaluator() {
        return new CelEvaluator();
    }

    @Produces
    @DefaultBean
    @Singleton
    public ProtobufJsonTranscoder protobufJsonTranscoder(DescriptorRegistry descriptorRegistry) {
        return new ProtobufJsonTranscoder(descriptorRegistry);
    }

    @Produces
    @DefaultBean
    @Singleton
    public ProtoRestMethodRegistry protoRestMethodRegistry() {
        return new ProtoRestMethodRegistry();
    }

    /** Default server config; apps override the REST prefix by producing their own bean. */
    @Produces
    @DefaultBean
    @Singleton
    public ProtoToolsServerConfig protoToolsServerConfig() {
        return ProtoToolsServerConfig.defaults();
    }

    /**
     * Fail-closed default token validator: every token-protected method is rejected until the
     * application supplies a real {@link ProtoApiTokenValidator} bean (for example
     * {@link ProtoApiTokenValidator#sharedSecret(String)}). This is deliberate; a default that
     * accepts any non-blank token would silently pass junk tokens.
     */
    @Produces
    @DefaultBean
    @Singleton
    public ProtoApiTokenValidator protoApiTokenValidator() {
        return (tokenConfig, headers, queryParams) -> Optional.of(
                "No API token validator is configured; rejecting request. Provide a "
                        + "ProtoApiTokenValidator bean to enable token-protected methods.");
    }

    @Produces
    @DefaultBean
    @Singleton
    public ProtoRestGateway protoRestGateway(
            ProtoRestMethodRegistry protoRestMethodRegistry,
            ProtobufJsonTranscoder protobufJsonTranscoder,
            ProtoApiTokenValidator protoApiTokenValidator) {
        return new ProtoRestGateway(
                protoRestMethodRegistry,
                protobufJsonTranscoder,
                protoApiTokenValidator);
    }

    /**
     * Registry whose {@code addLoader} is idempotent per loader <em>instance</em>: this
     * producer registers every available {@link DescriptorLoader} bean up front, and extension
     * installers (e.g. the Apicurio extension's startup observer) may re-add their loader bean
     * afterwards; without deduplication the loader would be consulted twice per lookup and
     * bulk load.
     */
    static final class LoaderDedupingDescriptorRegistry extends DescriptorRegistry {
        private final Set<DescriptorLoader> addedLoaders =
                Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));

        @Override
        public void addLoader(DescriptorLoader loader) {
            if (loader == null || !addedLoaders.add(loader)) {
                return;
            }
            super.addLoader(loader);
        }
    }
}
