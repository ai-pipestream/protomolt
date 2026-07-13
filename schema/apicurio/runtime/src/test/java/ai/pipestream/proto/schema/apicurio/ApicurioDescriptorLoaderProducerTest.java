package ai.pipestream.proto.schema.apicurio;

import ai.pipestream.proto.descriptors.DescriptorLoader;
import ai.pipestream.proto.schema.apicurio.ApicurioDescriptorLoaderProducer.ApicurioRegistryClientHolder;
import io.apicurio.registry.rest.client.RegistryClient;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Disposes;
import jakarta.inject.Singleton;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class ApicurioDescriptorLoaderProducerTest {

    private final ApicurioDescriptorLoaderProducer producer = new ApicurioDescriptorLoaderProducer();

    @Test
    void disabledConfigYieldsUnavailableLoaderInsteadOfNull() {
        ProtoToolsApicurioConfig config = config(false, Optional.empty());
        DescriptorLoader loader = producer.produceApicurioDescriptorLoader(null, config);
        assertThat(loader).isNotNull();
        assertThat(loader.isAvailable()).isFalse();
    }

    @Test
    void missingRegistryUrlYieldsUnavailableLoaderInsteadOfNull() {
        ProtoToolsApicurioConfig config = config(true, Optional.of(" "));
        ApicurioRegistryClientHolder holder = producer.registryClientHolder(config);
        assertThat(holder.client()).isNull();
        assertThat(producer.produceRegistryClient(holder)).isNull();
        holder.close(); // no owned transport; must be a safe no-op
        DescriptorLoader loader = producer.produceApicurioDescriptorLoader(null, config);
        assertThat(loader).isNotNull();
        assertThat(loader.isAvailable()).isFalse();
    }

    @Test
    void registryClientProducerIsDependentScopedSoNullIsPermitted() throws Exception {
        Method method = ApicurioDescriptorLoaderProducer.class.getMethod(
                "produceRegistryClient", ApicurioRegistryClientHolder.class);
        assertThat(method.isAnnotationPresent(Dependent.class))
                .as("a producer that can return null must be @Dependent")
                .isTrue();
        assertThat(method.isAnnotationPresent(Singleton.class)).isFalse();
    }

    @Test
    void holderIsSingletonScopedWithADisposer() throws Exception {
        Method holderProducer = ApicurioDescriptorLoaderProducer.class.getMethod(
                "registryClientHolder", ProtoToolsApicurioConfig.class);
        assertThat(holderProducer.isAnnotationPresent(Singleton.class))
                .as("one client per application")
                .isTrue();

        Method disposer = ApicurioDescriptorLoaderProducer.class.getDeclaredMethod(
                "closeRegistryClientHolder", ApicurioRegistryClientHolder.class);
        assertThat(disposer.getParameters()[0].isAnnotationPresent(Disposes.class))
                .as("the transport must be closed on shutdown")
                .isTrue();
    }

    @Test
    void configuredUrlYieldsSharedClientWhoseTransportCloses() {
        ProtoToolsApicurioConfig config = config(true, Optional.of("http://localhost:65535/apis/registry/v3"));
        ApicurioRegistryClientHolder holder = producer.registryClientHolder(config);
        try {
            assertThat(holder.client()).isNotNull();
            // Every injection point sees the holder's single client instance.
            assertThat(producer.produceRegistryClient(holder))
                    .isSameAs(producer.produceRegistryClient(holder))
                    .isSameAs(holder.client());
        } finally {
            holder.close();
        }
    }

    @Test
    void descriptorLoaderProducerNeverReturnsNull() {
        assertThat(producer.produceApicurioDescriptorLoader((RegistryClient) null, config(true, Optional.empty())))
                .isNotNull();
    }

    private static ProtoToolsApicurioConfig config(boolean enabled, Optional<String> registryUrl) {
        return new ProtoToolsApicurioConfig() {
            @Override
            public boolean enabled() {
                return enabled;
            }

            @Override
            public Optional<String> registryUrl() {
                return registryUrl;
            }

            @Override
            public String groupId() {
                return "default";
            }

            @Override
            public boolean autoLoadOnStartup() {
                return false;
            }
        };
    }
}
