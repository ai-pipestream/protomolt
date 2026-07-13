package ai.pipestream.proto.schema.apicurio;

import ai.pipestream.proto.descriptors.DescriptorLoader.DescriptorLoadException;
import com.microsoft.kiota.ApiException;
import com.microsoft.kiota.RequestAdapter;
import com.microsoft.kiota.RequestInformation;
import com.microsoft.kiota.serialization.Parsable;
import com.microsoft.kiota.serialization.ParsableFactory;
import com.microsoft.kiota.serialization.SerializationWriterFactory;
import com.microsoft.kiota.serialization.ValuedEnumParser;
import com.microsoft.kiota.store.BackingStoreFactory;
import io.apicurio.registry.rest.client.RegistryClient;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApicurioDescriptorLoaderTest {

    @Test
    void builderRequiresRegistryUrl() {
        assertThatThrownBy(() -> ApicurioDescriptorLoader.builder().groupId("g").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Registry URL");
    }

    @Test
    void builderRequiresGroupId() {
        assertThatThrownBy(() -> ApicurioDescriptorLoader.builder()
                .registryUrl("http://localhost:8080")
                .groupId(" ")
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Group ID");
    }

    @Test
    void nullClientIsUnavailable() {
        // Explicitly supplying a null client opts out of client construction.
        ApicurioDescriptorLoader loader = ApicurioDescriptorLoader.builder()
                .registryUrl("http://localhost:8080/apis/registry/v3")
                .groupId("default")
                .registryClient(null)
                .build();
        assertThat(loader.isAvailable()).isFalse();
        assertThat(loader.getLoaderType()).contains("Apicurio");
        assertThat(loader.getGroupId()).isEqualTo("default");
        assertThatThrownBy(loader::loadDescriptors)
                .isInstanceOf(DescriptorLoadException.class);
    }

    @Test
    void builderConstructsClientFromRegistryUrlWhenNoneSupplied() {
        ApicurioDescriptorLoader loader = ApicurioDescriptorLoader.builder()
                .registryUrl("http://localhost:65535/apis/registry/v3")
                .groupId("default")
                .build();
        assertThat(loader.isAvailable())
                .as("builder().registryUrl(...).build() must yield a usable loader")
                .isTrue();
    }

    @Test
    void notFoundReturnsNullAndIsNegativelyCached() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        ApicurioDescriptorLoader loader = new ApicurioDescriptorLoader(
                clientFailingWith(() -> {
                    attempts.incrementAndGet();
                    return apiException(404);
                }),
                "default");

        assertThat(loader.loadDescriptor("missing")).isNull();
        int firstLookupAttempts = attempts.get();
        assertThat(firstLookupAttempts).isGreaterThan(0);

        assertThat(loader.loadDescriptor("missing")).isNull();
        assertThat(attempts.get())
                .as("negative result must be cached, not re-fetched")
                .isEqualTo(firstLookupAttempts);

        loader.clearCache();
        assertThat(loader.loadDescriptor("missing")).isNull();
        assertThat(attempts.get())
                .as("clearCache must drop negative entries")
                .isGreaterThan(firstLookupAttempts);
    }

    @Test
    void authFailureSurfacesInsteadOfMasqueradingAsNotFound() {
        ApicurioDescriptorLoader loader = new ApicurioDescriptorLoader(
                clientFailingWith(() -> apiException(401)), "default");
        assertThatThrownBy(() -> loader.loadDescriptor("anything"))
                .isInstanceOf(DescriptorLoadException.class)
                .hasMessageContaining("401");
    }

    @Test
    void serverErrorSurfacesInsteadOfMasqueradingAsNotFound() {
        ApicurioDescriptorLoader loader = new ApicurioDescriptorLoader(
                clientFailingWith(() -> apiException(503)), "default");
        assertThatThrownBy(() -> loader.loadDescriptor("anything"))
                .isInstanceOf(DescriptorLoadException.class)
                .hasMessageContaining("503");
    }

    @Test
    void connectionFailureSurfacesInsteadOfMasqueradingAsNotFound() {
        ApicurioDescriptorLoader loader = new ApicurioDescriptorLoader(
                clientFailingWith(() -> new RuntimeException("connection refused")), "default");
        assertThatThrownBy(() -> loader.loadDescriptor("anything"))
                .isInstanceOf(DescriptorLoadException.class)
                .hasCauseInstanceOf(RuntimeException.class);
    }

    // ---------------------------------------------------------------- stubs

    private static ApiException apiException(int status) {
        return new ApiException("HTTP " + status) {
            {
                setResponseStatusCode(status);
            }
        };
    }

    /** A registry client whose every request fails with the supplied exception. */
    private static RegistryClient clientFailingWith(Supplier<RuntimeException> failure) {
        return new RegistryClient(new ThrowingRequestAdapter(failure));
    }

    private record ThrowingRequestAdapter(Supplier<RuntimeException> failure) implements RequestAdapter {
        @Override
        public void enableBackingStore(BackingStoreFactory backingStoreFactory) {
        }

        @Override
        public SerializationWriterFactory getSerializationWriterFactory() {
            throw failure.get();
        }

        @Override
        public <ModelType extends Parsable> ModelType send(
                RequestInformation requestInfo,
                HashMap<String, ParsableFactory<? extends Parsable>> errorMappings,
                ParsableFactory<ModelType> factory) {
            throw failure.get();
        }

        @Override
        public <ModelType extends Parsable> List<ModelType> sendCollection(
                RequestInformation requestInfo,
                HashMap<String, ParsableFactory<? extends Parsable>> errorMappings,
                ParsableFactory<ModelType> factory) {
            throw failure.get();
        }

        @Override
        public <ModelType> ModelType sendPrimitive(
                RequestInformation requestInfo,
                HashMap<String, ParsableFactory<? extends Parsable>> errorMappings,
                Class<ModelType> targetClass) {
            throw failure.get();
        }

        @Override
        public <ModelType> List<ModelType> sendPrimitiveCollection(
                RequestInformation requestInfo,
                HashMap<String, ParsableFactory<? extends Parsable>> errorMappings,
                Class<ModelType> targetClass) {
            throw failure.get();
        }

        @Override
        public <ModelType extends Enum<ModelType>> ModelType sendEnum(
                RequestInformation requestInfo,
                HashMap<String, ParsableFactory<? extends Parsable>> errorMappings,
                ValuedEnumParser<ModelType> enumParser) {
            throw failure.get();
        }

        @Override
        public <ModelType extends Enum<ModelType>> List<ModelType> sendEnumCollection(
                RequestInformation requestInfo,
                HashMap<String, ParsableFactory<? extends Parsable>> errorMappings,
                ValuedEnumParser<ModelType> enumParser) {
            throw failure.get();
        }

        @Override
        public void setBaseUrl(String baseUrl) {
        }

        @Override
        public String getBaseUrl() {
            return "http://stub";
        }

        @Override
        public <T> T convertToNativeRequest(RequestInformation requestInfo) {
            throw failure.get();
        }
    }
}
