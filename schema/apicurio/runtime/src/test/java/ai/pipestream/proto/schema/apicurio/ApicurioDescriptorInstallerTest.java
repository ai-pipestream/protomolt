package ai.pipestream.proto.schema.apicurio;

import ai.pipestream.proto.descriptors.DescriptorLoader;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import com.microsoft.kiota.ApiException;
import com.microsoft.kiota.RequestAdapter;
import com.microsoft.kiota.RequestInformation;
import com.microsoft.kiota.serialization.Parsable;
import com.microsoft.kiota.serialization.ParsableFactory;
import com.microsoft.kiota.serialization.SerializationWriterFactory;
import com.microsoft.kiota.serialization.ValuedEnumParser;
import com.microsoft.kiota.store.BackingStoreFactory;
import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.client.models.ArtifactSearchResults;
import io.apicurio.registry.rest.client.models.SearchedArtifact;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.util.TypeLiteral;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.lang.annotation.Annotation;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Unit tests for {@link ApicurioDescriptorInstaller} with hand-rolled {@link Instance} fakes
 * and a {@link DescriptorRegistry} subclass that records {@code addLoader} calls (no CDI
 * container, no live registry).
 */
class ApicurioDescriptorInstallerTest {

    private static final String PROTO =
            "syntax = \"proto3\";\npackage unit.v1;\nmessage Registered { string id = 1; }\n";

    private final ApicurioDescriptorInstaller installer = new ApicurioDescriptorInstaller();

    @Test
    void disabledConfigSkipsRegistrationEntirely() {
        RecordingRegistry registry = new RecordingRegistry();

        installer.onStart(new StartupEvent(), instances(registry), instances(availableLoader()),
                config(false, true));

        assertThat(registry.added).isEmpty();
        assertThat(registry.findDescriptorByFullName("unit.v1.Registered")).isNull();
    }

    @Test
    void unsatisfiedRegistryIsANoOp() {
        assertThatCode(() -> installer.onStart(new StartupEvent(),
                ApicurioDescriptorInstallerTest.<DescriptorRegistry>unsatisfied(),
                instances(availableLoader()), config(true, true)))
                .doesNotThrowAnyException();
    }

    @Test
    void availableLoaderIsRegisteredUnavailableOnesAreSkipped() {
        RecordingRegistry registry = new RecordingRegistry();
        ApicurioDescriptorLoader available = availableLoader();
        ApicurioDescriptorLoader unavailable = new ApicurioDescriptorLoader((RegistryClient) null, "default");

        installer.onStart(new StartupEvent(), instances(registry), instances(available, unavailable),
                config(true, false));

        assertThat(registry.added).containsExactly(available);
    }

    @Test
    void nonApicurioLoadersAreIgnored() {
        RecordingRegistry registry = new RecordingRegistry();
        DescriptorLoader foreign = new DescriptorLoader() {
            @Override
            public List<com.google.protobuf.Descriptors.FileDescriptor> loadDescriptors() {
                return List.of();
            }

            @Override
            public com.google.protobuf.Descriptors.FileDescriptor loadDescriptor(String fileName) {
                return null;
            }

            @Override
            public boolean isAvailable() {
                return true;
            }

            @Override
            public String getLoaderType() {
                return "foreign";
            }
        };

        installer.onStart(new StartupEvent(), instances(registry), instances(foreign),
                config(true, false));

        assertThat(registry.added).isEmpty();
    }

    @Test
    void autoLoadOnStartupLoadsDescriptorsIntoTheRegistry() {
        RecordingRegistry registry = new RecordingRegistry();

        installer.onStart(new StartupEvent(), instances(registry), instances(availableLoader()),
                config(true, true));

        assertThat(registry.findDescriptorByFullName("unit.v1.Registered")).isNotNull();
    }

    @Test
    void withoutAutoLoadNothingIsFetchedAtStartup() {
        RecordingRegistry registry = new RecordingRegistry();
        SingleArtifactAdapter adapter = new SingleArtifactAdapter(PROTO, null);

        installer.onStart(new StartupEvent(), instances(registry),
                instances(new ApicurioDescriptorLoader(new RegistryClient(adapter), "default")),
                config(true, false));

        assertThat(adapter.searchCalls)
                .as("autoLoadOnStartup=false means startup performs no bulk load")
                .isZero();
        // The registered loader can still supply the descriptor when the registry asks.
        registry.autoLoadDescriptors();
        assertThat(registry.findDescriptorByFullName("unit.v1.Registered")).isNotNull();
    }

    @Test
    void autoLoadFailureIsLoggedNotPropagated() {
        RecordingRegistry registry = new RecordingRegistry();
        ApicurioDescriptorLoader broken = new ApicurioDescriptorLoader(
                new RegistryClient(new SingleArtifactAdapter(null, 500)), "default");

        assertThatCode(() -> installer.onStart(new StartupEvent(), instances(registry),
                instances(broken), config(true, true)))
                .doesNotThrowAnyException();
        // Registration still happened; only the bulk load failed.
        assertThat(registry.added).containsExactly(broken);
    }

    // ---------------------------------------------------------------- fixtures

    /** A loader that is available and serves one in-memory artifact. */
    private static ApicurioDescriptorLoader availableLoader() {
        return new ApicurioDescriptorLoader(
                new RegistryClient(new SingleArtifactAdapter(PROTO, null)), "default");
    }

    private static ProtoToolsApicurioConfig config(boolean enabled, boolean autoLoadOnStartup) {
        return new ProtoToolsApicurioConfig() {
            @Override
            public boolean enabled() {
                return enabled;
            }

            @Override
            public Optional<String> registryUrl() {
                return Optional.empty();
            }

            @Override
            public String groupId() {
                return "default";
            }

            @Override
            public boolean autoLoadOnStartup() {
                return autoLoadOnStartup;
            }
        };
    }

    @SafeVarargs
    private static <T> Instance<T> instances(T... values) {
        return new FakeInstance<>(List.of(values), false);
    }

    private static <T> Instance<T> unsatisfied() {
        return new FakeInstance<>(List.of(), true);
    }

    /** Records {@code addLoader} calls so the installer's registration is observable. */
    private static final class RecordingRegistry extends DescriptorRegistry {
        final List<DescriptorLoader> added = new ArrayList<>();

        @Override
        public void addLoader(DescriptorLoader loader) {
            added.add(loader);
            super.addLoader(loader);
        }
    }

    /** Minimal {@link Instance}: fixed contents, everything beyond the used surface unsupported. */
    private record FakeInstance<T>(List<T> values, boolean unsatisfied) implements Instance<T> {

        @Override
        public T get() {
            if (unsatisfied || values.isEmpty()) {
                throw new IllegalStateException("unsatisfied instance");
            }
            return values.getFirst();
        }

        @Override
        public Iterator<T> iterator() {
            return values.iterator();
        }

        @Override
        public boolean isUnsatisfied() {
            return unsatisfied;
        }

        @Override
        public boolean isAmbiguous() {
            return values.size() > 1;
        }

        @Override
        public Instance<T> select(Annotation... qualifiers) {
            return this;
        }

        @Override
        public <U extends T> Instance<U> select(Class<U> subtype, Annotation... qualifiers) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <U extends T> Instance<U> select(TypeLiteral<U> subtype, Annotation... qualifiers) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void destroy(T instance) {
        }

        @Override
        public Handle<T> getHandle() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<? extends Handle<T>> handles() {
            return List.of();
        }
    }

    /**
     * Serves exactly one artifact ({@code unit.proto}) in group {@code default}, or fails every
     * search with the given HTTP status when no content is configured.
     */
    private static final class SingleArtifactAdapter implements RequestAdapter {

        private final String proto;
        private final Integer failureStatus;
        int searchCalls;

        SingleArtifactAdapter(String proto, Integer failureStatus) {
            this.proto = proto;
            this.failureStatus = failureStatus;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <ModelType extends Parsable> ModelType send(
                RequestInformation requestInfo,
                HashMap<String, ParsableFactory<? extends Parsable>> errorMappings,
                ParsableFactory<ModelType> factory) {
            searchCalls++;
            if (failureStatus != null) {
                throw apiException(failureStatus);
            }
            SearchedArtifact artifact = new SearchedArtifact();
            artifact.setGroupId("default");
            artifact.setArtifactId("unit.proto");
            ArtifactSearchResults results = new ArtifactSearchResults();
            results.setArtifacts(new ArrayList<>(List.of(artifact)));
            results.setCount(1);
            return (ModelType) results;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <ModelType> ModelType sendPrimitive(
                RequestInformation requestInfo,
                HashMap<String, ParsableFactory<? extends Parsable>> errorMappings,
                Class<ModelType> targetClass) {
            if (proto == null) {
                throw apiException(404);
            }
            return (ModelType) new ByteArrayInputStream(proto.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public <ModelType extends Parsable> List<ModelType> sendCollection(
                RequestInformation requestInfo,
                HashMap<String, ParsableFactory<? extends Parsable>> errorMappings,
                ParsableFactory<ModelType> factory) {
            return List.of();
        }

        @Override
        public void enableBackingStore(BackingStoreFactory backingStoreFactory) {
        }

        @Override
        public SerializationWriterFactory getSerializationWriterFactory() {
            throw new UnsupportedOperationException();
        }

        @Override
        public <ModelType> List<ModelType> sendPrimitiveCollection(
                RequestInformation requestInfo,
                HashMap<String, ParsableFactory<? extends Parsable>> errorMappings,
                Class<ModelType> targetClass) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <ModelType extends Enum<ModelType>> ModelType sendEnum(
                RequestInformation requestInfo,
                HashMap<String, ParsableFactory<? extends Parsable>> errorMappings,
                ValuedEnumParser<ModelType> enumParser) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <ModelType extends Enum<ModelType>> List<ModelType> sendEnumCollection(
                RequestInformation requestInfo,
                HashMap<String, ParsableFactory<? extends Parsable>> errorMappings,
                ValuedEnumParser<ModelType> enumParser) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setBaseUrl(String baseUrl) {
        }

        @Override
        public String getBaseUrl() {
            return "http://fake";
        }

        @Override
        public <T> T convertToNativeRequest(RequestInformation requestInfo) {
            throw new UnsupportedOperationException();
        }

        private static ApiException apiException(int status) {
            return new ApiException("HTTP " + status) {
                {
                    setResponseStatusCode(status);
                }
            };
        }
    }
}
