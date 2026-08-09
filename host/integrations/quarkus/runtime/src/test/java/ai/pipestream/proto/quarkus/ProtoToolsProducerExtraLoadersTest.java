package ai.pipestream.proto.quarkus;

import ai.pipestream.proto.descriptors.DescriptorLoader;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import com.google.protobuf.Descriptors.FileDescriptor;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.util.TypeLiteral;
import org.junit.jupiter.api.Test;

import java.lang.annotation.Annotation;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Covers the {@code descriptorRegistry(Instance<DescriptorLoader>)} producer's handling of
 * application-supplied loader beans: available loaders join the registry, unavailable and null
 * ones are skipped, and the deduping registry tolerates nulls and distinct instances.
 */
class ProtoToolsProducerExtraLoadersTest {

    @Test
    void availableExtraLoadersAreRegisteredAndConsulted() {
        CountingLoader loader = new CountingLoader(true);
        DescriptorRegistry registry = new ProtoToolsProducer()
                .descriptorRegistry(new ListInstance<>(List.of(loader)));

        registry.autoLoadDescriptors();

        assertThat(loader.loadCount())
                .as("an available loader bean must be consulted during auto-load")
                .isEqualTo(1);
    }

    @Test
    void unavailableExtraLoadersAreSkipped() {
        CountingLoader unavailable = new CountingLoader(false);
        DescriptorRegistry registry = new ProtoToolsProducer()
                .descriptorRegistry(new ListInstance<>(List.of(unavailable)));

        registry.autoLoadDescriptors();

        assertThat(unavailable.loadCount())
                .as("an unavailable loader bean must never be added to the registry")
                .isZero();
    }

    @Test
    void nullExtraLoaderEntriesAreSkipped() {
        List<DescriptorLoader> loaders = new java.util.ArrayList<>();
        loaders.add(null);
        loaders.add(new CountingLoader(true));
        loaders.add(null);

        DescriptorRegistry registry = new ProtoToolsProducer()
                .descriptorRegistry(new ListInstance<>(loaders));

        assertThat(registry).isNotNull();
        assertThatCode(registry::autoLoadDescriptors).doesNotThrowAnyException();
    }

    @Test
    void dedupingRegistryToleratesNullAndConsultsDistinctLoaderInstances() {
        DescriptorRegistry registry = new ProtoToolsProducer().descriptorRegistry(null);
        CountingLoader first = new CountingLoader(true);
        CountingLoader second = new CountingLoader(true);

        registry.addLoader(null);
        registry.addLoader(first);
        // Same instance re-added: deduped. Distinct instance: kept.
        registry.addLoader(first);
        registry.addLoader(second);
        registry.autoLoadDescriptors();

        assertThat(first.loadCount()).isEqualTo(1);
        assertThat(second.loadCount()).isEqualTo(1);
    }

    private static final class CountingLoader implements DescriptorLoader {
        private final boolean available;
        private final AtomicInteger loads = new AtomicInteger();

        private CountingLoader(boolean available) {
            this.available = available;
        }

        int loadCount() {
            return loads.get();
        }

        @Override
        public List<FileDescriptor> loadDescriptors() {
            loads.incrementAndGet();
            return List.of();
        }

        @Override
        public FileDescriptor loadDescriptor(String fileName) {
            return null;
        }

        @Override
        public boolean isAvailable() {
            return available;
        }

        @Override
        public String getLoaderType() {
            return "counting";
        }
    }

    /** Minimal {@link Instance} stub: only iteration and {@code get()} are meaningful. */
    private static final class ListInstance<T> implements Instance<T> {
        private final List<T> items;

        private ListInstance(List<T> items) {
            this.items = items;
        }

        @Override
        public Iterator<T> iterator() {
            return items.iterator();
        }

        @Override
        public T get() {
            return items.isEmpty() ? null : items.get(0);
        }

        @Override
        public Instance<T> select(Annotation... qualifiers) {
            throw new UnsupportedOperationException();
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
        public boolean isUnsatisfied() {
            return items.isEmpty();
        }

        @Override
        public boolean isAmbiguous() {
            return items.size() > 1;
        }

        @Override
        public void destroy(T instance) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Handle<T> getHandle() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<? extends Handle<T>> handles() {
            throw new UnsupportedOperationException();
        }
    }
}
