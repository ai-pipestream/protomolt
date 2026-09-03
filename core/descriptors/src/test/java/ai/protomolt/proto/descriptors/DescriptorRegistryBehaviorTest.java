package ai.protomolt.proto.descriptors;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Behavior tests for {@link DescriptorRegistry} beyond the basic register/find paths:
 * snapshot semantics, {@code loadFrom} counting, auto-load failure handling, the bounded
 * negative-lookup cache, and concurrent registration. The basics live in
 * {@link DescriptorRegistryTest} and {@link DescriptorRegistryLoaderTest}.
 */
class DescriptorRegistryBehaviorTest {

    @Test
    void registeredDescriptorsIsAnImmutablePointInTimeSnapshot() throws Exception {
        DescriptorRegistry registry = new DescriptorRegistry();
        Descriptor descriptor = message("snap.pkg", "snap/a.proto", "Snap");
        registry.register(descriptor);

        List<Descriptor> snapshot = registry.registeredDescriptors();

        assertThat(snapshot).contains(descriptor);
        assertThatThrownBy(() -> snapshot.add(descriptor))
                .isInstanceOf(UnsupportedOperationException.class);

        // Later registrations do not leak into an already-taken snapshot.
        registry.register(message("snap.pkg", "snap/b.proto", "Later"));
        assertThat(snapshot).noneMatch(d -> d.getFullName().equals("snap.pkg.Later"));
        assertThat(registry.registeredDescriptors())
                .anyMatch(d -> d.getFullName().equals("snap.pkg.Later"));
    }

    @Test
    void loadFromCountsTopLevelMessagesButRegistersNestedOnesToo() throws Exception {
        DescriptorRegistry registry = new DescriptorRegistry();
        FileDescriptor file = file("count/nested.proto", "count.pkg",
                parentWithNested("Parent", "Inner"));

        int count = registry.loadFrom(new EnumeratingLoader(List.of(file)));

        // The contract counts fd.getMessageTypes() — top-level messages only...
        assertThat(count).isEqualTo(1);
        // ...but registration recurses into nested types.
        assertThat(registry.findDescriptorByFullName("count.pkg.Parent")).isNotNull();
        assertThat(registry.findDescriptorByFullName("count.pkg.Parent.Inner")).isNotNull();
    }

    @Test
    void loadFromSumsCountsAcrossFiles() throws Exception {
        DescriptorRegistry registry = new DescriptorRegistry();
        FileDescriptor a = file("count/a.proto", "count.pkg", simple("A"));
        FileDescriptor b = file("count/b.proto", "count.pkg", simple("B"));

        int count = registry.loadFrom(new EnumeratingLoader(List.of(a, b)));

        assertThat(count).isEqualTo(2);
    }

    @Test
    void loadFromPropagatesLoaderFailure() {
        DescriptorRegistry registry = new DescriptorRegistry();
        DescriptorLoader failing = new FailingLoader(true);

        assertThatThrownBy(() -> registry.loadFrom(failing))
                .isInstanceOf(DescriptorLoader.DescriptorLoadException.class)
                .hasMessageContaining("load failed");
    }

    @Test
    void autoLoadSkipsUnavailableLoaders() {
        DescriptorRegistry registry = new DescriptorRegistry();
        // Throws if consulted at all; must never be consulted while unavailable.
        registry.addLoader(new UnavailableLoader());
        int sizeBefore = registry.size();

        registry.autoLoadDescriptors();

        assertThat(registry.size()).isEqualTo(sizeBefore);
    }

    @Test
    void autoLoadSwallowsOneLoadersFailureAndStillServesTheNext() throws Exception {
        DescriptorRegistry registry = new DescriptorRegistry();
        registry.addLoader(new FailingLoader(true));
        FileDescriptor file = file("after.proto", "after.pkg", simple("AfterFailure"));
        registry.addLoader(new EnumeratingLoader(List.of(file)));

        // Must not throw: a failing loader is logged, not fatal.
        registry.autoLoadDescriptors();

        assertThat(registry.findDescriptorByFullName("after.pkg.AfterFailure")).isNotNull();
    }

    @Test
    void findDescriptorPrefersFullNameMatchOverSimpleNameCollision() throws Exception {
        DescriptorRegistry registry = new DescriptorRegistry();
        Descriptor first = message("pref.one", "pref/one.proto", "Dup");
        Descriptor second = message("pref.two", "pref/two.proto", "Dup");
        registry.register(first);
        registry.register(second);

        // A name that IS a full name resolves by full name, even though a simple-name
        // collision exists and the first registration wins simple-name lookups.
        assertThat(registry.findDescriptor("pref.two.Dup")).isSameAs(second);
        assertThat(registry.findDescriptor("Dup")).isSameAs(first);
    }

    @Test
    void registeringAPreviouslyMissedTypeClearsItsNegativeCacheEntry() throws Exception {
        DescriptorRegistry registry = new DescriptorRegistry();
        CountingLoader loader = new CountingLoader();
        registry.addLoader(loader);

        assertThat(registry.findDescriptorByFullName("late.pkg.Late")).isNull();
        assertThat(loader.loadCalls).isEqualTo(1);

        // Direct registration of the missed name must be visible without re-consulting loaders.
        registry.register(message("late.pkg", "late/late.proto", "Late"));

        assertThat(registry.findDescriptorByFullName("late.pkg.Late")).isNotNull();
        assertThat(loader.loadCalls).isEqualTo(1);
    }

    /**
     * The negative-lookup cache is bounded at 1024 entries; on overflow it is reset wholesale,
     * so an older miss becomes re-resolvable through the loaders. This pins the reset so the
     * cache can never grow without limit.
     */
    @Test
    void negativeLookupCacheResetsAtItsBound() {
        DescriptorRegistry registry = new DescriptorRegistry();
        CountingLoader loader = new CountingLoader();
        registry.addLoader(loader);

        // 1024 misses fill the cache exactly; the 1025th trip consults the loader, then
        // clears the cache and records only the newest name.
        for (int i = 0; i < 1025; i++) {
            assertThat(registry.findDescriptorByFullName("missing.Type" + i)).isNull();
        }
        assertThat(loader.loadCalls).isEqualTo(1025);

        // Type0's miss was evicted by the reset: the loader is consulted again.
        assertThat(registry.findDescriptorByFullName("missing.Type0")).isNull();
        assertThat(loader.loadCalls).isEqualTo(1026);

        // Type1024 survived the reset: still answered from the cache.
        assertThat(registry.findDescriptorByFullName("missing.Type1024")).isNull();
        assertThat(loader.loadCalls).isEqualTo(1026);
    }

    @Test
    void concurrentRegistrationKeepsEveryType() throws Exception {
        DescriptorRegistry registry = new DescriptorRegistry();
        int initialSize = registry.size();
        int count = 200;

        try (var executor = Executors.newFixedThreadPool(8)) {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                final int n = i;
                futures.add(executor.submit(() -> {
                    registry.register(message("concurrent.pkg", "concurrent/c" + n + ".proto", "Type" + n));
                    return null;
                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        }

        assertThat(registry.size()).isEqualTo(initialSize + count);
        for (int i = 0; i < count; i++) {
            assertThat(registry.findDescriptorByFullName("concurrent.pkg.Type" + i)).isNotNull();
        }
    }

    // ---- fixtures ----

    private record EnumeratingLoader(List<FileDescriptor> files) implements DescriptorLoader {
        @Override
        public List<FileDescriptor> loadDescriptors() {
            return files;
        }

        @Override
        public FileDescriptor loadDescriptor(String fileName) {
            return null;
        }

        @Override
        public boolean isAvailable() {
            return true;
        }

        @Override
        public String getLoaderType() {
            return "enumerating-stub";
        }
    }

    private record FailingLoader(boolean available) implements DescriptorLoader {
        @Override
        public List<FileDescriptor> loadDescriptors() throws DescriptorLoadException {
            throw new DescriptorLoadException("load failed");
        }

        @Override
        public FileDescriptor loadDescriptor(String fileName) throws DescriptorLoadException {
            throw new DescriptorLoadException("load failed");
        }

        @Override
        public boolean isAvailable() {
            return available;
        }

        @Override
        public String getLoaderType() {
            return "failing-stub";
        }
    }

    private static final class UnavailableLoader implements DescriptorLoader {
        @Override
        public List<FileDescriptor> loadDescriptors() {
            throw new AssertionError("unavailable loader must not be consulted");
        }

        @Override
        public FileDescriptor loadDescriptor(String fileName) {
            throw new AssertionError("unavailable loader must not be consulted");
        }

        @Override
        public boolean isAvailable() {
            return false;
        }

        @Override
        public String getLoaderType() {
            return "unavailable-stub";
        }
    }

    private static final class CountingLoader implements DescriptorLoader {
        private int loadCalls = 0;

        @Override
        public List<FileDescriptor> loadDescriptors() {
            loadCalls++;
            return List.of();
        }

        @Override
        public FileDescriptor loadDescriptor(String fileName) {
            return null;
        }

        @Override
        public boolean isAvailable() {
            return true;
        }

        @Override
        public String getLoaderType() {
            return "counting";
        }
    }

    private static DescriptorProto simple(String name) {
        return DescriptorProto.newBuilder()
                .setName(name)
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("id").setNumber(1)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .build();
    }

    private static DescriptorProto parentWithNested(String parentName, String nestedName) {
        return DescriptorProto.newBuilder()
                .setName(parentName)
                .addNestedType(DescriptorProto.newBuilder().setName(nestedName))
                .build();
    }

    private static FileDescriptor file(String fileName, String packageName, DescriptorProto message)
            throws DescriptorValidationException {
        FileDescriptorProto fileProto = FileDescriptorProto.newBuilder()
                .setName(fileName)
                .setPackage(packageName)
                .addMessageType(message)
                .build();
        return FileDescriptor.buildFrom(fileProto, new FileDescriptor[0]);
    }

    private static Descriptor message(String packageName, String fileName, String messageName)
            throws DescriptorValidationException {
        return file(fileName, packageName, simple(messageName)).findMessageTypeByName(messageName);
    }
}
