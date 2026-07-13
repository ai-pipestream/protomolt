package ai.pipestream.proto.descriptors;

import ai.pipestream.proto.descriptors.DescriptorLoader;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.descriptors.GoogleDescriptorLoader;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for DescriptorRegistry integration with DescriptorLoaders.
 */
public class DescriptorRegistryLoaderTest {

    @Test
    void testAutoLoadConstructor() {
        DescriptorRegistry registry = DescriptorRegistry.create(true);
        assertNotNull(registry);

        // Should have well-known types at minimum
        assertNotNull(registry.findDescriptorByFullName("google.protobuf.Struct"));
        assertNotNull(registry.findDescriptorByFullName("google.protobuf.Any"));

        // If descriptor file is available, should have loaded additional types
        int size = registry.size();
        assertTrue(size > 0);
    }

    @Test
    void testAddLoaderAndAutoLoad() throws Exception {
        DescriptorRegistry registry = new DescriptorRegistry();
        int initialSize = registry.size();

        GoogleDescriptorLoader loader = new GoogleDescriptorLoader();
        registry.addLoader(loader);

        if (loader.isAvailable()) {
            registry.autoLoadDescriptors();
            
            // Registry should now have more descriptors
            int newSize = registry.size();
            assertTrue(newSize >= initialSize);
        }
    }

    @Test
    void testBuilderWithGoogleDescriptorLoader() {
        DescriptorRegistry registry = DescriptorRegistry.builder()
            .withGoogleDescriptorLoader()
            .build();

        assertNotNull(registry);
        // Well-known types should be registered
        assertNotNull(registry.findDescriptorByFullName("google.protobuf.Struct"));
    }

    @Test
    void testBuilderWithCustomPath() {
        DescriptorRegistry registry = DescriptorRegistry.builder()
            .withGoogleDescriptorLoader("custom/path.dsc")
            .build();

        assertNotNull(registry);
    }

    @Test
    void testBuilderWithAutoLoad() {
        DescriptorRegistry registry = DescriptorRegistry.builder()
            .withGoogleDescriptorLoader()
            .withAutoLoad()
            .build();

        assertNotNull(registry);
        // Should have attempted to load well-known types at least
        assertNotNull(registry.findDescriptorByFullName("google.protobuf.Struct"));
    }

    @Test
    void testAutoLoadIsIdempotent() {
        DescriptorRegistry registry = new DescriptorRegistry();
        int initialSize = registry.size();

        // Call autoLoad multiple times
        registry.autoLoadDescriptors();
        int afterFirstLoad = registry.size();

        registry.autoLoadDescriptors();
        int afterSecondLoad = registry.size();

        registry.autoLoadDescriptors();
        int afterThirdLoad = registry.size();

        // Size should be stable after first load
        assertEquals(afterFirstLoad, afterSecondLoad);
        assertEquals(afterSecondLoad, afterThirdLoad);
    }

    @Test
    void testAddMultipleLoaders() {
        DescriptorRegistry registry = new DescriptorRegistry();

        GoogleDescriptorLoader loader1 = new GoogleDescriptorLoader();
        GoogleDescriptorLoader loader2 = new GoogleDescriptorLoader("custom/path.dsc");

        registry.addLoader(loader1);
        registry.addLoader(loader2);

        // Should not throw
        registry.autoLoadDescriptors();
    }

    @Test
    void testResolveOnDemandReturnsNullWhenLoadedFileLacksType() throws Exception {
        DescriptorRegistry registry = new DescriptorRegistry();
        FileDescriptor unrelatedFile = createFileDescriptor("unrelated.proto", "test.pkg", "Unrelated");
        registry.addLoader(new StubLoader(unrelatedFile));

        // Loader returns a file that does not contain the requested type;
        // this must return null instead of recursing forever (StackOverflowError).
        assertNull(registry.findDescriptorByFullName("com.example.Missing"));

        // The unrelated file's types should still have been registered.
        assertNotNull(registry.findDescriptorByFullName("test.pkg.Unrelated"));
    }

    @Test
    void testLoaderAddedAfterSimpleNameMissIsStillAutoLoaded() throws Exception {
        DescriptorRegistry registry = new DescriptorRegistry();

        // Miss with zero loaders — this must not permanently latch auto-loading off.
        assertNull(registry.findDescriptorBySimpleName("LateType"));

        FileDescriptor lateFile = createFileDescriptor("late.proto", "test.pkg", "LateType");
        registry.addLoader(new StubLoader(lateFile));

        assertNotNull(registry.findDescriptorBySimpleName("LateType"));
    }

    @Test
    void testResolveOnDemandWithGoogleDescriptorLoader() throws Exception {
        // The registry must be able to resolve a TYPE name on demand through a
        // GoogleDescriptorLoader, whose loadDescriptor(name) matches proto FILE names only.
        DescriptorProtos.FileDescriptorProto fooProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("test/on_demand.proto")
                .setPackage("test.ondemand")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("OnDemand")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("id").setNumber(1)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)))
                .build();
        DescriptorProtos.FileDescriptorSet set = DescriptorProtos.FileDescriptorSet.newBuilder()
                .addFile(fooProto)
                .build();

        String resourcePath = "test-descriptors/on-demand.dsc";
        ClassLoader cl = new MapBackedClassLoader(java.util.Map.of(resourcePath, set.toByteArray()));
        GoogleDescriptorLoader loader = new GoogleDescriptorLoader(resourcePath, cl);

        DescriptorRegistry registry = new DescriptorRegistry();
        registry.addLoader(loader);

        assertNotNull(registry.findDescriptorByFullName("test.ondemand.OnDemand"));
    }

    @Test
    void testRepeatedMissHitsLoaderOnlyOnce() {
        DescriptorRegistry registry = new DescriptorRegistry();
        CountingLoader loader = new CountingLoader();
        registry.addLoader(loader);

        assertNull(registry.findDescriptorByFullName("com.example.Missing"));
        assertEquals(1, loader.loadCalls, "first miss should consult the loader exactly once");

        assertNull(registry.findDescriptorByFullName("com.example.Missing"));
        assertEquals(1, loader.loadCalls, "repeated miss must be answered from the negative cache");
    }

    @Test
    void testNegativeCacheClearedByAddLoader() throws Exception {
        DescriptorRegistry registry = new DescriptorRegistry();

        // Miss once so the name lands in the negative cache.
        assertNull(registry.findDescriptorByFullName("test.pkg.LateFull"));

        // A newly added loader may supply the type: the cached miss must be forgotten.
        FileDescriptor lateFile = createFileDescriptor("late_full.proto", "test.pkg", "LateFull");
        registry.addLoader(new StubLoader(lateFile));

        assertNotNull(registry.findDescriptorByFullName("test.pkg.LateFull"));
    }

    @Test
    void testClearResetsAutoLoadSoDescriptorsAreReloadable() throws Exception {
        DescriptorRegistry registry = new DescriptorRegistry();
        FileDescriptor file = createFileDescriptor("reload.proto", "test.pkg", "Reloadable");
        registry.addLoader(new StubLoader(file));
        registry.autoLoadDescriptors();
        assertNotNull(registry.findDescriptorBySimpleName("Reloadable"));

        registry.clear();

        // Simple-name lookup relies on auto-loading; clear() must re-arm it.
        assertNotNull(registry.findDescriptorBySimpleName("Reloadable"));
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

    private static final class MapBackedClassLoader extends ClassLoader {
        private final java.util.Map<String, byte[]> resources;

        private MapBackedClassLoader(java.util.Map<String, byte[]> resources) {
            super(MapBackedClassLoader.class.getClassLoader());
            this.resources = resources;
        }

        @Override
        public java.io.InputStream getResourceAsStream(String name) {
            byte[] bytes = resources.get(name);
            if (bytes != null) {
                return new java.io.ByteArrayInputStream(bytes);
            }
            return super.getResourceAsStream(name);
        }
    }

    private static FileDescriptor createFileDescriptor(String fileName, String packageName, String messageName)
            throws DescriptorValidationException {
        DescriptorProtos.FileDescriptorProto fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName(fileName)
                .setPackage(packageName)
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                        .setName(messageName)
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                .setName("id").setNumber(1)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)))
                .build();
        return FileDescriptor.buildFrom(fileProto, new FileDescriptor[]{});
    }

    private record StubLoader(FileDescriptor fileDescriptor) implements DescriptorLoader {

        @Override
        public List<FileDescriptor> loadDescriptors() {
            return List.of(fileDescriptor);
        }

        @Override
        public FileDescriptor loadDescriptor(String fileName) {
            return fileDescriptor;
        }

        @Override
        public boolean isAvailable() {
            return true;
        }

        @Override
        public String getLoaderType() {
            return "stub";
        }
    }

    @Test
    void testAddNullLoader() {
        DescriptorRegistry registry = new DescriptorRegistry();
        int initialSize = registry.size();

        registry.addLoader(null);

        // Should handle null gracefully
        assertEquals(initialSize, registry.size());
    }
}
