package ai.pipestream.proto.descriptors;

import ai.pipestream.proto.descriptors.DescriptorLoader;
import ai.pipestream.proto.descriptors.GoogleDescriptorLoader;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for GoogleDescriptorLoader.
 */
public class GoogleDescriptorLoaderTest {

    private static final class MapBackedClassLoader extends ClassLoader {
        private final Map<String, byte[]> resources;

        private MapBackedClassLoader(Map<String, byte[]> resources) {
            super(MapBackedClassLoader.class.getClassLoader());
            this.resources = resources;
        }

        @Override
        public InputStream getResourceAsStream(String name) {
            byte[] bytes = resources.get(name);
            if (bytes != null) {
                return new ByteArrayInputStream(bytes);
            }
            return super.getResourceAsStream(name);
        }
    }

    @Test
    void testGetLoaderType() {
        GoogleDescriptorLoader loader = new GoogleDescriptorLoader();
        assertEquals("Google Descriptor File", loader.getLoaderType());
    }

    @Test
    void testGetDescriptorPath() {
        String customPath = "custom/path/descriptors.dsc";
        GoogleDescriptorLoader loader = new GoogleDescriptorLoader(customPath);
        assertEquals(customPath, loader.getDescriptorPath());
    }

    @Test
    void testDefaultDescriptorPath() {
        GoogleDescriptorLoader loader = new GoogleDescriptorLoader();
        assertEquals(GoogleDescriptorLoader.DEFAULT_DESCRIPTOR_PATH, loader.getDescriptorPath());
    }

    @Test
    void testLoadDescriptors_resolvesWellKnownDependencyFromRuntime() throws Exception {
        // Build a minimal descriptor set containing one file that depends on struct.proto,
        // but do NOT include struct.proto in the set. The loader should resolve it via
        // tryGetWellKnownType(...) using protobuf runtime descriptors.
        FileDescriptorProto fooProto = FileDescriptorProto.newBuilder()
            .setName("test/foo.proto")
            .setPackage("test")
            .addDependency("google/protobuf/struct.proto")
            .addMessageType(
                DescriptorProto.newBuilder()
                    .setName("Foo")
                    .addField(
                        FieldDescriptorProto.newBuilder()
                            .setName("metadata")
                            .setNumber(1)
                            .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                            .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                            .setTypeName(".google.protobuf.Struct")
                    )
            )
            .build();

        FileDescriptorSet set = FileDescriptorSet.newBuilder()
            .addFile(fooProto)
            .build();

        String resourcePath = "test-descriptors/services.dsc";
        ClassLoader cl = new MapBackedClassLoader(Map.of(resourcePath, set.toByteArray()));
        GoogleDescriptorLoader loader = new GoogleDescriptorLoader(resourcePath, cl);

        assertTrue(loader.isAvailable());

        List<FileDescriptor> descriptors = loader.loadDescriptors();
        assertEquals(1, descriptors.size());

        FileDescriptor fd = loader.loadDescriptor("test/foo.proto");
        assertNotNull(fd);
        assertEquals("test/foo.proto", fd.getName());

        Descriptors.Descriptor foo = fd.findMessageTypeByName("Foo");
        assertNotNull(foo);

        Descriptors.FieldDescriptor metadata = foo.findFieldByName("metadata");
        assertNotNull(metadata);
        assertEquals(Descriptors.FieldDescriptor.Type.MESSAGE, metadata.getType());
        assertEquals("google.protobuf.Struct", metadata.getMessageType().getFullName());
    }

    @Test
    void testLoadDescriptorByFileName() throws Exception {
        FileDescriptorProto fooProto = FileDescriptorProto.newBuilder()
            .setName("test/foo.proto")
            .setPackage("test")
            .build();

        FileDescriptorSet set = FileDescriptorSet.newBuilder()
            .addFile(fooProto)
            .build();

        String resourcePath = "test-descriptors/services.dsc";
        ClassLoader cl = new MapBackedClassLoader(Map.of(resourcePath, set.toByteArray()));
        GoogleDescriptorLoader loader = new GoogleDescriptorLoader(resourcePath, cl);

        assertNotNull(loader.loadDescriptor("test/foo.proto"));
        assertNull(loader.loadDescriptor("nonexistent.proto"));
    }

    @Test
    void testLoadNonExistentDescriptor() throws Exception {
        FileDescriptorProto fooProto = FileDescriptorProto.newBuilder()
            .setName("test/foo.proto")
            .setPackage("test")
            .build();

        FileDescriptorSet set = FileDescriptorSet.newBuilder()
            .addFile(fooProto)
            .build();

        String resourcePath = "test-descriptors/services.dsc";
        ClassLoader cl = new MapBackedClassLoader(Map.of(resourcePath, set.toByteArray()));
        GoogleDescriptorLoader loader = new GoogleDescriptorLoader(resourcePath, cl);

        FileDescriptor fd = loader.loadDescriptor("nonexistent.proto");
        assertNull(fd, "Should return null for non-existent descriptor");
    }

    @Test
    void testInvalidDescriptorPath() {
        GoogleDescriptorLoader loader = new GoogleDescriptorLoader("invalid/path.dsc");

        assertFalse(loader.isAvailable());
        assertThrows(DescriptorLoader.DescriptorLoadException.class, () -> {
            loader.loadDescriptors();
        });
    }

    @Test
    void testMissingDependencyWrappedAsDescriptorLoadException() {
        // Create a file descriptor that depends on something we cannot resolve.
        // This should become a DescriptorLoadException (not an unchecked IllegalStateException).
        FileDescriptorProto badProto = FileDescriptorProto.newBuilder()
            .setName("test/bad.proto")
            .setPackage("test")
            .addDependency("missing/does_not_exist.proto")
            .addMessageType(
                DescriptorProto.newBuilder()
                    .setName("Bad")
                    .addField(
                        FieldDescriptorProto.newBuilder()
                            .setName("x")
                            .setNumber(1)
                            .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                            .setType(FieldDescriptorProto.Type.TYPE_STRING)
                    )
            )
            .build();

        FileDescriptorSet set = FileDescriptorSet.newBuilder()
            .addFile(badProto)
            .build();

        String resourcePath = "test-descriptors/bad.dsc";
        ClassLoader cl = new MapBackedClassLoader(Map.of(resourcePath, set.toByteArray()));
        GoogleDescriptorLoader loader = new GoogleDescriptorLoader(resourcePath, cl);

        DescriptorLoader.DescriptorLoadException ex =
            assertThrows(DescriptorLoader.DescriptorLoadException.class, loader::loadDescriptors);
        assertTrue(ex.getMessage().contains("Failed to build descriptors"));
    }

    @Test
    void testSelfDependencyCycleThrowsDescriptorLoadException() {
        FileDescriptorProto selfProto = FileDescriptorProto.newBuilder()
            .setName("test/self.proto")
            .setPackage("test")
            .addDependency("test/self.proto")
            .build();

        FileDescriptorSet set = FileDescriptorSet.newBuilder()
            .addFile(selfProto)
            .build();

        String resourcePath = "test-descriptors/self-cycle.dsc";
        ClassLoader cl = new MapBackedClassLoader(Map.of(resourcePath, set.toByteArray()));
        GoogleDescriptorLoader loader = new GoogleDescriptorLoader(resourcePath, cl);

        DescriptorLoader.DescriptorLoadException ex =
            assertThrows(DescriptorLoader.DescriptorLoadException.class, loader::loadDescriptors);
        assertTrue(ex.getMessage().contains("dependency cycle"), ex.getMessage());
        assertTrue(ex.getMessage().contains("test/self.proto"), ex.getMessage());
    }

    @Test
    void testTwoFileDependencyCycleThrowsDescriptorLoadException() {
        FileDescriptorProto aProto = FileDescriptorProto.newBuilder()
            .setName("test/a.proto")
            .setPackage("test")
            .addDependency("test/b.proto")
            .build();
        FileDescriptorProto bProto = FileDescriptorProto.newBuilder()
            .setName("test/b.proto")
            .setPackage("test")
            .addDependency("test/a.proto")
            .build();

        FileDescriptorSet set = FileDescriptorSet.newBuilder()
            .addFile(aProto)
            .addFile(bProto)
            .build();

        String resourcePath = "test-descriptors/two-cycle.dsc";
        ClassLoader cl = new MapBackedClassLoader(Map.of(resourcePath, set.toByteArray()));
        GoogleDescriptorLoader loader = new GoogleDescriptorLoader(resourcePath, cl);

        DescriptorLoader.DescriptorLoadException ex =
            assertThrows(DescriptorLoader.DescriptorLoadException.class, loader::loadDescriptors);
        assertTrue(ex.getMessage().contains("dependency cycle"), ex.getMessage());
        assertTrue(ex.getMessage().contains("test/a.proto"), ex.getMessage());
        assertTrue(ex.getMessage().contains("test/b.proto"), ex.getMessage());

        // The static descriptor-set entry point must honor the same contract.
        DescriptorLoader.DescriptorLoadException fromSetEx =
            assertThrows(DescriptorLoader.DescriptorLoadException.class,
                () -> GoogleDescriptorLoader.fromDescriptorSet(set));
        assertTrue(fromSetEx.getMessage().contains("dependency cycle"), fromSetEx.getMessage());
    }

    @Test
    void testSearchPaths() {
        GoogleDescriptorLoader loader = GoogleDescriptorLoader.searchPaths(
            "invalid/path1.dsc",
            "invalid/path2.dsc",
            GoogleDescriptorLoader.DEFAULT_DESCRIPTOR_PATH
        );

        assertNotNull(loader);
        // If the default path is available, the loader should find it
        // Otherwise, it will use the first invalid path
        assertNotNull(loader.getDescriptorPath());
    }

    @Test
    void testSearchPathsWithNoValidPaths() {
        GoogleDescriptorLoader loader = GoogleDescriptorLoader.searchPaths(
            "invalid/path1.dsc",
            "invalid/path2.dsc"
        );

        assertNotNull(loader);
        assertEquals("invalid/path1.dsc", loader.getDescriptorPath());
    }

    @Test
    void testSearchPathsWithNoPathsThrows() {
        assertThrows(IllegalArgumentException.class, () -> GoogleDescriptorLoader.searchPaths());
    }

    @Test
    void testSearchPathsWithNullContextClassLoader() {
        Thread current = Thread.currentThread();
        ClassLoader original = current.getContextClassLoader();
        current.setContextClassLoader(null);
        try {
            GoogleDescriptorLoader loader = GoogleDescriptorLoader.searchPaths("invalid/path1.dsc");
            assertNotNull(loader);
            assertEquals("invalid/path1.dsc", loader.getDescriptorPath());
        } finally {
            current.setContextClassLoader(original);
        }
    }

    @Test
    void testCustomClassLoader() {
        ClassLoader customLoader = Thread.currentThread().getContextClassLoader();
        GoogleDescriptorLoader loader = new GoogleDescriptorLoader(
            GoogleDescriptorLoader.DEFAULT_DESCRIPTOR_PATH,
            customLoader
        );

        assertNotNull(loader);
    }

    @Test
    void testNullClassLoaderFallback() {
        GoogleDescriptorLoader loader = new GoogleDescriptorLoader(
            GoogleDescriptorLoader.DEFAULT_DESCRIPTOR_PATH,
            null
        );

        assertNotNull(loader);
        // Should use system class loader as fallback
    }
}
