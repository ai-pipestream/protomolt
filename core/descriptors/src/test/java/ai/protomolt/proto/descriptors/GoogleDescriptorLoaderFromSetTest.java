package ai.protomolt.proto.descriptors;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link GoogleDescriptorLoader#fromDescriptorSet} and multi-file descriptor-set
 * assembly: dependency order independence and the well-known-type fallback table. The
 * classpath-resource, cycle, and error-wrapping paths live in {@link GoogleDescriptorLoaderTest}.
 */
class GoogleDescriptorLoaderFromSetTest {

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
    void buildsFilesListedDependentBeforeDependency() throws Exception {
        // foo.proto depends on bar.proto but is listed FIRST in the set: the recursive build
        // must resolve bar.proto on the way through, not rely on set ordering.
        FileDescriptorProto bar = FileDescriptorProto.newBuilder()
                .setName("test/bar.proto")
                .setPackage("test.multi")
                .addMessageType(DescriptorProto.newBuilder().setName("Bar"))
                .build();
        FileDescriptorProto foo = FileDescriptorProto.newBuilder()
                .setName("test/foo.proto")
                .setPackage("test.multi")
                .addDependency("test/bar.proto")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Foo")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("bar")
                                .setNumber(1)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".test.multi.Bar")))
                .build();
        FileDescriptorSet set = FileDescriptorSet.newBuilder()
                .addFile(foo)
                .addFile(bar)
                .build();

        List<FileDescriptor> files = GoogleDescriptorLoader.fromDescriptorSet(set);

        assertThat(files).extracting(FileDescriptor::getName)
                .containsExactlyInAnyOrder("test/foo.proto", "test/bar.proto");
        Descriptor fooDescriptor = files.stream()
                .filter(fd -> fd.getName().equals("test/foo.proto"))
                .findFirst()
                .orElseThrow()
                .findMessageTypeByName("Foo");
        assertThat(fooDescriptor.findFieldByName("bar").getMessageType().getFullName())
                .isEqualTo("test.multi.Bar");
    }

    @Test
    void resolvesEveryWellKnownTypeFallback() throws Exception {
        // One field per well-known type the loader's fallback table claims to resolve.
        DescriptorProto.Builder message = DescriptorProto.newBuilder().setName("KitchenSink");
        String[][] fields = {
                {"any_field", "google/protobuf/any.proto", ".google.protobuf.Any"},
                {"struct_field", "google/protobuf/struct.proto", ".google.protobuf.Struct"},
                {"timestamp_field", "google/protobuf/timestamp.proto", ".google.protobuf.Timestamp"},
                {"duration_field", "google/protobuf/duration.proto", ".google.protobuf.Duration"},
                {"empty_field", "google/protobuf/empty.proto", ".google.protobuf.Empty"},
                {"mask_field", "google/protobuf/field_mask.proto", ".google.protobuf.FieldMask"},
                {"wrapper_field", "google/protobuf/wrappers.proto", ".google.protobuf.StringValue"},
        };
        FileDescriptorProto.Builder file = FileDescriptorProto.newBuilder()
                .setName("test/sink.proto")
                .setPackage("test.wkt");
        int number = 1;
        for (String[] field : fields) {
            file.addDependency(field[1]);
            message.addField(FieldDescriptorProto.newBuilder()
                    .setName(field[0])
                    .setNumber(number++)
                    .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
                    .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                    .setTypeName(field[2]));
        }
        FileDescriptorSet set = FileDescriptorSet.newBuilder()
                .addFile(file.addMessageType(message).build())
                .build();

        List<FileDescriptor> files = GoogleDescriptorLoader.fromDescriptorSet(set);

        assertThat(files).hasSize(1);
        Descriptor sink = files.get(0).findMessageTypeByName("KitchenSink");
        assertThat(sink).isNotNull();
        for (String[] field : fields) {
            assertThat(sink.findFieldByName(field[0]).getMessageType().getFullName())
                    .isEqualTo(field[2].substring(1));
        }
    }

    @Test
    void emptyDescriptorSetBuildsNoFiles() throws Exception {
        assertThat(GoogleDescriptorLoader.fromDescriptorSet(FileDescriptorSet.getDefaultInstance()))
                .isEmpty();
    }

    @Test
    void sameFileTwiceInSetBuildsOnce() throws Exception {
        FileDescriptorProto proto = FileDescriptorProto.newBuilder()
                .setName("test/dup.proto")
                .setPackage("test.dup")
                .addMessageType(DescriptorProto.newBuilder().setName("Dup"))
                .build();
        FileDescriptorSet set = FileDescriptorSet.newBuilder()
                .addFile(proto)
                .addFile(proto)
                .build();

        List<FileDescriptor> files = GoogleDescriptorLoader.fromDescriptorSet(set);

        assertThat(files).hasSize(1);
    }

    @Test
    void classpathLoaderServesSameSetAsFromDescriptorSet() throws Exception {
        // The resource-backed and in-memory entry points share the build machinery; the
        // descriptors they produce for the same bytes must agree.
        FileDescriptorProto proto = FileDescriptorProto.newBuilder()
                .setName("test/shared.proto")
                .setPackage("test.shared")
                .addMessageType(DescriptorProto.newBuilder().setName("Shared"))
                .build();
        FileDescriptorSet set = FileDescriptorSet.newBuilder().addFile(proto).build();

        String resourcePath = "test-descriptors/shared.dsc";
        GoogleDescriptorLoader classpath = new GoogleDescriptorLoader(
                resourcePath, new MapBackedClassLoader(Map.of(resourcePath, set.toByteArray())));

        assertThat(classpath.isAvailable()).isTrue();
        assertThat(classpath.loadDescriptors())
                .extracting(FileDescriptor::getName)
                .containsExactly("test/shared.proto");
        assertThat(GoogleDescriptorLoader.fromDescriptorSet(set))
                .extracting(FileDescriptor::getName)
                .containsExactly("test/shared.proto");
    }

    @Test
    void loadDescriptorForTypeUsesInterfaceDefaultEnumeration() throws Exception {
        // GoogleDescriptorLoader does not override loadDescriptorForType: the interface
        // default enumerates the set, so TYPE names resolve through it too.
        FileDescriptorProto proto = FileDescriptorProto.newBuilder()
                .setName("test/typed.proto")
                .setPackage("test.typed")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Outer")
                        .addNestedType(DescriptorProto.newBuilder().setName("Inner")))
                .build();
        FileDescriptorSet set = FileDescriptorSet.newBuilder().addFile(proto).build();

        String resourcePath = "test-descriptors/typed.dsc";
        GoogleDescriptorLoader loader = new GoogleDescriptorLoader(
                resourcePath, new MapBackedClassLoader(Map.of(resourcePath, set.toByteArray())));

        assertThat(loader.loadDescriptorForType("test.typed.Outer")).isNotNull();
        assertThat(loader.loadDescriptorForType("test.typed.Outer.Inner")).isNotNull();
        assertThat(loader.loadDescriptorForType("test.typed.Absent")).isNull();
    }
}
