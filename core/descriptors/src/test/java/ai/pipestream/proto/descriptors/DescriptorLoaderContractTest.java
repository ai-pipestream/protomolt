package ai.pipestream.proto.descriptors;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the {@link DescriptorLoader} interface contract itself: the default
 * {@code loadDescriptorForType} enumeration (which nested-type lookup included) and the
 * {@link DescriptorLoader.DescriptorLoadException} carriers. Implementations under test
 * elsewhere override the default; these tests pin what the default promises.
 */
class DescriptorLoaderContractTest {

    /** Enumerates files and relies on the interface default for type lookup. */
    private record EnumeratingLoader(List<FileDescriptor> files) implements DescriptorLoader {
        @Override
        public List<FileDescriptor> loadDescriptors() {
            return files;
        }

        @Override
        public FileDescriptor loadDescriptor(String fileName) {
            return files.stream()
                    .filter(fd -> fd.getName().equals(fileName))
                    .findFirst()
                    .orElse(null);
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

    @Test
    void defaultTypeLookupFindsTopLevelMessage() throws Exception {
        EnumeratingLoader loader = new EnumeratingLoader(List.of(nestedFile()));

        FileDescriptor fd = loader.loadDescriptorForType("contract.pkg.Parent");

        assertThat(fd).isNotNull();
        assertThat(fd.getName()).isEqualTo("contract/nested.proto");
    }

    @Test
    void defaultTypeLookupFindsNestedAndDoublyNestedMessages() throws Exception {
        EnumeratingLoader loader = new EnumeratingLoader(List.of(nestedFile()));

        assertThat(loader.loadDescriptorForType("contract.pkg.Parent.Inner")).isNotNull();
        assertThat(loader.loadDescriptorForType("contract.pkg.Parent.Inner.Leaf")).isNotNull();
    }

    @Test
    void defaultTypeLookupReturnsNullWhenNoFileDefinesTheType() throws Exception {
        EnumeratingLoader loader = new EnumeratingLoader(List.of(nestedFile()));

        assertThat(loader.loadDescriptorForType("contract.pkg.Absent")).isNull();
        // Prefix/suffix near-misses must not match either.
        assertThat(loader.loadDescriptorForType("contract.pkg.Parent.Inner.Leaf.Extra")).isNull();
        assertThat(loader.loadDescriptorForType("Parent")).isNull();
    }

    @Test
    void defaultTypeLookupReturnsNullWhenLoaderHasNoFiles() throws Exception {
        EnumeratingLoader loader = new EnumeratingLoader(List.of());

        assertThat(loader.loadDescriptorForType("anything.AtAll")).isNull();
    }

    @Test
    void loadExceptionCarriesMessageOnly() {
        DescriptorLoader.DescriptorLoadException ex =
                new DescriptorLoader.DescriptorLoadException("boom");

        assertThat(ex.getMessage()).isEqualTo("boom");
        assertThat(ex.getCause()).isNull();
    }

    @Test
    void loadExceptionCarriesMessageAndCause() {
        RuntimeException cause = new RuntimeException("root");
        DescriptorLoader.DescriptorLoadException ex =
                new DescriptorLoader.DescriptorLoadException("boom", cause);

        assertThat(ex.getMessage()).isEqualTo("boom");
        assertThat(ex.getCause()).isSameAs(cause);
    }

    private static FileDescriptor nestedFile() throws DescriptorValidationException {
        DescriptorProto leaf = DescriptorProto.newBuilder().setName("Leaf").build();
        DescriptorProto inner = DescriptorProto.newBuilder()
                .setName("Inner")
                .addNestedType(leaf)
                .build();
        DescriptorProto parent = DescriptorProto.newBuilder()
                .setName("Parent")
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("id").setNumber(1)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addNestedType(inner)
                .build();
        FileDescriptorProto fileProto = FileDescriptorProto.newBuilder()
                .setName("contract/nested.proto")
                .setPackage("contract.pkg")
                .addMessageType(parent)
                .build();
        return FileDescriptor.buildFrom(fileProto, new FileDescriptor[0]);
    }
}
