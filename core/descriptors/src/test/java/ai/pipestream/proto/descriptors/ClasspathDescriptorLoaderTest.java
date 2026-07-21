package ai.pipestream.proto.descriptors;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ClasspathDescriptorLoaderTest {

    private static final String TYPE = "ai.pipestream.proto.descriptors.fixture.GeneratedLike";

    private final ClasspathDescriptorLoader loader = new ClasspathDescriptorLoader();

    @Test
    void loadsFileDescriptorForTypeOnTheClasspath() throws Exception {
        FileDescriptor fd = loader.loadDescriptorForType(TYPE);

        assertThat(fd).isNotNull();
        assertThat(fd.getMessageTypes()).extracting(Descriptor::getFullName).contains(TYPE);
    }

    @Test
    void loadsFileDescriptorForNestedType() throws Exception {
        FileDescriptor fd = loader.loadDescriptorForType(TYPE + ".Inner");

        assertThat(fd).isNotNull();
        assertThat(fd.getMessageTypes().get(0).getNestedTypes())
            .extracting(Descriptor::getFullName)
            .contains(TYPE + ".Inner");
    }

    @Test
    void returnsNullForTypeWithNoGeneratedClass() throws Exception {
        assertThat(loader.loadDescriptorForType("no.such.pkg.Missing")).isNull();
    }

    @Test
    void returnsNullForClassThatIsNotAProtobufMessage() throws Exception {
        assertThat(loader.loadDescriptorForType(String.class.getName())).isNull();
    }

    /**
     * The registry resolves on demand through {@code loadDescriptorForType}. A loader that only
     * implements {@code loadDescriptor} (or relies on the interface default, which enumerates
     * {@code loadDescriptors}) is inert once registered.
     */
    @Test
    void registryResolvesTypeThroughRegisteredLoader() {
        DescriptorRegistry registry = DescriptorRegistry.create();
        registry.addLoader(loader);

        Descriptor descriptor = registry.findDescriptorByFullName(TYPE);

        assertThat(descriptor).isNotNull();
        assertThat(descriptor.getFullName()).isEqualTo(TYPE);
        assertThat(registry.findDescriptorByFullName(TYPE + ".Inner")).isNotNull();
    }
}
