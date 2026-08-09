package ai.pipestream.proto.descriptors;

import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Edge cases for {@link ClasspathDescriptorLoader}: the deliberate non-enumeration contract,
 * null/empty inputs, the {@code loadDescriptor} delegation, and classloader fallback.
 * Happy-path resolution lives in {@link ClasspathDescriptorLoaderTest}.
 */
class ClasspathDescriptorLoaderEdgeCasesTest {

    private static final String TYPE = "ai.pipestream.proto.descriptors.fixture.GeneratedLike";
    private static final String DEEP_TYPE = "ai.pipestream.proto.descriptors.fixture.Deep";

    private final ClasspathDescriptorLoader loader = new ClasspathDescriptorLoader();

    @Test
    void returnsNullForNullTypeName() throws Exception {
        assertThat(loader.loadDescriptorForType(null)).isNull();
    }

    @Test
    void returnsNullForEmptyTypeName() throws Exception {
        assertThat(loader.loadDescriptorForType("")).isNull();
    }

    /** Enumeration is deliberately unsupported: classpath scanning is too expensive. */
    @Test
    void loadDescriptorsIsDeliberatelyEmpty() throws Exception {
        assertThat(loader.loadDescriptors()).isEmpty();
    }

    /**
     * {@code loadDescriptor} takes a proto FILE name by contract, but this loader has no file
     * index and delegates to type resolution, so a type name passed by mistake is still served.
     */
    @Test
    void loadDescriptorDelegatesToTypeResolution() throws Exception {
        FileDescriptor fd = loader.loadDescriptor(TYPE);

        assertThat(fd).isNotNull();
        assertThat(fd.getName()).isEqualTo("fixture/generated_like.proto");
    }

    @Test
    void loadDescriptorReturnsNullForUnknownName() throws Exception {
        assertThat(loader.loadDescriptor("no/such/file.proto")).isNull();
    }

    @Test
    void resolvesTypeNestedTwoLevelsDeep() throws Exception {
        FileDescriptor fd = loader.loadDescriptorForType(DEEP_TYPE + ".Mid.Leaf");

        assertThat(fd).isNotNull();
        assertThat(fd.getMessageTypes().get(0).getFullName()).isEqualTo(DEEP_TYPE);
    }

    @Test
    void resolvesUsingOwnClassLoaderWhenContextClassLoaderIsNull() throws Exception {
        Thread current = Thread.currentThread();
        ClassLoader original = current.getContextClassLoader();
        current.setContextClassLoader(null);
        try {
            assertThat(loader.loadDescriptorForType(TYPE)).isNotNull();
        } finally {
            current.setContextClassLoader(original);
        }
    }

    @Test
    void isAlwaysAvailable() {
        assertThat(loader.isAvailable()).isTrue();
    }

    @Test
    void reportsLoaderType() {
        assertThat(loader.getLoaderType()).isEqualTo("Classpath Class Resolver");
    }
}
