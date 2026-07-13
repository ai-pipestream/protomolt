package ai.pipestream.proto.descriptors;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import static com.google.protobuf.DescriptorProtos.DescriptorProto;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the DescriptorRegistry class.
 */
public class DescriptorRegistryTest {

    @Test
    void testRegisterAndFindByFullName() throws DescriptorValidationException {
        DescriptorRegistry registry = new DescriptorRegistry();
        FileDescriptor fd = createTestFileDescriptor();
        Descriptor testDescriptor = fd.findMessageTypeByName("TestMessage");

        registry.register(testDescriptor);

        Descriptor found = registry.findDescriptorByFullName("ai.pipestream.test.TestMessage");
        assertNotNull(found);
        assertEquals(testDescriptor, found);
    }

    @Test
    void testRegisterAndFindBySimpleName() throws DescriptorValidationException {
        DescriptorRegistry registry = new DescriptorRegistry();
        FileDescriptor fd = createTestFileDescriptor();
        Descriptor testDescriptor = fd.findMessageTypeByName("TestMessage");

        registry.register(testDescriptor);

        Descriptor found = registry.findDescriptorBySimpleName("TestMessage");
        assertNotNull(found);
        assertEquals(testDescriptor, found);
    }

    @Test
    void testRegisterFile() throws DescriptorValidationException {
        DescriptorRegistry registry = new DescriptorRegistry();
        FileDescriptor fd = createTestFileDescriptor();

        registry.registerFile(fd);

        Descriptor found = registry.findDescriptorByFullName("ai.pipestream.test.TestMessage");
        assertNotNull(found);
    }

    @Test
    void testRegisterFromMessage() throws DescriptorValidationException {
        DescriptorRegistry registry = new DescriptorRegistry();
        FileDescriptor fd = createTestFileDescriptor();
        Descriptor testDescriptor = fd.findMessageTypeByName("TestMessage");

        Message message = DynamicMessage.newBuilder(testDescriptor)
                .setField(testDescriptor.findFieldByName("name"), "test")
                .build();

        registry.registerFromMessage(message);

        Descriptor found = registry.findDescriptorByFullName("ai.pipestream.test.TestMessage");
        assertNotNull(found);
        assertEquals(testDescriptor, found);
    }

    @Test
    void testFindDescriptor() throws DescriptorValidationException {
        DescriptorRegistry registry = new DescriptorRegistry();
        FileDescriptor fd = createTestFileDescriptor();
        Descriptor testDescriptor = fd.findMessageTypeByName("TestMessage");

        registry.register(testDescriptor);

        // Find by full name
        Descriptor foundByFullName = registry.findDescriptor("ai.pipestream.test.TestMessage");
        assertNotNull(foundByFullName);

        // Find by simple name
        Descriptor foundBySimpleName = registry.findDescriptor("TestMessage");
        assertNotNull(foundBySimpleName);

        // Not found
        Descriptor notFound = registry.findDescriptor("NonExistent");
        assertNull(notFound);
    }

    @Test
    void testIsRegistered() throws DescriptorValidationException {
        DescriptorRegistry registry = new DescriptorRegistry();
        FileDescriptor fd = createTestFileDescriptor();
        Descriptor testDescriptor = fd.findMessageTypeByName("TestMessage");

        assertFalse(registry.isRegistered("ai.pipestream.test.TestMessage"));

        registry.register(testDescriptor);

        assertTrue(registry.isRegistered("ai.pipestream.test.TestMessage"));
    }

    @Test
    void testWellKnownTypes() {
        DescriptorRegistry registry = new DescriptorRegistry();

        // Well-known types should be registered by default
        assertNotNull(registry.findDescriptorByFullName("google.protobuf.Struct"));
        assertNotNull(registry.findDescriptorByFullName("google.protobuf.Value"));
        assertNotNull(registry.findDescriptorByFullName("google.protobuf.Timestamp"));
        assertNotNull(registry.findDescriptorByFullName("google.protobuf.Any"));
    }

    @Test
    void testClear() throws DescriptorValidationException {
        DescriptorRegistry registry = new DescriptorRegistry();
        FileDescriptor fd = createTestFileDescriptor();
        Descriptor testDescriptor = fd.findMessageTypeByName("TestMessage");

        registry.register(testDescriptor);
        assertTrue(registry.isRegistered("ai.pipestream.test.TestMessage"));

        registry.clear();

        // Custom types should be cleared
        assertFalse(registry.isRegistered("ai.pipestream.test.TestMessage"));

        // Well-known types should still be there
        assertNotNull(registry.findDescriptorByFullName("google.protobuf.Struct"));
    }

    @Test
    void testSize() throws DescriptorValidationException {
        DescriptorRegistry registry = new DescriptorRegistry();

        // Should have well-known types
        int initialSize = registry.size();
        assertTrue(initialSize > 0);

        FileDescriptor fd = createTestFileDescriptor();
        Descriptor testDescriptor = fd.findMessageTypeByName("TestMessage");
        registry.register(testDescriptor);

        assertEquals(initialSize + 1, registry.size());
    }

    @Test
    void testBuilder() throws DescriptorValidationException {
        FileDescriptor fd = createTestFileDescriptor();
        Descriptor testDescriptor = fd.findMessageTypeByName("TestMessage");

        Message message = DynamicMessage.newBuilder(testDescriptor)
                .setField(testDescriptor.findFieldByName("name"), "test")
                .build();

        DescriptorRegistry registry = DescriptorRegistry.builder()
                .register(testDescriptor)
                .registerFromMessage(message)
                .registerFile(fd)
                .build();

        assertNotNull(registry.findDescriptorByFullName("ai.pipestream.test.TestMessage"));
    }

    @Test
    void testNestedTypes() throws DescriptorValidationException {
        FileDescriptor fd = createNestedFileDescriptor();

        DescriptorRegistry registry = new DescriptorRegistry();
        registry.registerFile(fd);

        // Parent type
        Descriptor parentDescriptor = registry.findDescriptorByFullName("ai.pipestream.test.Parent");
        assertNotNull(parentDescriptor);

        // Nested type
        Descriptor nestedDescriptor = registry.findDescriptorByFullName("ai.pipestream.test.Parent.Nested");
        assertNotNull(nestedDescriptor);
    }

    @Test
    void testSimpleNameCollisionKeepsFirstRegistration() throws DescriptorValidationException {
        DescriptorRegistry registry = new DescriptorRegistry();

        Descriptor first = createMessageInPackage("pkg.one", "one.proto", "Dup");
        Descriptor second = createMessageInPackage("pkg.two", "two.proto", "Dup");

        registry.register(first);
        registry.register(second);

        // Full-name lookups see both types.
        assertSame(first, registry.findDescriptorByFullName("pkg.one.Dup"));
        assertSame(second, registry.findDescriptorByFullName("pkg.two.Dup"));

        // Simple-name lookup deterministically returns the FIRST registration.
        assertSame(first, registry.findDescriptorBySimpleName("Dup"));
    }

    @Test
    void testReRegisteringSameTypeUpdatesSimpleNameMapping() throws DescriptorValidationException {
        DescriptorRegistry registry = new DescriptorRegistry();

        Descriptor original = createMessageInPackage("pkg.one", "one.proto", "Same");
        Descriptor rebuilt = createMessageInPackage("pkg.one", "one.proto", "Same");

        registry.register(original);
        registry.register(rebuilt);

        // Same full name is not a collision: the latest instance wins for both lookups.
        assertSame(rebuilt, registry.findDescriptorByFullName("pkg.one.Same"));
        assertSame(rebuilt, registry.findDescriptorBySimpleName("Same"));
    }

    private static Descriptor createMessageInPackage(String packageName, String fileName, String messageName)
            throws DescriptorValidationException {
        DescriptorProto messageProto = DescriptorProto.newBuilder()
                .setName(messageName)
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("id").setNumber(1)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING))
                .build();

        DescriptorProtos.FileDescriptorProto fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName(fileName)
                .setPackage(packageName)
                .addMessageType(messageProto)
                .build();

        return FileDescriptor.buildFrom(fileProto, new FileDescriptor[]{})
                .findMessageTypeByName(messageName);
    }

    private FileDescriptor createTestFileDescriptor() throws DescriptorValidationException {
        DescriptorProto testMessageProto = DescriptorProto.newBuilder()
                .setName("TestMessage")
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("name").setNumber(1)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("count").setNumber(2)
                        .setType(FieldDescriptorProto.Type.TYPE_INT64))
                .build();

        DescriptorProtos.FileDescriptorProto fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("test_registry.proto")
                .setPackage("ai.pipestream.test")
                .addMessageType(testMessageProto)
                .build();

        return FileDescriptor.buildFrom(fileProto, new FileDescriptor[]{});
    }

    private FileDescriptor createNestedFileDescriptor() throws DescriptorValidationException {
        DescriptorProto nestedProto = DescriptorProto.newBuilder()
                .setName("Nested")
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("value").setNumber(1)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING))
                .build();

        DescriptorProto parentProto = DescriptorProto.newBuilder()
                .setName("Parent")
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("id").setNumber(1)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING))
                .addNestedType(nestedProto)
                .build();

        DescriptorProtos.FileDescriptorProto fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("test_nested.proto")
                .setPackage("ai.pipestream.test")
                .addMessageType(parentProto)
                .build();

        return FileDescriptor.buildFrom(fileProto, new FileDescriptor[]{});
    }
}
