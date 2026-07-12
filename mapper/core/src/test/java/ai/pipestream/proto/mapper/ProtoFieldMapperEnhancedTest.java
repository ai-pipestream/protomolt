package ai.pipestream.proto.mapper;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import com.google.protobuf.*;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.google.protobuf.DescriptorProtos.DescriptorProto;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Enhanced test suite for ProtoFieldMapper focusing on:
 * - google.protobuf.Any packing/unpacking
 * - Type conversions
 * - Mapping structured_data (Any) to search_metadata
 * - Generic metadata mapping scenarios
 */
public class ProtoFieldMapperEnhancedTest {

    private static ProtoFieldMapperImpl mapper;
    private static Descriptor documentDescriptor;
    private static Descriptor searchMetadataDescriptor;
    private static Descriptor customMetadataDescriptor;

    @BeforeAll
    static void setUp() throws DescriptorValidationException {
        FileDescriptor fileDescriptor = createTestFileDescriptor();
        documentDescriptor = fileDescriptor.findMessageTypeByName("Document");
        searchMetadataDescriptor = fileDescriptor.findMessageTypeByName("SearchMetadata");
        customMetadataDescriptor = fileDescriptor.findMessageTypeByName("CustomMetadata");

        assertNotNull(documentDescriptor);
        assertNotNull(searchMetadataDescriptor);
        assertNotNull(customMetadataDescriptor);

        // Create mapper and register descriptors
        DescriptorRegistry registry = DescriptorRegistry.create();
        registry.registerFile(fileDescriptor);
        mapper = new ProtoFieldMapperImpl(registry);
    }

    @Test
    void testAnyPackingAndUnpacking() throws Exception {
        // Create a custom metadata message
        Message customMetadata = DynamicMessage.newBuilder(customMetadataDescriptor)
                .setField(customMetadataDescriptor.findFieldByName("source_system"), "test-system")
                .setField(customMetadataDescriptor.findFieldByName("version"), "1.0")
                .setField(customMetadataDescriptor.findFieldByName("priority"), 5L)
                .build();

        // Pack it into an Any
        Any packedAny = mapper.getAnyHandler().pack(customMetadata);
        assertNotNull(packedAny);
        assertTrue(packedAny.getTypeUrl().contains("CustomMetadata"));

        // Unpack it
        Message unpackedMetadata = mapper.getAnyHandler().unpack(packedAny);
        assertNotNull(unpackedMetadata);
        assertEquals("test-system", unpackedMetadata.getField(customMetadataDescriptor.findFieldByName("source_system")));
        assertEquals("1.0", unpackedMetadata.getField(customMetadataDescriptor.findFieldByName("version")));
        assertEquals(5L, unpackedMetadata.getField(customMetadataDescriptor.findFieldByName("priority")));
    }

    @Test
    void testMappingFromAnyToSearchMetadata() throws Exception {
        // Create custom metadata
        Message customMetadata = DynamicMessage.newBuilder(customMetadataDescriptor)
                .setField(customMetadataDescriptor.findFieldByName("source_system"), "legacy-system")
                .setField(customMetadataDescriptor.findFieldByName("version"), "2.1")
                .setField(customMetadataDescriptor.findFieldByName("priority"), 10L)
                .setField(customMetadataDescriptor.findFieldByName("document_title"), "Important Document")
                .build();

        // Create Document with structured_data as Any
        Any structuredData = Any.pack(customMetadata);
        Message sourceDoc = DynamicMessage.newBuilder(documentDescriptor)
                .setField(documentDescriptor.findFieldByName("doc_id"), "doc-456")
                .setField(documentDescriptor.findFieldByName("structured_data"), structuredData)
                .build();

        // Map from structured_data to search_metadata
        Message.Builder targetBuilder = DynamicMessage.newBuilder(documentDescriptor);
        List<String> rules = Arrays.asList(
                "search_metadata.title = structured_data.document_title",
                "search_metadata.document_type = structured_data.source_system"
        );

        mapper.map(sourceDoc, targetBuilder, rules);
        Message result = targetBuilder.build();

        // Verify the mapping worked
        Message searchMetadata = (Message) result.getField(documentDescriptor.findFieldByName("search_metadata"));
        assertNotNull(searchMetadata);
        assertEquals("Important Document", searchMetadata.getField(searchMetadataDescriptor.findFieldByName("title")));
        assertEquals("legacy-system", searchMetadata.getField(searchMetadataDescriptor.findFieldByName("document_type")));
    }

    @Test
    void testMappingToAnyField() throws Exception {
        // Create a search metadata message
        Message searchMetadata = DynamicMessage.newBuilder(searchMetadataDescriptor)
                .setField(searchMetadataDescriptor.findFieldByName("title"), "Test Title")
                .setField(searchMetadataDescriptor.findFieldByName("body"), "Test Body")
                .setField(searchMetadataDescriptor.findFieldByName("document_type"), "article")
                .build();

        // Create source Document with search_metadata
        Message sourceDoc = DynamicMessage.newBuilder(documentDescriptor)
                .setField(documentDescriptor.findFieldByName("doc_id"), "doc-789")
                .setField(documentDescriptor.findFieldByName("search_metadata"), searchMetadata)
                .build();

        // Map search_metadata to structured_data (Any field)
        Message.Builder targetBuilder = DynamicMessage.newBuilder(documentDescriptor);
        List<String> rules = Collections.singletonList(
                "structured_data = search_metadata"
        );

        mapper.map(sourceDoc, targetBuilder, rules);
        Message result = targetBuilder.build();

        // Verify the Any field was populated
        Any packedData = (Any) result.getField(documentDescriptor.findFieldByName("structured_data"));
        assertNotNull(packedData);
        assertTrue(packedData.getTypeUrl().contains("SearchMetadata"));

        // Unpack and verify
        Message unpackedMetadata = mapper.getAnyHandler().unpack(packedData);
        assertEquals("Test Title", unpackedMetadata.getField(searchMetadataDescriptor.findFieldByName("title")));
        assertEquals("Test Body", unpackedMetadata.getField(searchMetadataDescriptor.findFieldByName("body")));
    }

    @Test
    void testNestedAnyFieldAccess() throws Exception {
        // Create custom metadata with nested data
        Struct.Builder nestedData = Struct.newBuilder();
        nestedData.putFields("nested_key", Value.newBuilder().setStringValue("nested_value").build());

        Message customMetadata = DynamicMessage.newBuilder(customMetadataDescriptor)
                .setField(customMetadataDescriptor.findFieldByName("source_system"), "nested-test")
                .setField(customMetadataDescriptor.findFieldByName("version"), "3.0")
                .setField(customMetadataDescriptor.findFieldByName("priority"), 15L)
                .setField(customMetadataDescriptor.findFieldByName("extra_data"), nestedData.build())
                .build();

        Any structuredData = Any.pack(customMetadata);
        Message sourceDoc = DynamicMessage.newBuilder(documentDescriptor)
                .setField(documentDescriptor.findFieldByName("doc_id"), "doc-nested")
                .setField(documentDescriptor.findFieldByName("structured_data"), structuredData)
                .build();

        // Access nested field through Any
        Message.Builder targetBuilder = DynamicMessage.newBuilder(documentDescriptor);
        List<String> rules = Arrays.asList(
                "search_metadata.document_type = structured_data.source_system",
                "search_metadata.custom_fields.version = structured_data.version"
        );

        mapper.map(sourceDoc, targetBuilder, rules);
        Message result = targetBuilder.build();

        Message searchMetadata = (Message) result.getField(documentDescriptor.findFieldByName("search_metadata"));
        assertEquals("nested-test", searchMetadata.getField(searchMetadataDescriptor.findFieldByName("document_type")));
    }

    @Test
    void testTypeConversionInMapping() throws Exception {
        // Create custom metadata with various types
        Message customMetadata = DynamicMessage.newBuilder(customMetadataDescriptor)
                .setField(customMetadataDescriptor.findFieldByName("source_system"), "conversion-test")
                .setField(customMetadataDescriptor.findFieldByName("priority"), 42L)
                .build();

        Any structuredData = Any.pack(customMetadata);
        Message sourceDoc = DynamicMessage.newBuilder(documentDescriptor)
                .setField(documentDescriptor.findFieldByName("doc_id"), "doc-conversion")
                .setField(documentDescriptor.findFieldByName("structured_data"), structuredData)
                .build();

        // Map numeric field to string field (type conversion)
        Message.Builder targetBuilder = DynamicMessage.newBuilder(documentDescriptor);
        List<String> rules = Collections.singletonList(
                "search_metadata.document_type = structured_data.priority"
        );

        mapper.map(sourceDoc, targetBuilder, rules);
        Message result = targetBuilder.build();

        Message searchMetadata = (Message) result.getField(documentDescriptor.findFieldByName("search_metadata"));
        // The number should be converted to string
        assertEquals("42", searchMetadata.getField(searchMetadataDescriptor.findFieldByName("document_type")));
    }

    @Test
    void testAnyToStructMapping() throws Exception {
        // Create custom metadata
        Message customMetadata = DynamicMessage.newBuilder(customMetadataDescriptor)
                .setField(customMetadataDescriptor.findFieldByName("source_system"), "struct-test")
                .setField(customMetadataDescriptor.findFieldByName("version"), "4.0")
                .setField(customMetadataDescriptor.findFieldByName("priority"), 20L)
                .build();

        Any structuredData = Any.pack(customMetadata);
        Message sourceDoc = DynamicMessage.newBuilder(documentDescriptor)
                .setField(documentDescriptor.findFieldByName("doc_id"), "doc-struct")
                .setField(documentDescriptor.findFieldByName("structured_data"), structuredData)
                .build();

        // Map Any fields to Struct custom_fields
        Message.Builder targetBuilder = DynamicMessage.newBuilder(documentDescriptor);
        List<String> rules = Arrays.asList(
                "search_metadata.custom_fields.source = structured_data.source_system",
                "search_metadata.custom_fields.ver = structured_data.version",
                "search_metadata.custom_fields.pri = structured_data.priority"
        );

        mapper.map(sourceDoc, targetBuilder, rules);
        Message result = targetBuilder.build();

        Message searchMetadata = (Message) result.getField(documentDescriptor.findFieldByName("search_metadata"));
        Message customFieldsMsg = (Message) searchMetadata.getField(searchMetadataDescriptor.findFieldByName("custom_fields"));
        Struct customFields = Struct.parseFrom(customFieldsMsg.toByteString());

        assertEquals("struct-test", customFields.getFieldsOrThrow("source").getStringValue());
        assertEquals("4.0", customFields.getFieldsOrThrow("ver").getStringValue());
        assertEquals(20.0, customFields.getFieldsOrThrow("pri").getNumberValue(), 0.001);
    }

    @Test
    void testMultipleAnyMappings() throws Exception {
        // Create two different custom metadata messages
        Message customMetadata1 = DynamicMessage.newBuilder(customMetadataDescriptor)
                .setField(customMetadataDescriptor.findFieldByName("document_title"), "From Custom 1")
                .build();

        Message customMetadata2 = DynamicMessage.newBuilder(customMetadataDescriptor)
                .setField(customMetadataDescriptor.findFieldByName("source_system"), "system-2")
                .build();

        // Create Document with structured_data
        Any structuredData1 = Any.pack(customMetadata1);
        Message sourceDoc1 = DynamicMessage.newBuilder(documentDescriptor)
                .setField(documentDescriptor.findFieldByName("doc_id"), "doc-multi-1")
                .setField(documentDescriptor.findFieldByName("structured_data"), structuredData1)
                .build();

        Any structuredData2 = Any.pack(customMetadata2);
        Message sourceDoc2 = DynamicMessage.newBuilder(documentDescriptor)
                .setField(documentDescriptor.findFieldByName("doc_id"), "doc-multi-2")
                .setField(documentDescriptor.findFieldByName("structured_data"), structuredData2)
                .build();

        // Map from first document
        Message.Builder targetBuilder1 = DynamicMessage.newBuilder(documentDescriptor);
        mapper.map(sourceDoc1, targetBuilder1, Collections.singletonList(
                "search_metadata.title = structured_data.document_title"
        ));
        Message result1 = targetBuilder1.build();
        Message searchMetadata1 = (Message) result1.getField(documentDescriptor.findFieldByName("search_metadata"));
        assertEquals("From Custom 1", searchMetadata1.getField(searchMetadataDescriptor.findFieldByName("title")));

        // Map from second document
        Message.Builder targetBuilder2 = DynamicMessage.newBuilder(documentDescriptor);
        mapper.map(sourceDoc2, targetBuilder2, Collections.singletonList(
                "search_metadata.document_type = structured_data.source_system"
        ));
        Message result2 = targetBuilder2.build();
        Message searchMetadata2 = (Message) result2.getField(documentDescriptor.findFieldByName("search_metadata"));
        assertEquals("system-2", searchMetadata2.getField(searchMetadataDescriptor.findFieldByName("document_type")));
    }

    @Test
    void testAnyFieldWithInvalidType() {
        // Create an Any with a type not in the registry
        Any unknownAny = Any.newBuilder()
                .setTypeUrl("type.googleapis.com/unknown.UnknownType")
                .setValue(ByteString.copyFromUtf8("some data"))
                .build();

        Message sourceDoc = DynamicMessage.newBuilder(documentDescriptor)
                .setField(documentDescriptor.findFieldByName("doc_id"), "doc-unknown")
                .setField(documentDescriptor.findFieldByName("structured_data"), unknownAny)
                .build();

        // Try to map from the unknown Any - should fail gracefully
        Message.Builder targetBuilder = DynamicMessage.newBuilder(documentDescriptor);
        List<String> rules = Collections.singletonList(
                "search_metadata.title = structured_data.some_field"
        );

        assertThrows(MappingException.class, () ->
                mapper.map(sourceDoc, targetBuilder, rules));
    }

    /**
     * Creates a test file descriptor with Document, SearchMetadata, and CustomMetadata types.
     */
    private static FileDescriptor createTestFileDescriptor() throws DescriptorValidationException {
        FileDescriptor timestampFd = Timestamp.getDescriptor().getFile();
        FileDescriptor structFd = Struct.getDescriptor().getFile();
        FileDescriptor anyFd = Any.getDescriptor().getFile();

        // CustomMetadata message (represents customer's custom proto type)
        DescriptorProto customMetadataProto = DescriptorProto.newBuilder()
                .setName("CustomMetadata")
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("source_system").setNumber(1)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("version").setNumber(2)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("priority").setNumber(3)
                        .setType(FieldDescriptorProto.Type.TYPE_INT64))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("document_title").setNumber(4)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("extra_data").setNumber(5)
                        .setTypeName("google.protobuf.Struct"))
                .build();

        // SearchMetadata message
        DescriptorProto searchMetadataProto = DescriptorProto.newBuilder()
                .setName("SearchMetadata")
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("title").setNumber(1)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("body").setNumber(2)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("document_type").setNumber(3)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("custom_fields").setNumber(4)
                        .setTypeName("google.protobuf.Struct")
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .build();

        // Document message
        DescriptorProto documentProto = DescriptorProto.newBuilder()
                .setName("Document")
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("doc_id").setNumber(1)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("search_metadata").setNumber(2)
                        .setTypeName(".ai.pipestream.test.SearchMetadata")
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("structured_data").setNumber(3)
                        .setTypeName("google.protobuf.Any")
                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .build();

        DescriptorProtos.FileDescriptorProto fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("test_document.proto")
                .setPackage("ai.pipestream.test")
                .addDependency(timestampFd.getFullName())
                .addDependency(structFd.getFullName())
                .addDependency(anyFd.getFullName())
                .addMessageType(customMetadataProto)
                .addMessageType(searchMetadataProto)
                .addMessageType(documentProto)
                .build();

        return FileDescriptor.buildFrom(fileProto, new FileDescriptor[]{timestampFd, structFd, anyFd});
    }
}
