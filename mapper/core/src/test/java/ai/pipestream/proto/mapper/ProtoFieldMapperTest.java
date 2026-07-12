package ai.pipestream.proto.mapper;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import com.google.protobuf.*;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.google.protobuf.DescriptorProtos.DescriptorProto.newBuilder;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the ProtoFieldMapper class.
 * This test suite uses programmatically generated descriptors based on the provided
 * Document proto definition to test various mapping scenarios, including deep nesting,
 * struct manipulation, literal assignments, and error handling.
 */
public class ProtoFieldMapperTest {

    private static ProtoFieldMapperImpl mapper;
    private static Descriptor pipeStreamDescriptor;
    private static Descriptor documentDescriptor;

    @BeforeAll
    static void setUp() throws DescriptorValidationException {
        mapper = new ProtoFieldMapperImpl(new DescriptorRegistry());
        FileDescriptor fileDescriptor = createDocumentFileDescriptor();
        pipeStreamDescriptor = fileDescriptor.findMessageTypeByName("PipeStream");
        documentDescriptor = fileDescriptor.findMessageTypeByName("Document");
        assertNotNull(pipeStreamDescriptor, "PipeStream descriptor should not be null");
        assertNotNull(documentDescriptor, "Document descriptor should not be null");
    }

    /**
     * Creates a complex source message for testing, simulating a real-world PipeStream object.
     */
    private Message createSourcePipeStream() {
        Struct.Builder customDataBuilder = Struct.newBuilder();
        Struct.Builder infoBuilder = Struct.newBuilder();
        infoBuilder.putFields("version", Value.newBuilder().setStringValue("v1.2").build());
        infoBuilder.putFields("processed", Value.newBuilder().setBoolValue(true).build());

        customDataBuilder.putFields("source_system", Value.newBuilder().setStringValue("legacy-importer").build());
        customDataBuilder.putFields("run_id", Value.newBuilder().setNumberValue(98765).build());
        customDataBuilder.putFields("info", Value.newBuilder().setStructValue(infoBuilder).build());

        Descriptor semanticResultDesc = documentDescriptor.findFieldByName("semantic_results").getMessageType();
        Descriptor chunkDesc = semanticResultDesc.findFieldByName("chunks").getMessageType();
        Descriptor embeddingInfoDesc = chunkDesc.findFieldByName("embedding_info").getMessageType();

        Message chunk = DynamicMessage.newBuilder(chunkDesc)
                .setField(chunkDesc.findFieldByName("chunk_id"), "chunk-001")
                .setField(chunkDesc.findFieldByName("embedding_info"),
                        DynamicMessage.newBuilder(embeddingInfoDesc)
                                .setField(embeddingInfoDesc.findFieldByName("text_content"), "This is the first sentence.")
                                .build())
                .build();

        Message semanticResult = DynamicMessage.newBuilder(semanticResultDesc)
                .setField(semanticResultDesc.findFieldByName("result_id"), "res-abc")
                .setField(semanticResultDesc.findFieldByName("source_field_name"), "body")
                .addRepeatedField(semanticResultDesc.findFieldByName("chunks"), chunk)
                .build();

        Message sourceDocument = DynamicMessage.newBuilder(documentDescriptor)
                .setField(documentDescriptor.findFieldByName("id"), "doc-123")
                .setField(documentDescriptor.findFieldByName("title"), "Test Document Title")
                .setField(documentDescriptor.findFieldByName("body"), "Main body content.")
                .addRepeatedField(documentDescriptor.findFieldByName("keywords"), "java")
                .addRepeatedField(documentDescriptor.findFieldByName("keywords"), "protobuf")
                .setField(documentDescriptor.findFieldByName("custom_data"), customDataBuilder.build())
                .addRepeatedField(documentDescriptor.findFieldByName("semantic_results"), semanticResult)
                .build();

        return DynamicMessage.newBuilder(pipeStreamDescriptor)
                .setField(pipeStreamDescriptor.findFieldByName("stream_id"), "stream-xyz")
                .setField(pipeStreamDescriptor.findFieldByName("document"), sourceDocument)
                .build();
    }

    @Test
    void testSimpleAssignment() throws MappingException {
        Message source = createSourcePipeStream();
        Message.Builder targetBuilder = DynamicMessage.newBuilder(pipeStreamDescriptor);
        List<String> rules = Collections.singletonList("stream_id = stream_id");
        mapper.map(source, targetBuilder, rules);
        Message target = targetBuilder.build();
        assertEquals("stream-xyz", target.getField(pipeStreamDescriptor.findFieldByName("stream_id")));
    }

    @Test
    void testDeepAssignment() throws MappingException {
        Message source = createSourcePipeStream();
        Message.Builder targetBuilder = DynamicMessage.newBuilder(pipeStreamDescriptor);
        List<String> rules = Collections.singletonList("document.id = document.id");
        mapper.map(source, targetBuilder, rules);
        Message target = targetBuilder.build();
        Message targetDocument = (Message) target.getField(pipeStreamDescriptor.findFieldByName("document"));
        assertEquals("doc-123", targetDocument.getField(documentDescriptor.findFieldByName("id")));
    }

    @Test
    void testAssignmentToStruct() throws MappingException, InvalidProtocolBufferException {
        Message source = createSourcePipeStream();
        Message.Builder targetBuilder = DynamicMessage.newBuilder(documentDescriptor);
        List<String> rules = Collections.singletonList("custom_data.original_title = document.title");
        mapper.map(source, targetBuilder, rules);
        Message target = targetBuilder.build();

        // **FIXED HERE:** Safely convert the DynamicMessage field to a Struct before asserting.
        Message customDataMessage = (Message) target.getField(documentDescriptor.findFieldByName("custom_data"));
        Struct customData = Struct.parseFrom(customDataMessage.toByteString());

        Value titleValue = customData.getFieldsMap().get("original_title");
        assertNotNull(titleValue);
        assertEquals("Test Document Title", titleValue.getStringValue());
    }

    @Test
    void testAssignmentFromStruct() throws MappingException {
        Message source = createSourcePipeStream();
        Message.Builder targetBuilder = DynamicMessage.newBuilder(documentDescriptor);
        List<String> rules = Collections.singletonList("revision_id = document.custom_data.source_system");
        mapper.map(source, targetBuilder, rules);
        Message target = targetBuilder.build();
        assertEquals("legacy-importer", target.getField(documentDescriptor.findFieldByName("revision_id")));
    }

    @Test
    void testDeepAssignmentFromStruct() throws MappingException {
        Message source = createSourcePipeStream();
        Message.Builder targetBuilder = DynamicMessage.newBuilder(documentDescriptor);
        List<String> rules = Collections.singletonList("document_type = document.custom_data.info.version");
        mapper.map(source, targetBuilder, rules);
        Message target = targetBuilder.build();
        assertEquals("v1.2", target.getField(documentDescriptor.findFieldByName("document_type")));
    }

    @Test
    void testAppendToRepeatedField() throws MappingException {
        Message source = createSourcePipeStream();
        Message.Builder targetBuilder = DynamicMessage.newBuilder(documentDescriptor);
        List<String> rules = Collections.singletonList("keywords += document.keywords");
        mapper.map(source, targetBuilder, rules);
        Message target = targetBuilder.build();
        List<?> keywords = (List<?>) target.getField(documentDescriptor.findFieldByName("keywords"));
        assertEquals(Arrays.asList("java", "protobuf"), keywords);
    }

    @Test
    void testAppendComplexMessage() throws MappingException {
        Message source = createSourcePipeStream();
        Message.Builder targetBuilder = DynamicMessage.newBuilder(documentDescriptor);
        List<String> rules = Collections.singletonList("semantic_results += document.semantic_results");
        mapper.map(source, targetBuilder, rules);
        Message target = targetBuilder.build();
        FieldDescriptor semanticResultsField = documentDescriptor.findFieldByName("semantic_results");
        assertEquals(1, target.getRepeatedFieldCount(semanticResultsField));
        Message result = (Message) target.getRepeatedField(semanticResultsField, 0);
        assertEquals("res-abc", result.getField(semanticResultsField.getMessageType().findFieldByName("result_id")));
    }

    @Test
    void testClearField() throws MappingException {
        Message source = createSourcePipeStream();
        Message.Builder targetBuilder = source.toBuilder();
        List<String> rules = Collections.singletonList("-document.title");
        mapper.map(source, targetBuilder, rules);
        Message target = targetBuilder.build();
        Message targetDocument = (Message) target.getField(pipeStreamDescriptor.findFieldByName("document"));
        assertFalse(targetDocument.hasField(documentDescriptor.findFieldByName("title")));
        assertTrue(targetDocument.hasField(documentDescriptor.findFieldByName("id")));
    }

    @Test
    void testLiteralAssignments() throws MappingException, InvalidProtocolBufferException {
        Message source = createSourcePipeStream();
        Message.Builder targetBuilder = DynamicMessage.newBuilder(documentDescriptor);
        List<String> rules = Arrays.asList(
                "document_type = \"article\"",
                "custom_data.is_test = true",
                "custom_data.source_system = null",
                "custom_data.score = 123.45"
        );
        mapper.map(source, targetBuilder, rules);
        Message target = targetBuilder.build();
        assertEquals("article", target.getField(documentDescriptor.findFieldByName("document_type")));

        // **FIXED HERE:** Safely convert the DynamicMessage field to a Struct before asserting.
        Message customDataMessage = (Message) target.getField(documentDescriptor.findFieldByName("custom_data"));
        Struct customData = Struct.parseFrom(customDataMessage.toByteString());

        assertTrue(customData.getFieldsOrThrow("is_test").getBoolValue());
        assertEquals(123.45, customData.getFieldsOrThrow("score").getNumberValue(), 0.001);
        assertEquals(NullValue.NULL_VALUE, customData.getFieldsOrThrow("source_system").getNullValue());
    }

    @Test
    void testInvalidSourcePath() {
        Message source = createSourcePipeStream();
        Message.Builder targetBuilder = DynamicMessage.newBuilder(pipeStreamDescriptor);
        List<String> rules = Collections.singletonList("stream_id = document.invalid_path");
        Executable execution = () -> mapper.map(source, targetBuilder, rules);
        MappingException e = assertThrows(MappingException.class, execution);
        assertTrue(e.getMessage().contains("Field 'invalid_path' not found in message 'Document'"));
    }

    @Test
    void testInvalidTargetPath() {
        Message source = createSourcePipeStream();
        Message.Builder targetBuilder = DynamicMessage.newBuilder(pipeStreamDescriptor);
        List<String> rules = Collections.singletonList("document.invalid_target = stream_id");
        Executable execution = () -> mapper.map(source, targetBuilder, rules);
        MappingException e = assertThrows(MappingException.class, execution);
        assertTrue(e.getMessage().contains("Field 'invalid_target' not found in message 'Document'"));
    }

    private static FileDescriptor createDocumentFileDescriptor() throws DescriptorValidationException {
        FileDescriptor timestampFd = Timestamp.getDescriptor().getFile();
        FileDescriptor structFd = Struct.getDescriptor().getFile();
        DescriptorProto embeddingProto = newBuilder().setName("Embedding").build();
        DescriptorProto chunkEmbeddingProto = newBuilder().setName("ChunkEmbedding")
                .addField(FieldDescriptorProto.newBuilder().setName("text_content").setNumber(1).setType(FieldDescriptorProto.Type.TYPE_STRING))
                .build();
        DescriptorProto blobProto = newBuilder().setName("Blob").build();
        DescriptorProto semanticChunkProto = newBuilder().setName("SemanticChunk")
                .addField(FieldDescriptorProto.newBuilder().setName("chunk_id").setNumber(1).setType(FieldDescriptorProto.Type.TYPE_STRING))
                .addField(FieldDescriptorProto.newBuilder().setName("embedding_info").setNumber(3).setTypeName(".com.rokkon.search.model.util.ChunkEmbedding"))
                .build();
        DescriptorProto semanticResultProto = newBuilder().setName("SemanticProcessingResult")
                .addField(FieldDescriptorProto.newBuilder().setName("result_id").setNumber(1).setType(FieldDescriptorProto.Type.TYPE_STRING))
                .addField(FieldDescriptorProto.newBuilder().setName("source_field_name").setNumber(2).setType(FieldDescriptorProto.Type.TYPE_STRING))
                .addField(FieldDescriptorProto.newBuilder().setName("chunks").setNumber(6).setTypeName(".com.rokkon.search.model.util.SemanticChunk").setLabel(FieldDescriptorProto.Label.LABEL_REPEATED))
                .build();
        DescriptorProto documentProto = newBuilder().setName("Document")
                .addField(FieldDescriptorProto.newBuilder().setName("id").setNumber(1).setType(FieldDescriptorProto.Type.TYPE_STRING))
                .addField(FieldDescriptorProto.newBuilder().setName("title").setNumber(4).setType(FieldDescriptorProto.Type.TYPE_STRING).setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(FieldDescriptorProto.newBuilder().setName("body").setNumber(5).setType(FieldDescriptorProto.Type.TYPE_STRING).setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(FieldDescriptorProto.newBuilder().setName("keywords").setNumber(6).setType(FieldDescriptorProto.Type.TYPE_STRING).setLabel(FieldDescriptorProto.Label.LABEL_REPEATED))
                .addField(FieldDescriptorProto.newBuilder().setName("revision_id").setNumber(8).setType(FieldDescriptorProto.Type.TYPE_STRING).setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(FieldDescriptorProto.newBuilder().setName("document_type").setNumber(7).setType(FieldDescriptorProto.Type.TYPE_STRING).setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(FieldDescriptorProto.newBuilder().setName("custom_data").setNumber(12).setTypeName("google.protobuf.Struct").setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .addField(FieldDescriptorProto.newBuilder().setName("semantic_results").setNumber(13).setTypeName(".com.rokkon.search.model.util.SemanticProcessingResult").setLabel(FieldDescriptorProto.Label.LABEL_REPEATED))
                .addField(FieldDescriptorProto.newBuilder().setName("blob").setNumber(15).setTypeName(".com.rokkon.search.model.util.Blob").setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                .build();
        DescriptorProto pipeStreamProto = newBuilder().setName("PipeStream")
                .addField(FieldDescriptorProto.newBuilder().setName("stream_id").setNumber(1).setType(FieldDescriptorProto.Type.TYPE_STRING))
                .addField(FieldDescriptorProto.newBuilder().setName("document").setNumber(2).setTypeName(".com.rokkon.search.model.util.Document"))
                .build();
        DescriptorProtos.FileDescriptorProto fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("document.proto").setPackage("com.rokkon.search.model.util")
                .addDependency(timestampFd.getFullName()).addDependency(structFd.getFullName())
                .addMessageType(pipeStreamProto).addMessageType(documentProto)
                .addMessageType(embeddingProto).addMessageType(chunkEmbeddingProto)
                .addMessageType(blobProto).addMessageType(semanticChunkProto)
                .addMessageType(semanticResultProto).build();
        return FileDescriptor.buildFrom(fileProto, new FileDescriptor[]{timestampFd, structFd});
    }
}
