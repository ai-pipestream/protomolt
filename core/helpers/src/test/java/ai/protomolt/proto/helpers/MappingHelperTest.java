package ai.protomolt.proto.helpers;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.protobuf.DescriptorProtos.DescriptorProto;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for MappingHelper - demonstrates frontend mapper use cases.
 * <p>
 * Example: Building a UI to map Document → OpenSearchDocument
 */
public class MappingHelperTest {

    private static MappingHelper helper;
    private static Descriptor documentDescriptor;
    private static Descriptor openSearchDocDescriptor;

    @BeforeAll
    static void setUp() throws DescriptorValidationException {
        helper = new MappingHelper();
        FileDescriptor fileDescriptor = createTestFileDescriptor();
        documentDescriptor = fileDescriptor.findMessageTypeByName("Document");
        openSearchDocDescriptor = fileDescriptor.findMessageTypeByName("OpenSearchDocument");
    }

    @Test
    void testGetAllFieldPaths() {
        // Get all available fields for Document (for source dropdown in UI)
        List<String> sourceFields = helper.getAllFieldPaths(documentDescriptor, 3);

        assertNotNull(sourceFields);
        assertTrue(sourceFields.contains("doc_id"));
        assertTrue(sourceFields.contains("search_metadata"));
        assertTrue(sourceFields.contains("search_metadata.title"));
        assertTrue(sourceFields.contains("search_metadata.body"));
        assertTrue(sourceFields.contains("search_metadata.custom_fields"));

        // Get all available fields for OpenSearchDocument (for target dropdown)
        List<String> targetFields = helper.getAllFieldPaths(openSearchDocDescriptor, 3);

        assertTrue(targetFields.contains("original_doc_id"));
        assertTrue(targetFields.contains("title"));
        assertTrue(targetFields.contains("body"));
        assertTrue(targetFields.contains("custom_fields"));
    }

    @Test
    void testGetFieldInfos() {
        // Get detailed field information for rendering in UI
        List<MappingHelper.FieldInfo> fields = helper.getFieldInfos(documentDescriptor, 2);

        assertNotNull(fields);
        assertFalse(fields.isEmpty());

        // Find the doc_id field
        MappingHelper.FieldInfo docIdField = fields.stream()
            .filter(f -> f.path.equals("doc_id"))
            .findFirst()
            .orElse(null);

        assertNotNull(docIdField);
        assertEquals("doc_id", docIdField.name);
        assertEquals("STRING", docIdField.type);
        assertFalse(docIdField.repeated);
        assertEquals(0, docIdField.depth);
    }

    @Test
    void testValidateRule_ValidMapping() {
        // User creates mapping: original_doc_id = doc_id
        String rule = "original_doc_id = doc_id";

        MappingHelper.ValidationResult result = helper.validateRule(
            rule,
            documentDescriptor,
            openSearchDocDescriptor
        );

        assertTrue(result.isValid);
        assertEquals(MappingHelper.ValidationResult.ValidationLevel.SUCCESS, result.level);
    }

    @Test
    void testValidateRule_InvalidSourceField() {
        // User tries to map from non-existent field
        String rule = "title = search_metadata.nonexistent_field";

        MappingHelper.ValidationResult result = helper.validateRule(
            rule,
            documentDescriptor,
            openSearchDocDescriptor
        );

        assertFalse(result.isValid);
        assertTrue(result.message.contains("nonexistent_field"));
    }

    @Test
    void testValidateRule_InvalidTargetField() {
        // User tries to map to non-existent field
        String rule = "nonexistent_target = doc_id";

        MappingHelper.ValidationResult result = helper.validateRule(
            rule,
            documentDescriptor,
            openSearchDocDescriptor
        );

        assertFalse(result.isValid);
        assertTrue(result.message.contains("nonexistent_target"));
    }

    @Test
    void testValidateRule_TrailingDotInSourcePathRejected() {
        // "doc_id." must NOT validate as "doc_id": trailing empty segments are errors.
        MappingHelper.ValidationResult result = helper.validateRule(
            "original_doc_id = doc_id.",
            documentDescriptor,
            openSearchDocDescriptor
        );

        assertFalse(result.isValid);
        assertTrue(result.message.contains("empty segment"));
    }

    @Test
    void testValidateRule_EmptyMiddleSegmentRejected() {
        MappingHelper.ValidationResult result = helper.validateRule(
            "title = search_metadata..title",
            documentDescriptor,
            openSearchDocDescriptor
        );

        assertFalse(result.isValid);
        assertTrue(result.message.contains("empty segment"));
    }

    @Test
    void testValidateRule_TrailingDotInTargetPathRejected() {
        MappingHelper.ValidationResult result = helper.validateRule(
            "original_doc_id. = doc_id",
            documentDescriptor,
            openSearchDocDescriptor
        );

        assertFalse(result.isValid);
        assertTrue(result.message.contains("empty segment"));
    }

    @Test
    void testValidateRule_ClearRule() {
        // Clear rules carry no source path; validation covers the target only.
        MappingHelper.ValidationResult result = helper.validateRule(
            "-title",
            documentDescriptor,
            openSearchDocDescriptor
        );

        assertTrue(result.isValid, () -> "expected a valid clear rule but got: " + result.message);
        assertEquals(MappingHelper.ValidationResult.ValidationLevel.SUCCESS, result.level);
    }

    @Test
    void testValidateRule_ClearRuleWithUnknownTarget() {
        MappingHelper.ValidationResult result = helper.validateRule(
            "-nonexistent_target",
            documentDescriptor,
            openSearchDocDescriptor
        );

        assertFalse(result.isValid);
        assertTrue(result.message.contains("nonexistent_target"));
    }

    @Test
    void testValidateRule_NestedMapping() {
        // User creates nested mapping
        String rule = "title = search_metadata.title";

        MappingHelper.ValidationResult result = helper.validateRule(
            rule,
            documentDescriptor,
            openSearchDocDescriptor
        );

        assertTrue(result.isValid);
    }

    @Test
    void testValidateRule_LiteralValue() {
        // User assigns a literal value
        String rule = "doc_type = \"article\"";

        MappingHelper.ValidationResult result = helper.validateRule(
            rule,
            documentDescriptor,
            openSearchDocDescriptor
        );

        assertTrue(result.isValid);
    }

    @Test
    void testSuggestTargetFields() {
        // User selects "doc_id" as source field
        // UI should suggest compatible target fields
        List<MappingHelper.FieldSuggestion> suggestions = helper.suggestTargetFields(
            "doc_id",
            documentDescriptor,
            openSearchDocDescriptor
        );

        assertNotNull(suggestions);
        assertFalse(suggestions.isEmpty());

        // Should suggest "original_doc_id" with high score
        MappingHelper.FieldSuggestion topSuggestion = suggestions.get(0);
        assertTrue(topSuggestion.score > 0);

        // "doc_id" contains "doc" so should match fields with "doc" in name
        boolean hasDocField = suggestions.stream()
            .anyMatch(s -> s.fieldPath.contains("doc"));
        assertTrue(hasDocField);
    }

    @Test
    void testSuggestTargetFields_ExactNameMatch() {
        // When field names match exactly, should get very high score
        List<MappingHelper.FieldSuggestion> suggestions = helper.suggestTargetFields(
            "search_metadata.title",
            documentDescriptor,
            openSearchDocDescriptor
        );

        assertNotNull(suggestions);
        assertFalse(suggestions.isEmpty());

        // "title" should be top suggestion with high score
        MappingHelper.FieldSuggestion topSuggestion = suggestions.get(0);
        assertEquals("title", topSuggestion.fieldPath);
        assertTrue(topSuggestion.score > 100); // Should have bonus for exact name match
    }

    @Test
    void testExportSchema() {
        // Export schema as tree for UI rendering
        MappingHelper.SchemaNode schema = helper.exportSchema(documentDescriptor, 2);

        assertNotNull(schema);
        assertEquals("Document", schema.name);
        assertFalse(schema.children.isEmpty());

        // Should have doc_id field
        boolean hasDocId = schema.children.stream()
            .anyMatch(child -> child.name.equals("doc_id"));
        assertTrue(hasDocId);

        // Should have search_metadata with nested fields
        MappingHelper.SchemaNode searchMetadata = schema.children.stream()
            .filter(child -> child.name.equals("search_metadata"))
            .findFirst()
            .orElse(null);

        assertNotNull(searchMetadata);
        assertFalse(searchMetadata.children.isEmpty());
    }

    @Test
    void testRealWorldMappingScenario() {
        // Simulate a user building a mapper in a UI

        // Step 1: Get all source fields for dropdown
        List<String> sourceFields = helper.getAllFieldPaths(documentDescriptor, 3);
        assertTrue(sourceFields.size() > 0);

        // Step 2: Get all target fields for dropdown
        List<String> targetFields = helper.getAllFieldPaths(openSearchDocDescriptor, 3);
        assertTrue(targetFields.size() > 0);

        // Step 3: User selects source field "doc_id"
        String selectedSource = "doc_id";

        // Step 4: Get suggestions for target field
        List<MappingHelper.FieldSuggestion> suggestions = helper.suggestTargetFields(
            selectedSource,
            documentDescriptor,
            openSearchDocDescriptor
        );

        assertTrue(suggestions.size() > 0);

        // Step 5: User selects suggested target field
        String selectedTarget = suggestions.get(0).fieldPath;

        // Step 6: Build and validate the rule
        String rule = selectedTarget + " = " + selectedSource;
        MappingHelper.ValidationResult validation = helper.validateRule(
            rule,
            documentDescriptor,
            openSearchDocDescriptor
        );

        assertTrue(validation.isValid);

        // Step 7: User can now use this rule with ProtoFieldMapper
        // mapper.map(doc, openSearchDocBuilder, List.of(rule));
    }

    @Test
    void testRepeatedMapPathsInvalidMappingsAndNumericLiterals() throws Exception {
        DescriptorProto itemProto = DescriptorProto.newBuilder()
                .setName("Item")
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("code").setNumber(1)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING))
                .build();
        DescriptorProto attrEntryProto = DescriptorProto.newBuilder()
                .setName("AttrEntry")
                .setOptions(com.google.protobuf.DescriptorProtos.MessageOptions.newBuilder()
                        .setMapEntry(true))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("key").setNumber(1)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("value").setNumber(2)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING))
                .build();
        DescriptorProto richDocProto = DescriptorProto.newBuilder()
                .setName("RichDoc")
                .addNestedType(attrEntryProto)
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("items").setNumber(1)
                        .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                        .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName(".ai.protomolt.test.rich.Item"))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("attrs").setNumber(2)
                        .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                        .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName(".ai.protomolt.test.rich.RichDoc.AttrEntry"))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("count").setNumber(3)
                        .setType(FieldDescriptorProto.Type.TYPE_INT32))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("active").setNumber(4)
                        .setType(FieldDescriptorProto.Type.TYPE_BOOL))
                .build();
        FileDescriptor richFd = FileDescriptor.buildFrom(
                com.google.protobuf.DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName("rich_mapping.proto")
                        .setPackage("ai.protomolt.test.rich")
                        .addMessageType(itemProto)
                        .addMessageType(richDocProto)
                        .build(),
                new FileDescriptor[0]);
        Descriptor richDoc = richFd.findMessageTypeByName("RichDoc");

        // Repeated and map fields appear as leaves without traversing into repeated children
        List<String> paths = helper.getAllFieldPaths(richDoc, 3);
        assertEquals(List.of("active", "attrs", "count", "items"), paths);

        // Traversing through a repeated message or scalar field is rejected
        MappingHelper.ValidationResult throughRepeated =
                helper.validateRule("title = items.code", richDoc, openSearchDocDescriptor);
        assertFalse(throughRepeated.isValid);
        assertTrue(throughRepeated.message.contains("Cannot traverse through repeated field 'items'"));

        MappingHelper.ValidationResult throughScalar =
                helper.validateRule("title = count.sub", richDoc, openSearchDocDescriptor);
        assertFalse(throughScalar.isValid);
        assertTrue(throughScalar.message.contains("Field 'count' is not a message type"));

        // Numeric literals (positive, negative, decimal) and append rules validate
        assertTrue(helper.validateRule("count = -42", documentDescriptor, richDoc).isValid);
        assertTrue(helper.validateRule("count = 3.14", documentDescriptor, richDoc).isValid);
        assertTrue(helper.validateRule("tags += doc_id", documentDescriptor, openSearchDocDescriptor).isValid);

        // Malformed numeric token is treated as a source path and rejected
        MappingHelper.ValidationResult badNumber =
                helper.validateRule("count = 12.34.56", documentDescriptor, richDoc);
        assertFalse(badNumber.isValid);

        // Incompatible non-string types produce a WARNING
        MappingHelper.ValidationResult warning =
                helper.validateRule("count = active", richDoc, richDoc);
        assertTrue(warning.isValid);
        assertEquals(MappingHelper.ValidationResult.ValidationLevel.WARNING, warning.level);
    }

    /**
     * Creates test file descriptor with Document and OpenSearchDocument types.
     */
    private static FileDescriptor createTestFileDescriptor() throws DescriptorValidationException {
        FileDescriptor structFd = com.google.protobuf.Struct.getDescriptor().getFile();
        FileDescriptor timestampFd = com.google.protobuf.Timestamp.getDescriptor().getFile();

        // SearchMetadata
        DescriptorProto searchMetadataProto = DescriptorProto.newBuilder()
            .setName("SearchMetadata")
            .addField(FieldDescriptorProto.newBuilder()
                .setName("title").setNumber(1)
                .setType(FieldDescriptorProto.Type.TYPE_STRING))
            .addField(FieldDescriptorProto.newBuilder()
                .setName("body").setNumber(2)
                .setType(FieldDescriptorProto.Type.TYPE_STRING))
            .addField(FieldDescriptorProto.newBuilder()
                .setName("document_type").setNumber(3)
                .setType(FieldDescriptorProto.Type.TYPE_STRING))
            .addField(FieldDescriptorProto.newBuilder()
                .setName("custom_fields").setNumber(4)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName("google.protobuf.Struct"))
            .build();

        // Document
        DescriptorProto documentProto = DescriptorProto.newBuilder()
            .setName("Document")
            .addField(FieldDescriptorProto.newBuilder()
                .setName("doc_id").setNumber(1)
                .setType(FieldDescriptorProto.Type.TYPE_STRING))
            .addField(FieldDescriptorProto.newBuilder()
                .setName("search_metadata").setNumber(2)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(".ai.protomolt.test.SearchMetadata"))
            .build();

        // OpenSearchDocument
        DescriptorProto openSearchDocProto = DescriptorProto.newBuilder()
            .setName("OpenSearchDocument")
            .addField(FieldDescriptorProto.newBuilder()
                .setName("original_doc_id").setNumber(1)
                .setType(FieldDescriptorProto.Type.TYPE_STRING))
            .addField(FieldDescriptorProto.newBuilder()
                .setName("doc_type").setNumber(2)
                .setType(FieldDescriptorProto.Type.TYPE_STRING))
            .addField(FieldDescriptorProto.newBuilder()
                .setName("title").setNumber(3)
                .setType(FieldDescriptorProto.Type.TYPE_STRING))
            .addField(FieldDescriptorProto.newBuilder()
                .setName("body").setNumber(4)
                .setType(FieldDescriptorProto.Type.TYPE_STRING))
            .addField(FieldDescriptorProto.newBuilder()
                .setName("tags").setNumber(5)
                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED))
            .addField(FieldDescriptorProto.newBuilder()
                .setName("custom_fields").setNumber(6)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName("google.protobuf.Struct"))
            .addField(FieldDescriptorProto.newBuilder()
                .setName("created_at").setNumber(7)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName("google.protobuf.Timestamp"))
            .build();

        com.google.protobuf.DescriptorProtos.FileDescriptorProto fileProto =
            com.google.protobuf.DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("test_mapping.proto")
                .setPackage("ai.protomolt.test")
                .addDependency(structFd.getFullName())
                .addDependency(timestampFd.getFullName())
                .addMessageType(searchMetadataProto)
                .addMessageType(documentProto)
                .addMessageType(openSearchDocProto)
                .build();

        return FileDescriptor.buildFrom(fileProto, new FileDescriptor[]{structFd, timestampFd});
    }
}
