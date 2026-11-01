package ai.protomolt.proto.helpers;

import com.google.protobuf.DescriptorProtos.*;
import com.google.protobuf.Descriptors.*;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MappingHelperBoundaryTest {
    private final MappingHelper helper = new MappingHelper();

    @Test
    void discoveryKeepsSortedPathsButDescriptorOrderForMetadata() throws Exception {
        Descriptor root = descriptor();
        assertEquals(List.of("attributes", "first", "first.name", "second", "second.name", "self", "values"),
                helper.getAllFieldPaths(root, 5));
        var infos = helper.getFieldInfos(root, 5);
        assertEquals(List.of("second", "second.name", "first", "first.name", "values", "attributes", "self"),
                infos.stream().map(i -> i.path).toList());
        assertEquals(List.of(0, 1, 0, 1, 0, 0, 0), infos.stream().map(i -> i.depth).toList());
        assertTrue(infos.get(4).repeated);
        assertTrue(infos.get(5).repeated);
        assertEquals(List.of("attributes", "first", "second", "self", "values"),
                helper.getAllFieldPaths(root, 0));
        assertTrue(helper.getAllFieldPaths(root, -1).isEmpty());
    }

    @Test
    void traversalErrorsAndTargetFirstValidationRemainExact() throws Exception {
        Descriptor root = descriptor();
        assertEquals("Invalid target path 'values.name': Cannot traverse through repeated field 'values'",
                helper.validateRule("values.name = missing", root, root).message);
        assertEquals("Invalid source path 'attributes.key': Cannot traverse through repeated field 'attributes'",
                helper.validateRule("first.name = attributes.key", root, root).message);
        assertEquals("Invalid source path 'first.name.child': Field 'name' is not a message type",
                helper.validateRule("first.name = first.name.child", root, root).message);
        assertTrue(helper.validateRule("first.name = second.name", root, root).isValid);
        assertTrue(helper.validateRule("-first", null, root).isValid);
        assertEquals("Invalid rule syntax: first", helper.validateRule("first", root, root).message);
    }

    @Test
    void mismatchIsWarningOnlyForAssignment() throws Exception {
        Descriptor root = descriptor();
        Descriptor timestamp = Timestamp.getDescriptor();
        var assignment = helper.validateRule("seconds = first", root, timestamp);
        assertTrue(assignment.isValid);
        assertEquals(MappingHelper.ValidationResult.ValidationLevel.WARNING, assignment.level);
        assertEquals("Type mismatch: MESSAGE → LONG (will attempt conversion)", assignment.message);
        assertEquals(MappingHelper.ValidationResult.ValidationLevel.SUCCESS,
                helper.validateRule("seconds += first", root, timestamp).level);
    }

    @Test
    void suggestionsKeepScoreThenAlphabeticalTieOrder() throws Exception {
        Descriptor root = descriptor();
        var suggestions = helper.suggestTargetFields("first.name", root, root);
        assertEquals(List.of("first.name", "second.name", "first", "second", "self", "attributes", "values"),
                suggestions.stream().map(s -> s.fieldPath).toList());
        assertEquals(List.of(170, 170, 70, 70, 70, 50, 50), suggestions.stream().map(s -> s.score).toList());
        assertTrue(helper.suggestTargetFields("missing", root, root).isEmpty());
    }

    @Test
    void resolutionRetainsDescriptorIdentityAcrossSameNameSchemas() throws Exception {
        Descriptor first = descriptor();
        Descriptor second = descriptor();
        assertNotSame(first, second);
        assertSame(first.findFieldByName("first").getMessageType().findFieldByName("name"),
                MappingPathSupport.validatePath("first.name", first).fieldDescriptor());
        assertSame(second.findFieldByName("first").getMessageType().findFieldByName("name"),
                MappingPathSupport.validatePath("first.name", second).fieldDescriptor());
    }

    @Test
    void schemaStopsAtDepthAndCyclesAndLeavesRepeatedAndMapFieldsAsLeaves() throws Exception {
        var schema = helper.exportSchema(descriptor(), 5);
        assertEquals(List.of("second", "first", "values", "attributes", "self"),
                schema.children.stream().map(n -> n.path).toList());
        assertEquals("second.name", schema.children.get(0).children.get(0).path);
        assertEquals("first.name", schema.children.get(1).children.get(0).path);
        assertTrue(schema.children.get(2).repeated);
        assertTrue(schema.children.get(3).repeated);
        for (int i = 2; i < 5; i++) assertTrue(schema.children.get(i).children.isEmpty());
        assertTrue(helper.exportSchema(descriptor(), 0).children.get(0).children.isEmpty());
        assertTrue(helper.exportSchema(descriptor(), -1).children.isEmpty());
    }

    @Test
    void schemaJsonRetainsExactShapeAndPropertyOrder() {
        assertEquals("""
                {
                  "name" : "Timestamp",
                  "path" : "",
                  "type" : "MESSAGE",
                  "repeated" : false,
                  "children" : [ {
                    "name" : "seconds",
                    "path" : "seconds",
                    "type" : "LONG",
                    "repeated" : false
                  }, {
                    "name" : "nanos",
                    "path" : "nanos",
                    "type" : "INT",
                    "repeated" : false
                  } ]
                }""", new MappingHelperJsonSupport().schemaToJson(helper.exportSchema(Timestamp.getDescriptor(), 3)));
    }

    private static Descriptor descriptor() throws DescriptorValidationException {
        var leaf = DescriptorProto.newBuilder().setName("Leaf")
                .addField(field("name", 1, FieldDescriptorProto.Type.TYPE_STRING));
        var entry = DescriptorProto.newBuilder().setName("AttributesEntry")
                .setOptions(MessageOptions.newBuilder().setMapEntry(true))
                .addField(field("key", 1, FieldDescriptorProto.Type.TYPE_STRING))
                .addField(field("value", 2, FieldDescriptorProto.Type.TYPE_STRING));
        var root = DescriptorProto.newBuilder().setName("Root").addNestedType(entry)
                .addField(field("second", 1, FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".test.Leaf"))
                .addField(field("first", 2, FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".test.Leaf"))
                .addField(field("values", 3, FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".test.Leaf")
                        .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED))
                .addField(field("attributes", 4, FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".test.Root.AttributesEntry")
                        .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED))
                .addField(field("self", 5, FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(".test.Root"));
        return FileDescriptor.buildFrom(FileDescriptorProto.newBuilder().setName("mapping_boundaries.proto")
                .setPackage("test").setSyntax("proto3").addMessageType(leaf).addMessageType(root).build(),
                new FileDescriptor[0]).findMessageTypeByName("Root");
    }

    private static FieldDescriptorProto.Builder field(String name, int number, FieldDescriptorProto.Type type) {
        return FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setType(type);
    }
}
