package ai.pipestream.proto.mapper;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import com.google.protobuf.Any;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Regression tests for parser, null-assignment, and DynamicMessage Any handling bugs. */
class ProtoFieldMapperRegressionTest {
    private final ProtoFieldMapper mapper;

    ProtoFieldMapperRegressionTest() {
        var registry = DescriptorRegistry.create();
        registry.registerFile(TestDescriptors.FILE);
        mapper = new ProtoFieldMapperImpl(registry);
    }

    // --- Bug 1: "target+=source" without whitespace mis-parsed as ASSIGN to "target+" ---

    @Test
    void appendRuleWithoutWhitespaceAppendsToRepeatedField() throws Exception {
        var source = TestDescriptors.document()
                .setField(TestDescriptors.DOCUMENT.findFieldByName("title"), "one").build();
        var target = TestDescriptors.document();
        mapper.map(source, target, List.of("tags+=title"));
        assertEquals(List.of("one"), mapper.getValue(target, "tags"));
    }

    // --- Bug 2: assigning null to a non-Struct field threw from setField(fd, null) ---

    @Test
    void assigningNullLiteralClearsField() throws Exception {
        var source = TestDescriptors.document().build();
        var target = TestDescriptors.document()
                .setField(TestDescriptors.DOCUMENT.findFieldByName("title"), "existing");
        mapper.map(source, target, List.of("title = null"));
        assertFalse(target.hasField(TestDescriptors.DOCUMENT.findFieldByName("title")));
    }

    @Test
    void assigningUnsetSourceFieldClearsTarget() throws Exception {
        var source = TestDescriptors.document().build(); // body never set
        var target = TestDescriptors.document()
                .setField(TestDescriptors.DOCUMENT.findFieldByName("title"), "existing");
        mapper.map(source, target, List.of("title = body"));
        assertFalse(target.hasField(TestDescriptors.DOCUMENT.findFieldByName("title")));
        assertNull(mapper.getValue(target, "title"));
    }

    // --- Bug 3: Any fields inside DynamicMessage trees were not detected as Any ---

    private DynamicMessage documentWithPackedInfoReparsedAsDynamic() throws Exception {
        var info = DynamicMessage.newBuilder(TestDescriptors.INFO)
                .setField(TestDescriptors.INFO.findFieldByName("version"), "v42").build();
        var document = TestDescriptors.document()
                .setField(TestDescriptors.DOCUMENT.findFieldByName("payload"), Any.pack(info)).build();
        // Re-parse so the payload field materialises as a DynamicMessage, not a concrete Any.
        return DynamicMessage.parseFrom(TestDescriptors.DOCUMENT, document.toByteString());
    }

    @Test
    void unpacksDynamicAnyOnFinalPathSegment() throws Exception {
        var source = documentWithPackedInfoReparsedAsDynamic();
        Object payload = mapper.getValue(source, "payload");
        assertEquals("test.Info", ((Message) payload).getDescriptorForType().getFullName());
    }

    @Test
    void traversesThroughDynamicAnyMidPath() throws Exception {
        var source = documentWithPackedInfoReparsedAsDynamic();
        var target = TestDescriptors.document();
        mapper.map(source, target, List.of("title = payload.version"));
        assertEquals("v42", mapper.getValue(target, "title"));
    }

    @Test
    void copyingDynamicAnyDoesNotDoublePack() throws Exception {
        // Setting a DynamicMessage form of an Any into an Any field must use it as-is,
        // not pack the Any inside another Any.
        var bareMapper = new ProtoFieldMapperImpl(new DescriptorRegistry());
        var source = documentWithPackedInfoReparsedAsDynamic();
        var target = TestDescriptors.document();
        var rawAny = (Message) source.getField(TestDescriptors.DOCUMENT.findFieldByName("payload"));
        bareMapper.setValue(target, "payload", rawAny);
        var payload = (Message) target.build().getField(TestDescriptors.DOCUMENT.findFieldByName("payload"));
        assertEquals("type.googleapis.com/test.Info",
                payload.getField(Any.getDescriptor().findFieldByName("type_url")));
    }

    // --- Bug 4: getValue could read through nested Struct keys but writes threw ---

    @Test
    void assignsNestedStructKeyCreatingIntermediateStructs() throws Exception {
        var target = TestDescriptors.document();
        mapper.mapInPlace(target, List.of("metadata.info.version = \"v2\""));
        assertEquals("v2", mapper.getValue(target, "metadata.info.version"));
    }

    @Test
    void assignsNestedStructKeyPreservingSiblingKeys() throws Exception {
        var target = TestDescriptors.document();
        mapper.setValue(target, "metadata.info.author", "me");
        mapper.setValue(target, "metadata.info.version", "v2");
        assertEquals("me", mapper.getValue(target, "metadata.info.author"));
        assertEquals("v2", mapper.getValue(target, "metadata.info.version"));
    }

    @Test
    void clearsNestedStructKeyPreservingSiblingKeys() throws Exception {
        var target = TestDescriptors.document();
        mapper.setValue(target, "metadata.info.version", "v2");
        mapper.setValue(target, "metadata.info.author", "me");
        mapper.mapInPlace(target, List.of("-metadata.info.version"));
        assertNull(mapper.getValue(target, "metadata.info.version"));
        assertEquals("me", mapper.getValue(target, "metadata.info.author"));
    }

    @Test
    void appendsToNestedStructKey() throws Exception {
        var target = TestDescriptors.document();
        mapper.appendValue(target, "metadata.info.tags", "a");
        mapper.appendValue(target, "metadata.info.tags", "b");
        assertEquals(List.of("a", "b"), mapper.getValue(target, "metadata.info.tags"));
    }

    // --- Bug 5: clearing through an unset parent materialised the parent ---

    @Test
    void clearOnUnsetMessageParentIsNoOp() throws Exception {
        var target = TestDescriptors.document();
        mapper.mapInPlace(target, List.of("-info.version"));
        assertFalse(target.hasField(TestDescriptors.DOCUMENT.findFieldByName("info")));
    }

    @Test
    void clearOnUnsetStructParentIsNoOp() throws Exception {
        var target = TestDescriptors.document();
        mapper.clearField(target, "metadata.key");
        assertFalse(target.hasField(TestDescriptors.DOCUMENT.findFieldByName("metadata")));
    }

    @Test
    void assignmentStillMaterialisesUnsetParents() throws Exception {
        var target = TestDescriptors.document();
        mapper.setValue(target, "info.version", "v1");
        assertTrue(target.hasField(TestDescriptors.DOCUMENT.findFieldByName("info")));
        assertEquals("v1", mapper.getValue(target, "info.version"));
    }

    // --- Bug 6: append skipped type conversion, so "counts += 5" threw on int32 fields ---

    @Test
    void appendConvertsElementToRepeatedFieldType() throws Exception {
        var target = TestDescriptors.document();
        mapper.mapInPlace(target, List.of("counts += 5"));
        assertEquals(List.of(5), mapper.getValue(target, "counts"));
    }

    // --- Bug 7: appending to a Struct key holding a scalar silently discarded the scalar ---

    @Test
    void appendToStructScalarPreservesExistingValue() throws Exception {
        var target = TestDescriptors.document();
        mapper.setValue(target, "metadata.tags", "a");
        mapper.appendValue(target, "metadata.tags", "b");
        assertEquals(List.of("a", "b"), mapper.getValue(target, "metadata.tags"));
    }

    // --- Bug 9: appending null reached addRepeatedField and threw a bare NullPointerException ---

    @Test
    void appendingNullValueReportsAMappingException() {
        var target = TestDescriptors.document();
        MappingException e = assertThrows(MappingException.class,
                () -> mapper.appendValue(target, "tags", null));
        assertTrue(e.getMessage().contains("Cannot append null"));
        assertTrue(e.getMessage().contains("tags"));
    }

    @Test
    void appendingNullLiteralReportsAMappingException() {
        var target = TestDescriptors.document();
        MappingException e = assertThrows(MappingException.class,
                () -> mapper.mapInPlace(target, List.of("tags += null")));
        assertTrue(e.getMessage().contains("Cannot append null"));
    }

    @Test
    void appendingUnsetSourceFieldReportsAMappingException() {
        var source = TestDescriptors.document().build(); // body never set
        var target = TestDescriptors.document();
        MappingException e = assertThrows(MappingException.class,
                () -> mapper.map(source, target, List.of("tags += body")));
        assertTrue(e.getMessage().contains("Cannot append null"));
    }

    // --- Bug 10: assigning null through an unset parent materialised the parent as PRESENT ---

    @Test
    void assigningNullThroughUnsetParentLeavesTheParentUnset() throws Exception {
        var target = TestDescriptors.document();
        mapper.setValue(target, "info.version", null);
        assertFalse(target.hasField(TestDescriptors.DOCUMENT.findFieldByName("info")));
    }

    @Test
    void assigningNullThroughSetParentClearsOnlyTheLeaf() throws Exception {
        var target = TestDescriptors.document();
        mapper.setValue(target, "info.version", "v1");
        mapper.setValue(target, "info.version", null);
        assertTrue(target.hasField(TestDescriptors.DOCUMENT.findFieldByName("info")));
        assertNull(mapper.getValue(target, "info.version"));
    }

    /** A null under a Struct key is a stored NullValue, so the Struct parent is still created. */
    @Test
    void assigningNullToStructKeyStillCreatesTheStruct() throws Exception {
        var target = TestDescriptors.document();
        mapper.setValue(target, "metadata.source", null);
        assertTrue(target.hasField(TestDescriptors.DOCUMENT.findFieldByName("metadata")));
    }

    // --- Bug 8: a failed Any unpack on the final path segment returned the raw packed Any ---

    @Test
    void unresolvableAnyOnFinalSegmentThrows() throws Exception {
        var bareMapper = new ProtoFieldMapperImpl(new DescriptorRegistry());
        var source = documentWithPackedInfoReparsedAsDynamic();
        MappingException e = assertThrows(MappingException.class, () -> bareMapper.getValue(source, "payload"));
        assertTrue(e.getMessage().contains("type.googleapis.com/test.Info"));
        assertTrue(e.getMessage().contains("register"));
    }
}
