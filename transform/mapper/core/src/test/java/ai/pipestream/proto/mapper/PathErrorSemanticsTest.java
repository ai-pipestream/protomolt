package ai.pipestream.proto.mapper;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Error messages and edge cases for dot-notation path resolution in get/set/append/clear. */
class PathErrorSemanticsTest {

    private final ProtoFieldMapper mapper = new ProtoFieldMapperImpl(new DescriptorRegistry());

    @Test
    void readingUnknownFieldReportsFieldAndMessage() {
        var document = TestDescriptors.document().build();
        MappingException e = assertThrows(MappingException.class,
                () -> mapper.getValue(document, "nope"));
        assertTrue(e.getMessage().contains("Field 'nope' not found in message 'Document'"), e.getMessage());
        assertEquals(MappingException.Category.GENERAL, e.category());
    }

    @Test
    void traversingThroughScalarIsRejected() throws Exception {
        var document = TestDescriptors.document()
                .setField(TestDescriptors.DOCUMENT.findFieldByName("title"), "t").build();
        MappingException e = assertThrows(MappingException.class,
                () -> mapper.getValue(document, "title.deeper"));
        assertTrue(e.getMessage().contains("non-message or repeated field 'title'"), e.getMessage());
    }

    @Test
    void traversingThroughRepeatedFieldIsRejected() {
        var document = TestDescriptors.document().build();
        MappingException e = assertThrows(MappingException.class,
                () -> mapper.getValue(document, "tags.deeper"));
        assertTrue(e.getMessage().contains("non-message or repeated field 'tags'"), e.getMessage());
    }

    @Test
    void traversingThroughUnsetIntermediateMessageIsRejected() {
        var document = TestDescriptors.document().build();
        MappingException e = assertThrows(MappingException.class,
                () -> mapper.getValue(document, "info.version"));
        assertTrue(e.getMessage().contains("intermediate field 'info' is not set"), e.getMessage());
        assertEquals(MappingException.Category.ABSENT_INTERMEDIATE, e.category());
    }

    @Test
    void missingStructKeyMidPathIsRejected() throws Exception {
        var document = TestDescriptors.document();
        mapper.setValue(document, "metadata.x", "1");
        MappingException e = assertThrows(MappingException.class,
                () -> mapper.getValue(document.build(), "metadata.y.z"));
        assertTrue(e.getMessage().contains("key 'y' not found in struct"), e.getMessage());
    }

    @Test
    void missingStructLeafKeyReadsAsNull() throws Exception {
        var document = TestDescriptors.document();
        mapper.setValue(document, "metadata.x", "1");
        assertNull(mapper.getValue(document.build(), "metadata.absent"));
    }

    @Test
    void traversingThroughAStructScalarValueIsRejected() throws Exception {
        var document = TestDescriptors.document();
        mapper.setValue(document, "metadata.title", "x");
        MappingException e = assertThrows(MappingException.class,
                () -> mapper.getValue(document.build(), "metadata.title.foo"));
        assertTrue(e.getMessage().contains("non-message, non-struct value at 'foo'"), e.getMessage());
    }

    @Test
    void unsetRepeatedLeafReadsAsEmptyList() throws Exception {
        var document = TestDescriptors.document().build();
        assertEquals(List.of(), mapper.getValue(document, "tags"));
    }

    @Test
    void repeatedLeafReadsAsList() throws Exception {
        var document = TestDescriptors.document();
        mapper.appendValue(document, "tags", "a");
        mapper.appendValue(document, "tags", "b");
        assertEquals(List.of("a", "b"), mapper.getValue(document.build(), "tags"));
    }

    @Test
    void settingUnknownFieldIsRejected() {
        var document = TestDescriptors.document();
        MappingException e = assertThrows(MappingException.class,
                () -> mapper.setValue(document, "nope", "v"));
        assertTrue(e.getMessage().contains("Field 'nope' not found"), e.getMessage());
    }

    @Test
    void clearingUnknownFieldIsRejected() {
        var document = TestDescriptors.document();
        assertThrows(MappingException.class, () -> mapper.clearField(document, "nope"));
    }

    @Test
    void appendingToNonRepeatedFieldIsRejected() {
        var document = TestDescriptors.document();
        MappingException e = assertThrows(MappingException.class,
                () -> mapper.appendValue(document, "title", "x"));
        assertTrue(e.getMessage().contains("is not repeated"), e.getMessage());
    }

    @Test
    void appendingAListAppendsEveryElement() throws Exception {
        var document = TestDescriptors.document();
        mapper.appendValue(document, "tags", List.of("a", "b", "c"));
        assertEquals(List.of("a", "b", "c"), mapper.getValue(document.build(), "tags"));
    }

    @Test
    void appendingAListWithANullElementIsRejected() {
        var document = TestDescriptors.document();
        MappingException e = assertThrows(MappingException.class,
                () -> mapper.appendValue(document, "tags", Arrays.asList("a", null)));
        assertTrue(e.getMessage().contains("Cannot append null"), e.getMessage());
    }

    @Test
    void traversingThroughRepeatedOnWriteIsRejected() {
        var document = TestDescriptors.document();
        MappingException e = assertThrows(MappingException.class,
                () -> mapper.setValue(document, "tags.first", "x"));
        assertTrue(e.getMessage().contains("non-singular message field 'tags'"), e.getMessage());
    }
}
