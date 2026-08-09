package ai.pipestream.proto.mapper;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ProtoFieldMapper#getValue} treats literal strings (null, booleans, quoted strings,
 * numbers) as values instead of paths.
 */
class LiteralValueReadTest {

    private final ProtoFieldMapper mapper = new ProtoFieldMapperImpl(new DescriptorRegistry());
    private final DynamicMessage document = TestDescriptors.document().build();

    @Test
    void nullLiteralReadsAsNull() throws Exception {
        assertNull(mapper.getValue(document, "null"));
    }

    @Test
    void booleanLiteralsReadAsBooleans() throws Exception {
        assertEquals(true, mapper.getValue(document, "true"));
        assertEquals(false, mapper.getValue(document, "false"));
    }

    @Test
    void quotedStringLiteralReadsAsString() throws Exception {
        assertEquals("hello", mapper.getValue(document, "\"hello\""));
    }

    @Test
    void emptyQuotedStringLiteralReadsAsEmptyString() throws Exception {
        assertEquals("", mapper.getValue(document, "\"\""));
    }

    @Test
    void loneQuoteIsRejected() {
        MappingException e = assertThrows(MappingException.class, () -> mapper.getValue(document, "\""));
        assertTrue(e.getMessage().contains("Invalid empty quoted string literal"), e.getMessage());
    }

    @Test
    void unterminatedQuoteFallsThroughToPathLookup() {
        // "\"abc" is not a closed quoted literal, so it is resolved as a (nonexistent) field path.
        assertThrows(MappingException.class, () -> mapper.getValue(document, "\"abc"));
    }

    @Test
    void integerLiteralReadsAsLong() throws Exception {
        assertEquals(42L, mapper.getValue(document, "42"));
    }

    @Test
    void negativeIntegerLiteralReadsAsLong() throws Exception {
        assertEquals(-7L, mapper.getValue(document, "-7"));
    }

    @Test
    void decimalLiteralReadsAsDouble() throws Exception {
        assertEquals(3.14d, mapper.getValue(document, "3.14"));
    }

    @Test
    void negativeDecimalLiteralReadsAsDouble() throws Exception {
        assertEquals(-0.5d, mapper.getValue(document, "-0.5"));
    }

    @Test
    void whitespaceAroundLiteralIsTrimmed() throws Exception {
        assertEquals(42L, mapper.getValue(document, "  42  "));
        assertEquals("x", mapper.getValue(document, " \"x\" "));
    }

    @Test
    void numberBeyondLongRangeFallsThroughToPathLookup() {
        // Matches the integer pattern but overflows Long: treated as a path, which then fails.
        assertThrows(MappingException.class,
                () -> mapper.getValue(document, "99999999999999999999999"));
    }

    @Test
    void dottedNonNumberIsAPathNotALiteral() {
        MappingException e = assertThrows(MappingException.class,
                () -> mapper.getValue(document, "1.2.3"));
        assertTrue(e.getMessage().contains("'1'"), e.getMessage());
    }

    @Test
    void blankPathIsNotALiteralAndFailsLookup() {
        assertThrows(MappingException.class, () -> mapper.getValue(document, "   "));
    }

    @Test
    void literalsResolveWhenReadingFromABuilder() throws Exception {
        var builder = TestDescriptors.document()
                .setField(TestDescriptors.DOCUMENT.findFieldByName("title"), "t");
        assertEquals("t", mapper.getValue(builder, "title"));
        assertEquals(5L, mapper.getValue(builder, "5"));
    }
}
