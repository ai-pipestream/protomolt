package ai.protomolt.proto.mapper;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Direct coverage of the text rule language: {@code target = source}, {@code target += source}, {@code -target}. */
class TextRuleParserTest {

    private final TextRuleParser parser = new TextRuleParser();

    @Test
    void parsesAssignWithFlexibleWhitespace() throws Exception {
        TextMappingRule rule = only(parser.parse(List.of("a = b")));
        assertEquals("a", rule.targetPath());
        assertEquals("b", rule.sourcePath());
        assertEquals(TextMappingRule.Operation.ASSIGN, rule.operation());
    }

    @Test
    void parsesAssignWithoutWhitespace() throws Exception {
        TextMappingRule rule = only(parser.parse(List.of("a=b")));
        assertEquals("a", rule.targetPath());
        assertEquals("b", rule.sourcePath());
        assertEquals(TextMappingRule.Operation.ASSIGN, rule.operation());
    }

    @Test
    void assignTrimsSurroundingWhitespaceButKeepsInnerSourceContent() throws Exception {
        TextMappingRule rule = only(parser.parse(List.of("  a   =   b = c  ")));
        assertEquals("a", rule.targetPath());
        assertEquals("b = c", rule.sourcePath());
    }

    @Test
    void quotedSourceIsKeptVerbatimByTheParser() throws Exception {
        TextMappingRule rule = only(parser.parse(List.of("a = \"x y\"")));
        assertEquals("\"x y\"", rule.sourcePath());
    }

    @Test
    void parsesAppendWithoutWhitespace() throws Exception {
        TextMappingRule rule = only(parser.parse(List.of("tags+=title")));
        assertEquals("tags", rule.targetPath());
        assertEquals("title", rule.sourcePath());
        assertEquals(TextMappingRule.Operation.APPEND, rule.operation());
    }

    @Test
    void parsesAppendWithWhitespace() throws Exception {
        TextMappingRule rule = only(parser.parse(List.of("tags  +=  title")));
        assertEquals("tags", rule.targetPath());
        assertEquals("title", rule.sourcePath());
        assertEquals(TextMappingRule.Operation.APPEND, rule.operation());
    }

    @Test
    void parsesClearWithFlexibleWhitespace() throws Exception {
        TextMappingRule compact = only(parser.parse(List.of("-title")));
        assertEquals("title", compact.targetPath());
        assertNull(compact.sourcePath());
        assertEquals(TextMappingRule.Operation.CLEAR, compact.operation());

        TextMappingRule spaced = only(parser.parse(List.of("  -  title  ")));
        assertEquals("title", spaced.targetPath());
        assertEquals(TextMappingRule.Operation.CLEAR, spaced.operation());
    }

    @Test
    void skipsNullAndBlankRuleStrings() throws Exception {
        List<TextMappingRule> rules = parser.parse(Arrays.asList(null, "", "   ", "a = b"));
        assertEquals(1, rules.size());
        assertEquals("a", rules.get(0).targetPath());
    }

    @Test
    void emptyListParsesToNoRules() throws Exception {
        assertTrue(parser.parse(List.of()).isEmpty());
    }

    @Test
    void originalRuleIsPreservedVerbatim() throws Exception {
        String original = "  tags+=title ";
        TextMappingRule rule = only(parser.parse(List.of(original)));
        assertEquals(original, rule.originalRule());
    }

    @Test
    void rejectsRuleWithOperatorButNoAssignment() {
        assertInvalid("a + b");
    }

    @Test
    void rejectsAppendWithSpaceBetweenPlusAndEquals() {
        assertInvalid("a+ = b");
    }

    @Test
    void rejectsMissingTarget() {
        assertInvalid("= b");
    }

    @Test
    void rejectsMissingSource() {
        assertInvalid("a =");
    }

    @Test
    void rejectsBareWord() {
        assertInvalid("justafield");
    }

    @Test
    void rejectsClearWithTrailingContent() {
        assertInvalid("-a b");
    }

    @Test
    void rejectsLoneDash() {
        assertInvalid("-");
    }

    // --- Cases carried over from the original suite ---

    @Test
    void parsesAssignAppendAndClearInOneList() throws Exception {
        var rules = parser.parse(List.of("title = body", "tags += title", "-language"));
        assertEquals(TextMappingRule.Operation.ASSIGN, rules.get(0).operation());
        assertEquals(TextMappingRule.Operation.APPEND, rules.get(1).operation());
        assertEquals(TextMappingRule.Operation.CLEAR, rules.get(2).operation());
    }

    @Test
    void trimsTabAndTrailingWhitespaceFromSource() throws Exception {
        assertEquals("body", only(parser.parse(List.of("title = body  "))).sourcePath());
        assertEquals("body", only(parser.parse(List.of("tags += body\t"))).sourcePath());
    }

    @Test
    void rejectsTildeOperator() {
        assertInvalid("title ~~ body");
    }

    @Test
    void listOfOnlyBlankLinesParsesToNoRules() throws Exception {
        assertTrue(parser.parse(Arrays.asList(null, " ", "\t")).isEmpty());
    }

    private void assertInvalid(String ruleString) {
        MappingException e = assertThrows(MappingException.class,
                () -> parser.parse(List.of(ruleString)));
        assertTrue(e.getMessage().contains("Invalid rule syntax"), e.getMessage());
        assertTrue(e.getMessage().contains(ruleString), e.getMessage());
    }

    private static TextMappingRule only(List<TextMappingRule> rules) {
        assertEquals(1, rules.size());
        return rules.get(0);
    }
}
