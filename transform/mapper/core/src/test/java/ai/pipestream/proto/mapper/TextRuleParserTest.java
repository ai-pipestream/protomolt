package ai.pipestream.proto.mapper;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class TextRuleParserTest {
    private final TextRuleParser parser = new TextRuleParser();

    @Test
    void parsesAssignAppendAndClear() throws Exception {
        var rules = parser.parse(List.of("title = body", "tags += title", "-language"));
        assertEquals(TextMappingRule.Operation.ASSIGN, rules.get(0).operation());
        assertEquals(TextMappingRule.Operation.APPEND, rules.get(1).operation());
        assertEquals(TextMappingRule.Operation.CLEAR, rules.get(2).operation());
    }

    @Test
    void parsesAppendWithoutSurroundingWhitespace() throws Exception {
        var rule = parser.parse(List.of("tags+=title")).getFirst();
        assertEquals(TextMappingRule.Operation.APPEND, rule.operation());
        assertEquals("tags", rule.targetPath());
        assertEquals("title", rule.sourcePath());
    }

    @Test
    void trimsTrailingWhitespaceFromSource() throws Exception {
        assertEquals("body", parser.parse(List.of("title = body  ")).getFirst().sourcePath());
        assertEquals("body", parser.parse(List.of("tags += body\t")).getFirst().sourcePath());
    }

    @Test
    void rejectsGarbage() {
        assertThrows(MappingException.class, () -> parser.parse(List.of("title ~~ body")));
    }

    @Test
    void skipsBlankLines() throws Exception {
        assertTrue(parser.parse(Arrays.asList(null, " ", "\t")).isEmpty());
    }

    @Test
    void preservesOriginalRule() throws Exception {
        assertEquals("  title = body  ", parser.parse(List.of("  title = body  ")).getFirst().originalRule());
    }
}
