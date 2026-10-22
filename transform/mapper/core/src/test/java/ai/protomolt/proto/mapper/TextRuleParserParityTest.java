package ai.protomolt.proto.mapper;

import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TextRuleParserParityTest {
    private static final Pattern[] OLD = {
        Pattern.compile("^\\s*([^=+\\s]+)\\s*=\\s*(.+)\\s*$"),
        Pattern.compile("^\\s*([^+\\s]+)\\s*\\+=\\s*(.+)\\s*$"),
        Pattern.compile("^\\s*-\\s*(\\S+)\\s*$")
    };

    @Test void agreesWithOriginalGrammarIncludingWhitespaceAndLineTerminators() throws Exception {
        for (String text : List.of("a= ", "a=\n", "a= \n", "a=\n x\n", "a=\u0085",
                "a=b\nc", "a=b\n\t", "a=b+=c", "a=b += c", "-a=b", "-a\u2028", "a+=\t",
                "a\u00a0=b", "a=\u0000", "a=\r\n")) check(text);
        char[] alphabet = {'a','b','=','+','-',' ', '\t','\n','\r','\f', '\u000b',
                '\u0085','\u2028','\u2029','\u00a0','\u0000','\ud83d','\ude00'};
        Random random = new Random(532);
        for (int n = 0; n < 30000; n++) {
            var text = new StringBuilder();
            int size = random.nextInt(20);
            for (int i = 0; i < size; i++) text.append(alphabet[random.nextInt(alphabet.length)]);
            check(text.toString());
        }
    }

    private static void check(String text) throws Exception {
        var parser = new TextRuleParser();
        if (text.trim().isEmpty()) { assertTrue(parser.parse(List.of(text)).isEmpty()); return; }
        for (int i = 0; i < OLD.length; i++) {
            var match = OLD[i].matcher(text);
            if (match.matches()) {
                var expected = new TextMappingRule(match.group(1), i == 2 ? null : match.group(2).trim(),
                        TextMappingRule.Operation.values()[i], text);
                assertEquals(List.of(expected), parser.parse(List.of(text)), () -> "input: " + text);
                return;
            }
        }
        assertThrows(MappingException.class, () -> parser.parse(List.of(text)), () -> "input: " + text);
    }
}
