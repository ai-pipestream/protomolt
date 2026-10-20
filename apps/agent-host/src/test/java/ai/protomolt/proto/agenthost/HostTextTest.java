package ai.protomolt.proto.agenthost;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The line splits replace {@code String.split("\n")} and {@code String.split("\\R")}; each
 * is checked against named cases and against the split it replaces over every short mix of
 * line breaks, so empty text, CRLF, trailing breaks and consecutive breaks all agree.
 */
class HostTextTest {

    private static final List<String> PIECES = List.of(
            "a", " ", "\n", "\r", "\r\n", "\u000B", "\f", "\u0085", "\u2028", "\u2029");

    @Test
    void splitOnNewlineKeepsCarriageReturnsAndDropsOnlyTrailingEmptyLines() {
        assertThat(HostText.splitOnNewline("")).containsExactly("");
        assertThat(HostText.splitOnNewline("a")).containsExactly("a");
        assertThat(HostText.splitOnNewline("a\n")).containsExactly("a");
        assertThat(HostText.splitOnNewline("a\n\n\n")).containsExactly("a");
        assertThat(HostText.splitOnNewline("\na")).containsExactly("", "a");
        assertThat(HostText.splitOnNewline("a\n\nb")).containsExactly("a", "", "b");
        assertThat(HostText.splitOnNewline("a\r\nb\r\n")).containsExactly("a\r", "b\r");
        assertThat(HostText.splitOnNewline("a\rb\u2028c")).containsExactly("a\rb\u2028c");
        assertThat(HostText.splitOnNewline("\n")).isEmpty();
        assertThat(HostText.splitOnNewline("\n\n")).isEmpty();
    }

    @Test
    void splitOnLineBreaksTreatsCrlfAsOneBreakAndHonoursEveryLineBreakCharacter() {
        assertThat(HostText.splitOnLineBreaks("")).containsExactly("");
        assertThat(HostText.splitOnLineBreaks("a")).containsExactly("a");
        assertThat(HostText.splitOnLineBreaks("a\r\nb\r\n")).containsExactly("a", "b");
        assertThat(HostText.splitOnLineBreaks("a\n\rb")).containsExactly("a", "", "b");
        assertThat(HostText.splitOnLineBreaks("a\r\r\nb")).containsExactly("a", "", "b");
        assertThat(HostText.splitOnLineBreaks("\r\na")).containsExactly("", "a");
        assertThat(HostText.splitOnLineBreaks("a\u000Bb\fc\u0085d\u2028e\u2029f"))
                .containsExactly("a", "b", "c", "d", "e", "f");
        assertThat(HostText.splitOnLineBreaks("\r\n\r\n")).isEmpty();
        assertThat(HostText.splitOnLineBreaks("\r")).isEmpty();
    }

    @Test
    void splitsAgreeWithTheRegexSplitsTheyReplace() {
        List<String> texts = new ArrayList<>();
        texts.add("");
        mixes("", 4, texts);
        for (String text : texts) {
            assertThat(HostText.splitOnNewline(text))
                    .as("split(\"\\n\") of %s", escaped(text))
                    .containsExactlyElementsOf(Arrays.asList(text.split("\n")));
            assertThat(HostText.splitOnLineBreaks(text))
                    .as("split(\"\\\\R\") of %s", escaped(text))
                    .containsExactlyElementsOf(Arrays.asList(text.split("\\R")));
        }
        assertThat(texts).hasSize(1 + 10 + 100 + 1000 + 10000);
    }

    @Test
    void characterClassesAreAsciiOnly() {
        for (char c = 0; c < 0x80; c++) {
            String s = String.valueOf(c);
            assertThat(HostText.isAsciiUpper(c)).as("upper %s", (int) c)
                    .isEqualTo(s.matches("[A-Z]"));
            assertThat(HostText.isAsciiDigit(c)).as("digit %s", (int) c)
                    .isEqualTo(s.matches("[0-9]"));
            assertThat(HostText.isAsciiLetterOrDigit(c)).as("alnum %s", (int) c)
                    .isEqualTo(s.matches("[A-Za-z0-9]"));
        }
        for (char c : new char[] {'\u00C9', '\u00E9', '\u00DF', '\u0130', '\u0661',
                '\uFF21', '\uFF11', '\u212A'}) {
            assertThat(HostText.isAsciiUpper(c)).isFalse();
            assertThat(HostText.isAsciiDigit(c)).isFalse();
            assertThat(HostText.isAsciiLetterOrDigit(c)).isFalse();
        }
    }

    private static void mixes(String prefix, int depth, List<String> into) {
        if (depth == 0) {
            return;
        }
        for (String piece : PIECES) {
            String text = prefix + piece;
            into.add(text);
            mixes(text, depth - 1, into);
        }
    }

    private static String escaped(String text) {
        StringBuilder out = new StringBuilder("\"");
        for (char c : text.toCharArray()) {
            out.append(c < 0x20 || c > 0x7E ? String.format("\\u%04X", (int) c) : c);
        }
        return out.append('"').toString();
    }
}
