package ai.protomolt.proto.parse.text;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Text cleaning on OpenNLP's Unicode whitespace model. */
class TextCleaningTest {

    @Test
    void blankLineSplittingRecognizesLfCrAndCrlf() {
        assertThat(TextCleaning.splitOnBlankLines("a\n\nb")).containsExactly("a", "b");
        assertThat(TextCleaning.splitOnBlankLines("a\r\rb")).containsExactly("a", "b");
        assertThat(TextCleaning.splitOnBlankLines("a\r\n\r\nb")).containsExactly("a", "b");
        // A lone break is not a blank line.
        assertThat(TextCleaning.splitOnBlankLines("a\nb")).containsExactly("a\nb");
        // CRLF is one break, so CRLF-LF is a blank line.
        assertThat(TextCleaning.splitOnBlankLines("a\r\n\nb")).containsExactly("a", "b");
    }

    @Test
    void blankLineSplittingRecognizesUnicodeLineBreaks() {
        // NEL, LINE SEPARATOR, PARAGRAPH SEPARATOR are all Unicode line breaks.
        assertThat(TextCleaning.splitOnBlankLines("a\u0085\u0085b")).containsExactly("a", "b");
        assertThat(TextCleaning.splitOnBlankLines("a\u2028\u2028b")).containsExactly("a", "b");
        assertThat(TextCleaning.splitOnBlankLines("a\u2029\u2029b")).containsExactly("a", "b");
    }

    @Test
    void blankLineSplittingToleratesEdgeRuns() {
        assertThat(TextCleaning.splitOnBlankLines("\n\na")).containsExactly("a");
        assertThat(TextCleaning.splitOnBlankLines("a\n\n")).containsExactly("a");
        assertThat(TextCleaning.splitOnBlankLines("")).isEmpty();
        assertThat(TextCleaning.splitOnBlankLines("\n\n")).isEmpty();
    }

    @Test
    void flowLinesTurnsEveryBreakIntoOneSpace() {
        assertThat(TextCleaning.flowLines("a\nb")).isEqualTo("a b");
        assertThat(TextCleaning.flowLines("a\r\nb")).isEqualTo("a b");
        assertThat(TextCleaning.flowLines("a\u2028b")).isEqualTo("a b");
        assertThat(TextCleaning.flowLines("a\u2029b")).isEqualTo("a b");
    }

    @Test
    void trimRemovesUnicodeWhitespace() {
        // Non-breaking space and U+2028 both trim; Character.isWhitespace misses both.
        assertThat(TextCleaning.trim("\u00A0a\u00A0")).isEqualTo("a");
        assertThat(TextCleaning.trim("\u2028a\u2029")).isEqualTo("a");
    }

    @Test
    void headingMarkupStripsAfterUnicodeWhitespaceToo() {
        assertThat(TextCleaning.stripHeadingMarkup("# Title")).isEqualTo("Title");
        assertThat(TextCleaning.stripHeadingMarkup("###\u00A0Title")).isEqualTo("Title");
        assertThat(TextCleaning.stripHeadingMarkup("#\u00A0Title")).isEqualTo("Title");
        assertThat(TextCleaning.stripHeadingMarkup("#")).isEmpty();
        assertThat(TextCleaning.stripHeadingMarkup("No markup")).isEqualTo("No markup");
    }
}
