package ai.pipestream.proto.chunk;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class SentenceRulesTest {

    @Test
    void terminatorRunsWithClosersEndSentences() {
        String text = "He said \"Stop!\" Then silence. Nothing more?! Yes.";
        List<SentenceRules.Sentence> sentences = SentenceRules.segment(text);
        assertThat(sentences).extracting(s -> text.substring(s.start(), s.end()))
                .containsExactly(
                        "He said \"Stop!\"",
                        "Then silence.",
                        "Nothing more?!",
                        "Yes.");
    }

    @Test
    void abbreviationsWithoutFollowingWhitespaceDoNotSplit() {
        String text = "Version 1.2.3 shipped today. It works.";
        List<SentenceRules.Sentence> sentences = SentenceRules.segment(text);
        assertThat(sentences).extracting(s -> text.substring(s.start(), s.end()))
                .containsExactly("Version 1.2.3 shipped today.", "It works.");
    }

    @Test
    void blankLinesOutrankPunctuation() {
        String text = "A heading without a period\n\nThe body starts here. And continues.";
        List<SentenceRules.Sentence> sentences = SentenceRules.segment(text);
        assertThat(sentences).extracting(s -> text.substring(s.start(), s.end()))
                .containsExactly(
                        "A heading without a period",
                        "The body starts here.",
                        "And continues.");
    }

    @Test
    void blankLineDetectionIgnoresSpacesTabsAndCarriageReturns() {
        String text = "First paragraph\r\n \t\r\nSecond paragraph";
        List<SentenceRules.Sentence> sentences = SentenceRules.segment(text);
        assertThat(sentences).extracting(s -> text.substring(s.start(), s.end()))
                .containsExactly("First paragraph", "Second paragraph");
    }

    @Test
    void tokensAreMaximalNonWhitespaceRuns() {
        assertThat(SentenceRules.countTokens("one  two\tthree\nfour", 0, 19)).isEqualTo(4);
        assertThat(SentenceRules.countTokens("  ", 0, 2)).isZero();
        assertThat(SentenceRules.countTokens("a-b c,d", 0, 7)).isEqualTo(2);
    }

    @Test
    void blankTextYieldsNoSentences() {
        assertThat(SentenceRules.segment("")).isEmpty();
        assertThat(SentenceRules.segment("   \n\t  ")).isEmpty();
    }

    @Test
    void spansExcludeSurroundingWhitespace() {
        String text = "  Padded sentence.   Next one.  ";
        List<SentenceRules.Sentence> sentences = SentenceRules.segment(text);
        assertThat(sentences).hasSize(2);
        assertThat(text.substring(sentences.get(0).start(), sentences.get(0).end()))
                .isEqualTo("Padded sentence.");
        assertThat(text.substring(sentences.get(1).start(), sentences.get(1).end()))
                .isEqualTo("Next one.");
    }
}
