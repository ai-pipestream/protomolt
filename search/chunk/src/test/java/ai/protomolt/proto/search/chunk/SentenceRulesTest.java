package ai.protomolt.proto.search.chunk;

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

    @Test
    void supplementaryPlaneCharactersAreTokenCharacters() {
        // An emoji is a surrogate pair; neither half is whitespace, so it
        // joins the token run and the span stays char-faithful.
        List<SentenceRules.Sentence> sentences = SentenceRules.segment("Party 🎉 time.");
        assertThat(sentences).hasSize(1);
        assertThat(sentences.getFirst().tokens()).isEqualTo(3);
    }

    @Test
    void cjkFullStopIsNotABoundaryUnderRulesV1() {
        // rules-v1 is Latin-oriented and pinned: '。' never ends a sentence.
        // CJK-aware segmentation would be a new rule-set id, not an edit here.
        assertThat(SentenceRules.segment("彼は来た。彼女は来た。")).hasSize(1);
    }

    @Test
    void anAbbreviationFollowedByWhitespaceSplits() {
        // The pinned rule has no abbreviation lexicon: a terminator followed
        // by whitespace ends the sentence, even after "Mr.". Documented so a
        // future fix is a deliberate rule-set bump, not a silent edit.
        String text = "Mr. Smith left. He returned.";
        List<SentenceRules.Sentence> sentences = SentenceRules.segment(text);
        assertThat(sentences).extracting(s -> text.substring(s.start(), s.end()))
                .containsExactly("Mr.", "Smith left.", "He returned.");
    }
}
