package ai.pipestream.proto.prompt;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.prompt.testdata.WideForm;
import com.google.protobuf.Descriptors.Descriptor;
import java.util.Locale;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Every declared constraint has to reach the model as a sentence, and a prose mapper fails
 * quietly: it says "at most" where the rule says "at least", or drops a bound, and the
 * output still reads like a well-formed instruction. Nothing downstream can catch that.
 * The model just fills the form wrong.
 *
 * <p>{@code PromptRendererTest} covers the families the decorated form carries. These are
 * the rest, on a fixture whose bounds are deliberately asymmetric so a swapped word has
 * somewhere to show.
 */
class InstructionRendererTest {

    private static final Descriptor FORM = WideForm.getDescriptor();

    private static String render() {
        return PromptRenderer.create().render(FORM,
                RenderPromptRequest.newBuilder().setTargetType(FORM.getFullName()).build(),
                "repo://forms/wide").getInstructions();
    }

    // --- strings ----------------------------------------------------------------

    @Test
    void stringRulesEachGetTheirOwnSentence() {
        assertThat(render())
                .contains("must equal \"fixed\"")
                .contains("must be exactly 5 characters")
                .contains("must start with \"start\"")
                .contains("must end with \"end\"")
                .contains("must contain \"mid\"")
                .contains("must not contain \"bad\"")
                .contains("must be one of: \"alpha\", \"beta\"")
                .contains("must not be any of: \"gamma\"")
                .contains("must match /^[a-z]+$/");
    }

    @Test
    void aDeclaredFormatIsNamedInWordsTheModelCanActOn() {
        assertThat(render()).contains("must be a valid email");
    }

    // --- numbers ----------------------------------------------------------------

    @Test
    void numericBoundsKeepTheirStrictness() {
        // The four comparisons are the easiest pair in the file to transpose, and the
        // fixture uses distinct values so a transposition cannot read as correct.
        assertThat(render())
                .contains("must equal 7")
                .contains("must be greater than 1")
                .contains("must be less than 10")
                .contains("must be at least 0.25")
                .contains("must be at most 0.75")
                .doesNotContain("must be greater than 10\n")
                .doesNotContain("must be less than 1\n")
                .doesNotContain("must be at least 0.75")
                .doesNotContain("must be at most 0.25");
    }

    @Test
    void numericMembershipListsBothDirections() {
        assertThat(render())
                .contains("must be one of: 11, 22")
                .contains("must not be any of: 33");
    }

    @Test
    void aBooleanConstantIsStated() {
        assertThat(render()).contains("must be true");
    }

    // --- enums ------------------------------------------------------------------

    /**
     * Enum rules are written as numbers and have to reach the model as names: the model
     * has never seen the numbering, and a prompt that says "must equal 1" is unfillable.
     */
    @Test
    void enumRulesAreRenderedAsNamesNotNumbers() {
        String out = render();
        assertThat(out)
                .contains("must equal POSTURE_AFFIRMED")
                .contains("must be one of: POSTURE_AFFIRMED, POSTURE_REVERSED")
                .contains("must not be any of: POSTURE_UNSPECIFIED");
        assertThat(out).doesNotContain("must equal 1");
    }

    @Test
    void anEnumFieldSpellsOutItsWholeVocabulary() {
        // The vocabulary lives in the schema and nowhere in prose the model has read.
        assertThat(render()).contains("defined values: POSTURE_UNSPECIFIED (means unknown),"
                + " POSTURE_AFFIRMED, POSTURE_REVERSED");
    }

    // --- bytes, maps, collections -----------------------------------------------

    @Test
    void byteLengthsSayBytesAndStringLengthsSayCharacters() {
        // The two families count different things, and saying "characters" for a digest
        // asks the model for the wrong length.
        assertThat(render())
                .contains("must be exactly 32 bytes")
                .contains("must be at least 1 bytes")
                .contains("must be at most 1024 bytes");
    }

    @Test
    void mapSizesAreCountedInEntries() {
        assertThat(render())
                .contains("must have at least 1 entries")
                .contains("must have at most 8 entries");
    }

    @Test
    void aRepeatedFieldsElementRulesAreMarkedAsPerItem() {
        // Without the prefix, the element's bound reads as a bound on the list.
        assertThat(render()).contains("each item: must be at least 1600");
    }

    // --- CEL --------------------------------------------------------------------

    @Test
    void aCelRuleWithAMessagePresentsTheMessage() {
        assertThat(render())
                .contains("rule wide.reviewed: a reviewed form names its reviewer");
    }

    @Test
    void aCelRuleWithoutAMessageFallsBackToTheExpression() {
        assertThat(render())
                .contains("rule wide.unexplained: must satisfy `this.size() < 100`");
    }

    // --- field headers ----------------------------------------------------------

    @Test
    void everyFieldIsNumberedWithItsJsonNameAndType() {
        assertThat(render())
                .contains("1. \"exact\" (string) - optional")
                .contains("10. \"ratio\" (double) - optional")
                .contains("11. \"agreed\" (bool) - optional");
    }

    @Test
    void compositeTypesAreSpelledOutRatherThanCalledMessages() {
        assertThat(render())
                .contains("\"tallies\" (map<string, int32>)")
                .contains("\"years\" (repeated int32)")
                .contains("\"nested\" (ai.pipestream.proto.prompt.testdata.v1.DecoratedOpinion)")
                .contains("\"settledPosture\" (ai.pipestream.proto.prompt.testdata.v1.Posture)");
    }

    @Test
    void aFieldWithNothingDeclaredSaysSoRatherThanShowingAnEmptyList() {
        assertThat(render()).contains("Requirements: none beyond the type.");
    }

    // --- locale -----------------------------------------------------------------

    private final Locale original = Locale.getDefault();

    @AfterEach
    void restoreLocale() {
        Locale.setDefault(original);
    }

    /**
     * Type names and format names are lowercased on the way into the prompt. Doing that in
     * the default locale means the prompt changes with the host's language settings: in
     * Turkish, "STRING" lowercases to "strıng" with a dotless i, and the model is handed a
     * type that does not exist. The text is protocol, not display, so it lowercases in the
     * root locale and reads the same everywhere.
     */
    @Test
    void theRenderedTextDoesNotChangeWithTheHostsLocale() {
        String inRoot = render();
        Locale.setDefault(Locale.forLanguageTag("tr-TR"));
        String inTurkish = render();

        assertThat(inTurkish).isEqualTo(inRoot);
        assertThat(inTurkish).contains("(string)").contains("must be a valid email");
    }
}
