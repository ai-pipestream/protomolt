package ai.pipestream.proto.screening;

import ai.pipestream.proto.screening.testdata.Contact;
import ai.pipestream.proto.screening.testdata.Profile;
import ai.pipestream.proto.types.ScreeningConfig;
import ai.pipestream.proto.types.ScreeningPolicy;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The screening walk and policy application, over a deterministic fake
 * engine (the seam the OpenNLP engine plugs into). Pins the chapter's
 * commitments: only declared-sensitivity fields are screened at all,
 * below-threshold detections are dropped without a trace, masked spans use
 * the shared redaction literal, every finding carries the model version and
 * threshold as evidence and never the text, and only an explicit policy
 * refuses.
 */
class ScreenerTest {

    /** Detects the word "Ada" (with confidence 0.9) wherever it appears. */
    private static final class AdaEngine implements ScreeningEngine {
        final List<String> seen = new ArrayList<>();
        final double confidence;

        AdaEngine(double confidence) {
            this.confidence = confidence;
        }

        @Override
        public List<Detection> detect(String text) {
            seen.add(text);
            int at = text.indexOf("Ada");
            return at < 0 ? List.of()
                    : List.of(new Detection("person", at, at + 3, confidence));
        }

        @Override
        public String modelVersion() {
            return "fake-2.1";
        }
    }

    private static ScreeningConfig config(ScreeningPolicy policy, double threshold) {
        return ScreeningConfig.newBuilder()
                .setSensitivityClass("pii")
                .setModelRef("file:unused")
                .setThreshold(threshold)
                .setPolicy(policy)
                .build();
    }

    private static Profile profile() {
        return Profile.newBuilder()
                .setDisplayName("Ada in the display name is NOT screened")
                .setBio("Written by Ada herself")
                .addAliases("Ada of the engine")
                .addAliases("countess")
                .setContact(Contact.newBuilder().setNote("ask Ada").setKind("Ada kind"))
                .putNotes("intro", "met Ada once")
                .addContacts(Contact.newBuilder().setNote("Ada again").setKind("plain"))
                .build();
    }

    @Test
    void maskReplacesSpansOnlyInScreenedFieldsAcrossEveryShape() {
        AdaEngine engine = new AdaEngine(0.9);
        Screener screener = new Screener(engine,
                config(ScreeningPolicy.SCREENING_POLICY_MASK, 0.5));

        Screener.Verdict verdict = screener.screen(profile());
        Profile masked = (Profile) verdict.message();

        // Screened fields mask the span with the shared redaction literal.
        assertThat(masked.getBio()).isEqualTo("Written by *** herself");
        assertThat(masked.getAliases(0)).isEqualTo("*** of the engine");
        assertThat(masked.getAliases(1)).isEqualTo("countess");
        assertThat(masked.getContact().getNote()).isEqualTo("ask ***");
        assertThat(masked.getNotesOrThrow("intro")).isEqualTo("met *** once");
        assertThat(masked.getContacts(0).getNote()).isEqualTo("*** again");

        // Unscreened fields are untouched AND never even reach the engine.
        assertThat(masked.getDisplayName()).contains("Ada");
        assertThat(masked.getContact().getKind()).isEqualTo("Ada kind");
        assertThat(engine.seen).noneMatch(text -> text.contains("display name"));
        assertThat(engine.seen).doesNotContain("Ada kind", "plain");

        // Every finding carries the evidence triple and never the text.
        assertThat(verdict.findings())
                .extracting(Screener.Finding::path)
                .containsExactlyInAnyOrder("bio", "aliases[0]", "contact.note",
                        "notes[intro]", "contacts[0].note");
        assertThat(verdict.findings()).allSatisfy(finding -> {
            assertThat(finding.type()).isEqualTo("person");
            assertThat(finding.modelVersion()).isEqualTo("fake-2.1");
            assertThat(finding.threshold()).isEqualTo(0.5);
            assertThat(finding.policy()).isEqualTo(ScreeningPolicy.SCREENING_POLICY_MASK);
            assertThat(finding.toString()).doesNotContain("Ada");
        });
    }

    @Test
    void belowThresholdDetectionsAreDroppedWithoutATrace() {
        Screener screener = new Screener(new AdaEngine(0.4),
                config(ScreeningPolicy.SCREENING_POLICY_MASK, 0.8));

        Screener.Verdict verdict = screener.screen(profile());

        assertThat(verdict.message()).isEqualTo(profile());
        assertThat(verdict.findings()).isEmpty();
    }

    @Test
    void tagRecordsFindingsButLeavesEveryValueUnchanged() {
        Screener screener = new Screener(new AdaEngine(0.9),
                config(ScreeningPolicy.SCREENING_POLICY_TAG, 0.5));

        Screener.Verdict verdict = screener.screen(profile());

        assertThat(verdict.message()).isEqualTo(profile());
        assertThat(verdict.findings()).hasSize(5);
        assertThat(verdict.findings()).allMatch(
                finding -> finding.policy() == ScreeningPolicy.SCREENING_POLICY_TAG);
    }

    @Test
    void onlyAnExplicitRefusePolicyRefusesAndNamesItsEvidence() {
        Screener screener = new Screener(new AdaEngine(0.9),
                config(ScreeningPolicy.SCREENING_POLICY_REFUSE, 0.5));

        assertThatThrownBy(() -> screener.screen(profile()))
                .isInstanceOfSatisfying(Screener.ScreeningRefusedException.class, e -> {
                    assertThat(e.finding().type()).isEqualTo("person");
                    assertThat(e.finding().modelVersion()).isEqualTo("fake-2.1");
                    assertThat(e.getMessage()).doesNotContain("Ada");
                });
    }

    @Test
    void aCleanMessagePassesUntouchedWithNoFindings() {
        Screener screener = new Screener(new AdaEngine(0.9),
                config(ScreeningPolicy.SCREENING_POLICY_MASK, 0.5));
        Profile clean = Profile.newBuilder()
                .setDisplayName("nobody here")
                .setBio("nothing to find")
                .putNotes("k", "plain note")
                .build();

        Screener.Verdict verdict = screener.screen(clean);

        assertThat(verdict.message()).isSameAs(clean);
        assertThat(verdict.findings()).isEmpty();
    }

    @Test
    void anUnspecifiedPolicyRefusesConstruction() {
        assertThatThrownBy(() -> new Screener(new AdaEngine(0.9),
                ScreeningConfig.newBuilder().setSensitivityClass("pii").build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("policy");
    }

    @Test
    void multipleSpansInOneValueMaskRightToLeft() {
        ScreeningEngine twoSpans = new ScreeningEngine() {
            @Override
            public List<Detection> detect(String text) {
                if (!text.startsWith("Ada met Babbage")) {
                    return List.of();
                }
                return List.of(new Detection("person", 0, 3, 0.9),
                        new Detection("person", 8, 15, 0.9));
            }

            @Override
            public String modelVersion() {
                return "fake-2.1";
            }
        };
        Screener screener = new Screener(twoSpans,
                config(ScreeningPolicy.SCREENING_POLICY_MASK, 0.5));

        Screener.Verdict verdict = screener.screen(Profile.newBuilder()
                .setBio("Ada met Babbage today").build());

        assertThat(((Profile) verdict.message()).getBio()).isEqualTo("*** met *** today");
        assertThat(verdict.findings()).hasSize(2);
    }

    @Test
    void theMountDocumentCarriesItsOwnContract() {
        // The config document's declared rules are the verify hook on the
        // config lane; pin them by ruleId here where the document lives.
        var validator = ai.pipestream.proto.validate.ProtoValidator.forMessageType(
                ScreeningConfig.getDescriptor());
        assertThat(validator.validate(config(ScreeningPolicy.SCREENING_POLICY_MASK, 0.5))
                .valid()).isTrue();

        var violations = validator.validate(ScreeningConfig.newBuilder()
                        .setSensitivityClass("Not-A-Slug")
                        .setThreshold(1.5)
                        .build())
                .violations();
        assertThat(violations).anyMatch(v ->
                v.path().equals("sensitivity_class") && v.ruleId().equals("string.slug"));
        assertThat(violations).anyMatch(v ->
                v.path().equals("model_ref") && v.ruleId().equals("required"));
        assertThat(violations).anyMatch(v ->
                v.path().equals("policy") && v.ruleId().equals("required"));
        assertThat(violations).anyMatch(v ->
                v.path().equals("threshold") && v.ruleId().startsWith("double."));
    }
}
