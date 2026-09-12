package ai.protomolt.proto.asset.characterize;

import ai.protomolt.proto.asset.v1.CharacterizationEvidence;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the wider published signature set adds for assets the registry
 * cannot place, and what it costs for the ones it can.
 */
class PublishedEvidenceTest {

    /**
     * An executable: two leading bytes the published set knows and the
     * registry has no entry for, which is exactly the case this covers.
     */
    private static byte[] executable() {
        byte[] content = new byte[1024];
        content[0] = 'M';
        content[1] = 'Z';
        content[2] = (byte) 0x90;
        return content;
    }

    /** Binary content written to resemble no published format. */
    private static byte[] noise() {
        byte[] content = new byte[600];
        for (int i = 0; i < content.length; i++) {
            content[i] = (byte) (i % 2 == 0 ? 0x00 : 0xEE);
        }
        return content;
    }

    private static List<CharacterizationEvidence> evidenceOf(byte[] content, String filename) {
        return Characterizer.identify(ByteWindows.ofWhole(content), filename).evidence();
    }

    private static List<String> signals(List<CharacterizationEvidence> evidence) {
        return evidence.stream().map(CharacterizationEvidence::getSignal).toList();
    }

    private static List<String> observations(List<CharacterizationEvidence> evidence) {
        return evidence.stream().map(CharacterizationEvidence::getObservation).toList();
    }

    @Test
    @DisplayName("an asset the registry cannot place is named by the published set")
    void namesTheUnplaceable() {
        byte[] content = executable();
        List<CharacterizationEvidence> evidence = evidenceOf(content, "mystery.bin");
        assertThat(Characterizer.identify(ByteWindows.ofWhole(content), "mystery.bin").fact())
                .isNull();
        assertThat(signals(evidence)).contains("published-signature");
        assertThat(observations(evidence))
                .anyMatch(o -> o.contains("the bytes carry the signature of"));
    }

    @Test
    @DisplayName("an asset the registry places pays nothing for the published set")
    void placedAssetsSkipIt() {
        // The published set costs a couple of thousand pattern runs. It is
        // worth that where the cheap path found nothing, and only there.
        byte[] pdf = "%PDF-1.4\nbody\n%%EOF\n".getBytes(StandardCharsets.ISO_8859_1);
        assertThat(signals(evidenceOf(pdf, "notes.pdf"))).doesNotContain("published-signature");
    }

    @Test
    @DisplayName("content matching nothing at all says how much was looked at")
    void reportsWhatWasEvaluated() {
        List<String> observations = observations(evidenceOf(noise(), "blob.bin"));
        assertThat(observations).anyMatch(o -> o.startsWith("no published signature matches:"));
        assertThat(observations).anyMatch(o -> o.contains("were evaluated"));
    }

    @Test
    @DisplayName("evidence stays inside the contract's limits")
    void staysWithinTheContract() {
        List<CharacterizationEvidence> evidence = evidenceOf(executable(), "mystery.bin");
        assertThat(evidence).hasSizeLessThanOrEqualTo(32);
        assertThat(evidence).allSatisfy(item -> {
            assertThat(item.getObservation()).isNotBlank().hasSizeLessThanOrEqualTo(500);
            assertThat(item.getSignal()).isNotBlank().hasSizeLessThanOrEqualTo(100);
        });
    }

    @Test
    @DisplayName("a published hit does not become a classification")
    void evidenceIsNotAConclusion() {
        // The registry names the formats the platform can act on. A
        // signature hit outside that list stays an observation.
        assertThat(Characterizer.identify(ByteWindows.ofWhole(executable()), "mystery.bin")
                .identified()).isFalse();
    }
}
