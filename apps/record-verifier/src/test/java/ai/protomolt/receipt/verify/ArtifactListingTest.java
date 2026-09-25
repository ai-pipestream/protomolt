package ai.protomolt.receipt.verify;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.receipt.ConformanceCorpus;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ArtifactListingTest {

    private static final byte[] TRUST = ConformanceCorpus.trust().toByteArray();

    @TempDir Path directory;

    @Test
    void listsTopLevelAndStepOnlyReferencesInSortedOrder() {
        byte[] record = ConformanceCorpus.fixtures().getFirst().record();
        List<String> expected = List.of("request", "response", "output").stream()
                .map(value -> ExternalVerifier.sha256Hex(value.getBytes()))
                .sorted().toList();

        assertThat(ExternalVerifier.verifiedArtifactDigests(record, TRUST))
                .containsExactlyElementsOf(expected);
    }

    @Test
    void refusesUntrustedAndMalformedRecordsBeforeListing() {
        for (String fixtureName : List.of("unknown-issuer", "forged-signature", "garbage-record")) {
            byte[] record = ConformanceCorpus.fixtures().stream()
                    .filter(fixture -> fixture.name().equals(fixtureName))
                    .findFirst().orElseThrow().record();
            assertThatThrownBy(() -> ExternalVerifier.verifiedArtifactDigests(record, TRUST))
                    .as(fixtureName).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("record refused by");
        }
    }

    @Test
    void loadsOnlySignedReferencesAndIgnoresUnrelatedFiles() throws Exception {
        byte[] record = ConformanceCorpus.fixtures().getFirst().record();
        for (String content : List.of("request", "response", "output")) {
            Files.write(directory.resolve(ExternalVerifier.sha256Hex(content.getBytes())),
                    content.getBytes());
        }
        String unrelated = ExternalVerifier.sha256Hex("another run's contact".getBytes());
        Files.writeString(directory.resolve(unrelated), "another run's contact");

        var loaded = Main.loadReferencedArtifacts(record, TRUST, directory);

        assertThat(loaded).hasSize(3).doesNotContainKey(unrelated);
        assertThat(ExternalVerifier.verify(record, TRUST, loaded).verified()).isTrue();
    }
}
