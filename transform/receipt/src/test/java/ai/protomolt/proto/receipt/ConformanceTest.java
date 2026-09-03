package ai.protomolt.proto.receipt;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** The corpus is the format's testable freeze: every fixture behaves as named. */
class ConformanceTest {

    static List<ConformanceCorpus.Fixture> fixtures() {
        return ConformanceCorpus.fixtures();
    }

    @ParameterizedTest
    @MethodSource("fixtures")
    void everyFixtureBehavesAsNamed(ConformanceCorpus.Fixture fixture) {
        Verification verification =
                RecordVerifier.verify(fixture.record(), ConformanceCorpus.trust());
        if (fixture.valid()) {
            assertThat(verification.verified())
                    .as("%s should verify; refusal: %s", fixture.name(),
                            verification.refusal())
                    .isTrue();
            assertThat(verification.manifest()).isNotNull();
        } else {
            assertThat(verification.verified())
                    .as("%s should refuse at %s", fixture.name(), fixture.failsAt())
                    .isFalse();
            assertThat(verification.refusal().id())
                    .as("%s refusal: %s", fixture.name(), verification.refusal())
                    .isEqualTo(fixture.failsAt());
        }
    }

    @Test
    void corpusCoversEveryCheck() {
        List<String> failures = fixtures().stream()
                .map(ConformanceCorpus.Fixture::failsAt)
                .filter(java.util.Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());
        assertThat(failures).containsExactlyInAnyOrder(
                RecordVerifier.CHECK_CONTAINER_BOUNDS,
                RecordVerifier.CHECK_MANIFEST_PARSE,
                RecordVerifier.CHECK_RESERIALIZATION_EQUALITY,
                RecordVerifier.CHECK_KEY_TRUSTED,
                RecordVerifier.CHECK_SIGNATURE_VALID,
                RecordVerifier.CHECK_ISSUER_AUTHORIZED,
                RecordVerifier.CHECK_COMPLETENESS_CONSISTENT);
    }

    /**
     * The byte freeze: fixed keys, fixed timestamps, deterministic Ed25519
     * and deterministic serialization make every fixture byte-stable, and
     * these digests pin them. A digest change here is a format change and
     * must be treated as one.
     */
    @Test
    void corpusBytesAreFrozen() {
        String digests = fixtures().stream()
                .map(fixture -> fixture.name() + ":"
                        + WorkRecords.sha256Hex(fixture.record()))
                .collect(Collectors.joining("\n"));
        assertThat(WorkRecords.sha256Hex(digests.getBytes())).isEqualTo(CORPUS_DIGEST);
    }

    private static final String CORPUS_DIGEST =
            "5f8840ddf95cf1a9ee440a46411a2d9577b4ecabf00cb840bba7fba425e2afe9";
}
