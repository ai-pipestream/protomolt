package ai.protomolt.receipt.verify;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.formats.Formats;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * {@link Rules} restates the platform's format parsers by hand, because the verifier ships
 * without the platform on its classpath. A hand copy drifts: the platform's parser gets a
 * fix and this one does not, and then a record the platform calls well-formed is refused
 * here, or worse, the other way round.
 *
 * <p>So the tests do two things. They state the grammar directly, which is what a reader
 * comes here to learn, and they hold the copy against the original, which is what keeps it
 * a copy. The original is a test-only dependency; the main source set stays JDK-only, and
 * {@code CrossCheckTest} pins that separately.
 */
class RulesTest {

    // --- the slug grammar -------------------------------------------------------

    @ParameterizedTest
    @ValueSource(strings = {"a", "0", "abc", "a-b", "a.b", "a_b", "a1-b2.c3_d4",
            "protomolt", "workflow-run", "v1.2.3"})
    void slugsAreAcceptedAsThePlatformAcceptsThem(String value) {
        assertThat(Rules.isSlug(value)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "",         // empty
        "A",        // uppercase
        "-a",       // leading separator
        "a-",       // trailing separator
        ".",        // separator alone
        "a--b",     // touching separators
        "a..b",
        "a-_b",     // touching separators of different kinds
        "a b",      // space
        "a/b",      // path separator
        "a+b",
        "café",     // non-ASCII letter
        "日本",      // non-ASCII script
    })
    void nonSlugsAreRefused(String value) {
        assertThat(Rules.isSlug(value)).isFalse();
    }

    @Test
    void aRequiredSlugNamesWhichWayItFailed() {
        assertThat(Rules.requiredSlug("", 10, "issuer")).isEqualTo("issuer: is required");
        assertThat(Rules.requiredSlug("abcdefghijk", 10, "issuer"))
                .isEqualTo("issuer: at most 10 characters");
        assertThat(Rules.requiredSlug("Nope", 10, "issuer")).isEqualTo("issuer: must be a slug");
        assertThat(Rules.requiredSlug("fine", 10, "issuer")).isNull();
    }

    @Test
    void theLengthBoundCountsCodePointsNotCharUnits() {
        // Two astral code points occupy four char units. A bound of two must admit them by
        // the documented rule, so the failure that follows is the slug rule, not length.
        String astral = "📄📄";
        assertThat(astral.length()).isEqualTo(4);
        assertThat(Rules.codePoints(astral)).isEqualTo(2);
        assertThat(Rules.requiredSlug(astral, 2, "name")).isEqualTo("name: must be a slug");
        assertThat(Rules.requiredSlug(astral, 1, "name")).isEqualTo("name: at most 1 characters");
    }

    @Test
    void theBoundIsInclusive() {
        assertThat(Rules.requiredSlug("abcde", 5, "name")).isNull();
        assertThat(Rules.requiredSlug("abcdef", 5, "name")).isEqualTo("name: at most 5 characters");
    }

    // --- the digest grammar -----------------------------------------------------

    @Test
    void aSha256DigestIsSixtyFourLowercaseHexCharacters() {
        String valid = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assertThat(Rules.isSha256Hex(valid)).isTrue();
        assertThat(Rules.isSha256Hex(valid.toUpperCase(java.util.Locale.ROOT))).isFalse();
        assertThat(Rules.isSha256Hex(valid.substring(1))).isFalse();
        assertThat(Rules.isSha256Hex(valid + "0")).isFalse();
        assertThat(Rules.isSha256Hex(valid.substring(0, 63) + "g")).isFalse();
        assertThat(Rules.isSha256Hex("")).isFalse();
    }

    // --- the media-type grammar -------------------------------------------------

    @ParameterizedTest
    @ValueSource(strings = {"text/plain", "application/json", "application/vnd.api+json",
            "application/x-protobuf", "text/x.custom", "a/b"})
    void mediaTypesAreAcceptedAsThePlatformAcceptsThem(String value) {
        assertThat(Rules.isMediaType(value)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "",                     // empty
        "text",                 // no subtype
        "/plain",               // empty type
        "text/",                // empty subtype
        "text/plain/extra",     // two slashes
        "text/plain; charset=utf-8",  // parameters are a header's business, not a type's
        ".text/plain",          // non-alphanumeric first character
        "text/.plain",
        "text/pla in",          // space
    })
    void nonMediaTypesAreRefused(String value) {
        assertThat(Rules.isMediaType(value)).isFalse();
    }

    @Test
    void aRestrictedNameRunsToOneHundredAndTwentySevenCharacters() {
        String at127 = "a".repeat(127);
        assertThat(Rules.isMediaType(at127 + "/" + at127)).isTrue();
        assertThat(Rules.isMediaType("a".repeat(128) + "/plain")).isFalse();
        assertThat(Rules.isMediaType("text/" + "a".repeat(128))).isFalse();
    }

    // --- the drift guard --------------------------------------------------------

    /**
     * Every string the other tests use, plus generated ones drawn from an alphabet chosen
     * to sit on the boundaries: the separators, the RFC 6838 punctuation, a case change, a
     * non-ASCII character. Each has to get the same verdict from both implementations.
     */
    @Test
    void theHandCopyAgreesWithThePlatformParsers() {
        List<String> corpus = corpus();
        List<String> drift = new ArrayList<>();
        for (String value : corpus) {
            if (Rules.isSlug(value) != Formats.isSlug(value)) {
                drift.add("isSlug(" + value + "): copy=" + Rules.isSlug(value)
                        + " platform=" + Formats.isSlug(value));
            }
            if (Rules.isMediaType(value) != Formats.isMediaType(value)) {
                drift.add("isMediaType(" + value + "): copy=" + Rules.isMediaType(value)
                        + " platform=" + Formats.isMediaType(value));
            }
            if (Rules.isSha256Hex(value) != Formats.isSha256Hex(value)) {
                drift.add("isSha256Hex(" + value + "): copy=" + Rules.isSha256Hex(value)
                        + " platform=" + Formats.isSha256Hex(value));
            }
        }
        assertThat(drift).as("the hand copy has drifted from the platform parsers").isEmpty();
        assertThat(corpus.stream().filter(Rules::isSlug).count())
                .as("slugs in the corpus").isGreaterThan(20);
        assertThat(corpus.stream().filter(Rules::isMediaType).count())
                .as("media types in the corpus").isGreaterThan(20);
    }

    private static List<String> corpus() {
        List<String> corpus = new ArrayList<>(List.of(
                "", "a", "A", "0", "-", ".", "_", "/", "+", "a-b", "a.b", "a_b", "a--b",
                "a/b", "text/plain", "application/vnd.api+json", "a/", "/a", "a//b",
                "café", "📄", "text/plain; charset=utf-8", " ", "a b",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855"));
        // Exhaustive over short strings from a boundary alphabet, then random longer ones.
        char[] alphabet = {'a', 'z', '0', '9', 'A', '-', '.', '_', '/', '+', '!', '#', 'é'};
        for (char first : alphabet) {
            for (char second : alphabet) {
                corpus.add("" + first + second);
                for (char third : alphabet) {
                    corpus.add("" + first + second + third);
                }
            }
        }
        Random random = new Random(5028841971693993L);
        for (int i = 0; i < 4_000; i++) {
            StringBuilder value = new StringBuilder();
            int length = 1 + random.nextInt(12);
            for (int j = 0; j < length; j++) {
                value.append(alphabet[random.nextInt(alphabet.length)]);
            }
            corpus.add(value.toString());
        }
        return corpus;
    }
}
