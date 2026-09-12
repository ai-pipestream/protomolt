package ai.protomolt.proto.asset.characterize.signature;

import ai.protomolt.proto.asset.characterize.ByteWindows;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The matcher against DROID's own conformance corpus: skeleton files built
 * by the {@code skeleton-test-suite-generator} project, each one hand
 * constructed to hold exactly the bytes one PRONOM internal signature
 * needs and nothing else.
 *
 * <p>A hand-written test proves the matcher handles the shapes its author
 * thought of. These files were built by people who read the published
 * signature definitions one at a time and asked "what is the smallest
 * thing that satisfies this", which routinely finds shapes a hand-written
 * test does not: an anchor placed by distance rather than magic, a
 * fragment chained outward from a subsequence, a set of alternative
 * subsequences with nothing in common.
 *
 * <p>Each file's name encodes what it was built to prove: {@code
 * fmt-136-signature-id-203.odt} is a construction for internal signature
 * {@code 203}, expected to be reported as {@code fmt/136}. The vendored
 * subset here is chosen for construct diversity — leading and trailing
 * anchors, unanchored search, left and right fragments alone and
 * combined, multi-subsequence signatures, bracketed byte ranges, and
 * subsequences placed hundreds of bytes from their anchor — not for
 * format coverage, so a failure here points at a specific mechanism
 * rather than a specific format.
 */
class SkeletonConformanceTest {

    private static final String SKELETON_DIR = "skeletons/";
    private static final Pattern NAME =
            Pattern.compile("(fmt|x-fmt)-(\\d+)-signature-id-(\\d+)\\.[^.]+");

    /** The vendored skeleton filenames, read from the manifest bundled with them. */
    private static List<String> skeletonNames() {
        try (InputStream in = SkeletonConformanceTest.class
                .getResourceAsStream(SKELETON_DIR + "manifest.txt")) {
            assertThat(in).as("the skeleton manifest is on the test classpath").isNotNull();
            String text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            return text.lines().filter(line -> !line.isBlank()).toList();
        } catch (IOException unreadable) {
            throw new UncheckedIOException(unreadable);
        }
    }

    @TestFactory
    @DisplayName("each vendored skeleton is identified as the PUID its name declares")
    Stream<DynamicTest> everySkeletonIdentifiesAsItsOwnPuid() {
        BinarySignatures signatures = BinarySignatures.bundled();
        List<String> names = skeletonNames();
        assertThat(names).hasSizeGreaterThanOrEqualTo(40);
        return names.stream().map(name -> DynamicTest.dynamicTest(name, () -> {
            Matcher matcher = NAME.matcher(name);
            assertThat(matcher.matches()).as("skeleton filename encodes its PUID: " + name).isTrue();
            String puid = matcher.group(1) + "/" + matcher.group(2);
            byte[] bytes;
            try (InputStream in = SkeletonConformanceTest.class
                    .getResourceAsStream(SKELETON_DIR + name)) {
                assertThat(in).as("skeleton on the test classpath: " + name).isNotNull();
                bytes = in.readAllBytes();
            }
            BinaryIdentification.Result result =
                    BinaryIdentification.identify(ByteWindows.ofWhole(bytes), signatures);
            List<String> hits = result.hits().stream()
                    .map(BinaryIdentification.Hit::formatId).toList();
            assertThat(hits).as(name + " should be identified as " + puid).contains(puid);
        }));
    }
}
