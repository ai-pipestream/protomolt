package ai.protomolt.proto.asset.characterize.signature;

import ai.protomolt.proto.asset.characterize.ByteWindows;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The compiler against the syntax as published, not as imagined: every
 * distinct byte-pattern expression in the format registry's signature
 * sets, compiled.
 *
 * <p>A hand-written test proves the compiler handles the constructs its
 * author thought of. This one proves it handles the constructs the
 * registry actually writes, which is a different and larger set.
 */
class SignatureCorpusTest {

    private static final String CORPUS = "signature-corpus.txt";

    private static List<String> corpus() throws IOException {
        List<String> expressions = new ArrayList<>();
        try (InputStream in = SignatureCorpusTest.class.getResourceAsStream(CORPUS)) {
            assertThat(in).as("the corpus fixture is on the test classpath").isNotNull();
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(in, StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank() && !line.startsWith("#")) {
                    expressions.add(line);
                }
            }
        }
        return expressions;
    }

    @Test
    @DisplayName("every published expression compiles")
    void everyExpressionCompiles() throws IOException {
        List<String> expressions = corpus();
        assertThat(expressions).hasSizeGreaterThan(3000);
        List<String> rejected = new ArrayList<>();
        for (String expression : expressions) {
            try {
                SignatureExpression.compile(expression);
            } catch (SignatureSyntaxException refused) {
                rejected.add(refused.getMessage());
            }
        }
        assertThat(rejected).isEmpty();
    }

    @Test
    @DisplayName("no published expression is unbounded, so all of them are evaluable")
    void everyExpressionIsBounded() throws IOException {
        // The window model rests on this. An unlimited stretch would mean
        // reading to the end of the content to decide one format, and the
        // published sets contain none; if that ever changes, this test is
        // where it surfaces rather than in a slow production read.
        List<String> unbounded = new ArrayList<>();
        for (String expression : corpus()) {
            try {
                if (!SignatureExpression.compile(expression).bounded()) {
                    unbounded.add(expression);
                }
            } catch (SignatureSyntaxException refused) {
                throw new AssertionError(refused);
            }
        }
        assertThat(unbounded).isEmpty();
    }

    @Test
    @DisplayName("the longest published expression stays inside the leading window")
    void lengthsFitTheWindows() throws IOException {
        int longest = 0;
        for (String expression : corpus()) {
            try {
                longest = Math.max(longest, SignatureExpression.compile(expression).maxLength());
            } catch (SignatureSyntaxException refused) {
                throw new AssertionError(refused);
            }
        }
        assertThat(longest).isPositive();
        assertThat(longest).isLessThan(ByteWindows.DEFAULT_HEAD_BYTES);
    }
}
