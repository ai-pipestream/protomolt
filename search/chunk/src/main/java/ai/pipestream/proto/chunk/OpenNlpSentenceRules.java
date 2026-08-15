package ai.pipestream.proto.chunk;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.sentdetect.ThreadSafeSentenceDetectorME;
import opennlp.tools.util.Span;
import opennlp.tools.util.StringUtil;

/**
 * Boundary rule set {@code opennlp-v1}: sentence segmentation by OpenNLP's
 * sentence detector over the pinned English UD-EWT model
 * ({@code org.apache.opennlp:opennlp-models-sentdetect-en:1.3.0}, loaded from
 * the classpath, never downloaded), with tokens delimited by Unicode
 * whitespace ({@link StringUtil#isUnicodeWhitespace(int)}) so non-ASCII text
 * tokenizes by the Unicode White_Space property rather than the JDK's
 * ASCII-leaning {@code Character.isWhitespace}.
 *
 * <p>Frozen under this id like every boundary rule set: a different model,
 * model version, language, or whitespace semantics is a NEW rule-set id,
 * because any of them moves boundaries and must re-derive corpora explicitly
 * through the policy digest.
 */
final class OpenNlpSentenceRules {

    /** The pinned rule-set id. */
    static final String ID = "opennlp-v1";

    /** The model resource, bundled at the jar root by the pinned models artifact. */
    private static final String MODEL_RESOURCE = "opennlp-en-ud-ewt-sentence-1.3-2.5.4.bin";

    /** Process-wide detector; the model is immutable and the detector thread-safe. */
    private static volatile ThreadSafeSentenceDetectorME detector;

    private OpenNlpSentenceRules() {
    }

    /** Segments the text; blank input yields no sentences. */
    static List<SentenceRules.Sentence> segment(String text) {
        List<SentenceRules.Sentence> sentences = new ArrayList<>();
        for (Span span : detector().sentPosDetect(text)) {
            List<BoundaryRules.TokenSpan> tokens = tokens(text, span.getStart(), span.getEnd());
            if (!tokens.isEmpty()) {
                // Span edges snap to the tokens inside them, so a detector
                // span carrying surrounding whitespace still trims clean.
                sentences.add(new SentenceRules.Sentence(
                        tokens.getFirst().start(), tokens.getLast().end(), tokens.size()));
            }
        }
        return sentences;
    }

    /** Token spans within {@code [start, end)}: maximal runs between Unicode whitespace. */
    static List<BoundaryRules.TokenSpan> tokens(String text, int start, int end) {
        List<BoundaryRules.TokenSpan> spans = new ArrayList<>();
        int i = start;
        while (i < end) {
            int codePoint = text.codePointAt(i);
            if (StringUtil.isUnicodeWhitespace(codePoint)) {
                i += Character.charCount(codePoint);
                continue;
            }
            int tokenStart = i;
            i += Character.charCount(codePoint);
            while (i < end && !StringUtil.isUnicodeWhitespace(text.codePointAt(i))) {
                i += Character.charCount(text.codePointAt(i));
            }
            spans.add(new BoundaryRules.TokenSpan(tokenStart, i));
        }
        return spans;
    }

    private static ThreadSafeSentenceDetectorME detector() {
        ThreadSafeSentenceDetectorME loaded = detector;
        if (loaded == null) {
            synchronized (OpenNlpSentenceRules.class) {
                if (detector == null) {
                    detector = load();
                }
                loaded = detector;
            }
        }
        return loaded;
    }

    private static ThreadSafeSentenceDetectorME load() {
        InputStream resource =
                OpenNlpSentenceRules.class.getClassLoader().getResourceAsStream(MODEL_RESOURCE);
        if (resource == null) {
            throw new IllegalStateException("boundary rules " + ID + " require the pinned"
                    + " sentence model '" + MODEL_RESOURCE + "' on the classpath"
                    + " (org.apache.opennlp:opennlp-models-sentdetect-en)");
        }
        try (resource) {
            return new ThreadSafeSentenceDetectorME(new SentenceModel(resource));
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "cannot read the pinned sentence model " + MODEL_RESOURCE, e);
        }
    }
}
