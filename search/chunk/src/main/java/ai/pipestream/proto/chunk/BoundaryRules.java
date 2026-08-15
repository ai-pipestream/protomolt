package ai.pipestream.proto.chunk;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The boundary rule sets the sentence-packed chunker can execute, keyed by
 * the id a {@code ChunkingPolicy.ChunkingSpec} pins. A rule set is frozen
 * under its id forever: new segmentation behavior lands as a new constant
 * here, never as an edit to an existing one, so a policy digest always
 * means exactly one segmentation.
 */
enum BoundaryRules {

    /** {@code rules-v1}: the hand-rolled JDK-pinned rules, see {@link SentenceRules}. */
    RULES_V1(SentenceRules.ID) {
        @Override
        List<SentenceRules.Sentence> segment(String text) {
            return SentenceRules.segment(text);
        }

        @Override
        List<TokenSpan> tokens(String text, int start, int end) {
            return SentenceRules.tokenSpans(text, start, end);
        }
    },

    /** {@code opennlp-v1}: OpenNLP sentence detection, see {@link OpenNlpSentenceRules}. */
    OPENNLP_V1(OpenNlpSentenceRules.ID) {
        @Override
        List<SentenceRules.Sentence> segment(String text) {
            return OpenNlpSentenceRules.segment(text);
        }

        @Override
        List<TokenSpan> tokens(String text, int start, int end) {
            return OpenNlpSentenceRules.tokens(text, start, end);
        }
    };

    private final String id;

    BoundaryRules(String id) {
        this.id = id;
    }

    /** The pinned rule-set id a policy names. */
    String id() {
        return id;
    }

    /** Sentence spans of the text: trimmed, in order, with token counts. */
    abstract List<SentenceRules.Sentence> segment(String text);

    /** Token spans within {@code [start, end)} under this rule set's whitespace semantics. */
    abstract List<TokenSpan> tokens(String text, int start, int end);

    /**
     * The rule set pinned under {@code id}.
     *
     * @throws IllegalArgumentException when the id names no carried rule set,
     *         listing the ids that are carried, so a policy never silently
     *         executes on the wrong segmentation
     */
    static BoundaryRules forId(String id) {
        for (BoundaryRules rules : values()) {
            if (rules.id.equals(id)) {
                return rules;
            }
        }
        throw new IllegalArgumentException("unknown boundary rule set '" + id
                + "'; this chunker carries: "
                + Arrays.stream(values()).map(BoundaryRules::id)
                        .collect(Collectors.joining(", ")));
    }

    /** A token span in the source text, half-open. */
    record TokenSpan(int start, int end) {
    }
}
