package ai.protomolt.proto.workflow;

import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Edge cases for the zero-regex replacements in the workflow path walks
 * and endpoint sanitizer, proven against the regex behavior they replaced
 * (the oracle may use regex; production code may not).
 */
class DotSegmentsTest {

    @Test
    void matchesDotSplitOnRepresentativePaths() {
        for (String path : new String[] {"", "a", "a.b", "a.b.", "a..b", ".a", ".", "..",
                "items", "nested.items", "a.b.c."}) {
            assertThat(DotSegments.segments(path))
                    .as("path '%s'", path)
                    .isEqualTo(path.split("\\."));
        }
    }

    @Test
    void endpointNameFoldsOutsideAlphabetToDash() {
        assertThat(WorkflowCompiler.endpointName("localhost:9090")).isEqualTo("localhost-9090");
        assertThat(WorkflowCompiler.endpointName("https://example.com/a b"))
                .isEqualTo("https---example.com-a-b");
        assertThat(WorkflowCompiler.endpointName("keep.ALL_of-these.9"))
                .isEqualTo("keep.ALL_of-these.9");
        assertThat(WorkflowCompiler.endpointName(":9090")).isEqualTo("e-9090");
        assertThat(WorkflowCompiler.endpointName("")).isEqualTo("e");
        assertThat(WorkflowCompiler.endpointName("x".repeat(200))).hasSize(128);
        for (String target : List.of("a@b#c$d%", "uniéode", "tab\there", "__..--")) {
            assertThat(WorkflowCompiler.endpointName(target))
                    .as("target '%s'", target)
                    .isEqualTo(sanitizedOracle(target));
        }
    }

    @Test
    void endpointNameFoldsSupplementaryCodePointsToOneDash() {
        // The replaced character class matched a supplementary character as one
        // code point, so one emoji folds to one dash, not two.
        assertThat(WorkflowCompiler.endpointName("x\uD83D\uDE00y")).isEqualTo("x-y");
        assertThat(WorkflowCompiler.endpointName("\uD83D\uDE00\uD83D\uDE00"))
                .isEqualTo("e--");
        // Lone surrogates are code points of their own: one dash each, matching
        // the character-class replacement on unpaired units.
        assertThat(WorkflowCompiler.endpointName("x\uD83Dy")).isEqualTo("x-y");
        assertThat(WorkflowCompiler.endpointName("x\uDE00y")).isEqualTo("x-y");
        assertThat(WorkflowCompiler.endpointName("x\uD83D\uD83Dy")).isEqualTo("x--y");
        for (String target : List.of("x\uD83D\uDE00y", "\uD83D\uDE00a.b", "a\uD83D",
                "\uDE00\uD83D\uDE00")) {
            assertThat(WorkflowCompiler.endpointName(target))
                    .as("target with supplementary characters")
                    .isEqualTo(sanitizedOracle(target));
        }
    }

    @Test
    void endpointTruncationCountsFoldedDashesAtTheBoundary() {
        // 127 kept characters plus one folded supplementary character fit in 128;
        // counting UTF-16 units instead would cut the result one dash early.
        String exactFit = "a".repeat(127) + "\uD83D\uDE00";
        assertThat(WorkflowCompiler.endpointName(exactFit))
                .isEqualTo("a".repeat(127) + "-").hasSize(128);
        String overBoundary = "a".repeat(128) + "\uD83D\uDE00tail";
        assertThat(WorkflowCompiler.endpointName(overBoundary))
                .isEqualTo("a".repeat(128)).hasSize(128);
        for (String target : List.of(exactFit, overBoundary, "b".repeat(126) + "\uD83D\uDE00c")) {
            assertThat(WorkflowCompiler.endpointName(target)).isEqualTo(sanitizedOracle(target));
        }
    }

    /** The original single-regex folding plus prefix/truncation rules, as oracle. */
    private static String sanitizedOracle(String target) {
        String sanitized = target.replaceAll("[^A-Za-z0-9._-]", "-");
        if (sanitized.isEmpty() || !Character.isLetterOrDigit(sanitized.charAt(0))) {
            sanitized = "e" + sanitized;
        }
        return sanitized.length() > 128 ? sanitized.substring(0, 128) : sanitized;
    }
}
