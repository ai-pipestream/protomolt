package ai.pipestream.proto.asset.characterize;

import ai.pipestream.proto.asset.v1.FormatFact;

import java.util.Map;
import java.util.Set;

/**
 * When a declaration and an identification name different formats, the
 * difference is a contradiction only when the bytes actually rule the
 * claim out. Identification often concludes a GENERALIZATION of the
 * truth — delimited tables sniff as plain text, OOXML documents sniff as
 * ZIP, a compressed tar sniffs as gzip — and a claim standing inside its
 * generalization is not in conflict with it.
 */
public final class FormatCompatibility {

    /**
     * identified-generalization → the declared arms that stand inside it.
     * Text generalizes the textual family; ZIP generalizes the OOXML
     * documents; gzip generalizes the compressed tar.
     */
    private static final Map<FormatFact.FormatCase, Set<FormatFact.FormatCase>> GENERALIZES =
            Map.of(
                    FormatFact.FormatCase.TEXT, Set.of(
                            FormatFact.FormatCase.DELIMITED,
                            FormatFact.FormatCase.NDJSON,
                            FormatFact.FormatCase.JSON,
                            FormatFact.FormatCase.XML,
                            FormatFact.FormatCase.YAML,
                            FormatFact.FormatCase.MARKDOWN,
                            FormatFact.FormatCase.HTML),
                    FormatFact.FormatCase.ZIP, Set.of(
                            FormatFact.FormatCase.WORD,
                            FormatFact.FormatCase.SPREADSHEET,
                            FormatFact.FormatCase.PRESENTATION),
                    FormatFact.FormatCase.GZIP, Set.of(
                            FormatFact.FormatCase.TAR));

    private FormatCompatibility() {
    }

    /**
     * Whether an identification contradicts a declaration. Same arm never
     * contradicts; a generalization of the declared arm never contradicts;
     * any other difference does.
     *
     * @param declared the producer's claim
     * @param identified characterization's conclusion
     * @return true when the bytes rule the claim out
     */
    public static boolean contradicts(FormatFact declared, FormatFact identified) {
        FormatFact.FormatCase claim = declared.getFormatCase();
        FormatFact.FormatCase conclusion = identified.getFormatCase();
        if (claim == conclusion) {
            return false;
        }
        return !GENERALIZES.getOrDefault(conclusion, Set.of()).contains(claim);
    }
}
