package ai.protomolt.proto.asset.characterize.signature;

import ai.protomolt.proto.asset.characterize.ByteWindows;
import ai.protomolt.proto.asset.characterize.signature.BinarySignatures.Format;
import ai.protomolt.proto.asset.characterize.signature.SignatureSequence.Outcome;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Running the published signature set over an asset's byte windows.
 *
 * <p>A format is identified when any one of the signatures listed for it
 * matches, and a signature matches when all of the byte sequences in it
 * match. Where the set says a format supersedes another, the superseded
 * one is dropped: a document that is both PDF and PDF/A is reported as
 * PDF/A, because saying both would be saying less.
 *
 * <p>Signatures reaching outside the windows are counted, not silently
 * skipped. A run that could not evaluate a hundred definitions is a
 * different fact from a run that evaluated all of them and found one
 * match, and the count is what lets a reader tell those apart.
 */
public final class BinaryIdentification {

    /** One format the bytes support. */
    public record Hit(String formatId, String name, String version, String mediaType) {

        /** The format's name with the version, when it has one. */
        public String label() {
            return version.isBlank() ? name : name + " " + version;
        }
    }

    /**
     * What running the set established.
     *
     * @param hits the formats identified, most specific first
     * @param evaluated how many signatures were decided
     * @param notEvaluable how many reached outside the windows
     * @param setVersion the published version of the set that ran
     */
    public record Result(List<Hit> hits, int evaluated, int notEvaluable, String setVersion) {

        /** Validates and copies. */
        public Result {
            hits = List.copyOf(hits);
        }

        /** Whether any format was identified. */
        public boolean identified() {
            return !hits.isEmpty();
        }
    }

    private BinaryIdentification() {
    }

    /**
     * Runs the bundled set.
     *
     * @param bytes the content's windows
     * @return what was established
     */
    public static Result identify(ByteWindows bytes) {
        return identify(bytes, BinarySignatures.bundled());
    }

    /**
     * Runs a signature set.
     *
     * @param bytes the content's windows
     * @param signatures the set to run
     * @return what was established
     */
    public static Result identify(ByteWindows bytes, BinarySignatures signatures) {
        if (bytes == null || bytes.isEmpty()) {
            return new Result(List.of(), 0, 0, signatures.version());
        }
        Map<String, Outcome> decided = new HashMap<>();
        int notEvaluable = 0;
        List<Format> matched = new ArrayList<>();
        for (Format format : signatures.formats()) {
            for (String id : format.signatureIds()) {
                Outcome outcome = decided.computeIfAbsent(id,
                        key -> run(signatures.signatures().get(key), bytes));
                if (outcome == Outcome.MATCHED) {
                    matched.add(format);
                    break;
                }
            }
        }
        for (Outcome outcome : decided.values()) {
            if (outcome == Outcome.NOT_EVALUABLE) {
                notEvaluable++;
            }
        }
        return new Result(rank(matched), decided.size() - notEvaluable, notEvaluable,
                signatures.version());
    }

    /** A signature holds when all of the sequences in it hold. */
    private static Outcome run(List<SignatureSequence> sequences, ByteWindows bytes) {
        if (sequences == null || sequences.isEmpty()) {
            return Outcome.NOT_MATCHED;
        }
        boolean blind = false;
        for (SignatureSequence sequence : sequences) {
            Outcome outcome = sequence.matchIn(bytes);
            if (outcome == Outcome.NOT_MATCHED) {
                return Outcome.NOT_MATCHED;
            }
            blind |= outcome == Outcome.NOT_EVALUABLE;
        }
        return blind ? Outcome.NOT_EVALUABLE : Outcome.MATCHED;
    }

    /** Drops the formats that a matching format supersedes. */
    private static List<Hit> rank(List<Format> matched) {
        Set<String> superseded = new HashSet<>();
        for (Format format : matched) {
            superseded.addAll(format.outranks());
        }
        List<Hit> hits = new ArrayList<>();
        for (Format format : matched) {
            if (!superseded.contains(format.id())) {
                hits.add(new Hit(format.formatId(), format.name(), format.version(),
                        format.mediaType()));
            }
        }
        for (Format format : matched) {
            if (superseded.contains(format.id())) {
                hits.add(new Hit(format.formatId(), format.name(), format.version(),
                        format.mediaType()));
            }
        }
        return hits;
    }
}
