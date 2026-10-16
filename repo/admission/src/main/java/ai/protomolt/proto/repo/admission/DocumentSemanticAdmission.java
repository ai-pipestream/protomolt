package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mapper.MappingException;
import ai.protomolt.proto.mapper.ProtoFieldMapperImpl;
import ai.protomolt.proto.repo.codec.SegmentationDigests;
import ai.protomolt.proto.repo.semantic.v1.EmbeddingSet;
import ai.protomolt.proto.repo.semantic.v1.EmbeddingSetKey;
import ai.protomolt.proto.repo.semantic.v1.OffsetUnit;
import ai.protomolt.proto.repo.semantic.v1.Segment;
import ai.protomolt.proto.repo.semantic.v1.Segmentation;
import ai.protomolt.proto.repo.semantic.v1.TextSource;
import ai.protomolt.proto.repo.v1.DocumentPart;
import com.google.protobuf.MessageOrBuilder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The cross-part rules between segmentations, their source text and embedding
 * sets (requirements §5 "Semantic results", §9; acceptance §15.1, §15.2, §15.15).
 * Transport validation cannot establish these relationships, so they run over
 * the composed candidate: the segmentations and embedding sets this save
 * writes, plus the carried ones whose content a rule needs.
 * <p>
 * For every present segmentation: segments are in contiguous zero-based ordinal
 * order with unique IDs; the source text resolves (a {@code field_path} through
 * {@link ProtoFieldMapperImpl} over the composed document view, or the carried
 * {@code DerivedText}); the binding's SHA-256 and UTF-8 length match that text;
 * every segment's {@code [start, end)} lies within the text in the declared
 * offset unit, on character boundaries, and its {@code text} is exactly that
 * span; a caller-supplied {@code content_digest} equals the computed one. These
 * are faults of the segmentation's own content: {@code VALIDATION_FAILED}.
 * <p>
 * For every embedding set, written or carried: its segmentation exists in the
 * composed candidate and its {@code segmentation_digest} equals that
 * segmentation's digest; a present set covers the present segmentation's
 * segment IDs exactly once each. These are binding faults between two parts:
 * {@code SEGMENTATION_MISMATCH}. Vector dimensions and finiteness are
 * ProtoValidator's rules on {@code EmbeddingSet} and {@code SegmentVector}.
 * <p>
 * Pure: no IO, no storage. The caller decides which carried parts to read.
 */
public final class DocumentSemanticAdmission {
    private DocumentSemanticAdmission() {}

    /** Which error a violation maps to. */
    public enum Kind {
        /** The segmentation's own content is wrong. */
        VALIDATION_FAILED,
        /** An embedding set does not bind to its segmentation. */
        SEGMENTATION_MISMATCH
    }

    /**
     * A segmentation whose content is in hand.
     *
     * @param segmentation the message, {@code content_digest} possibly empty
     * @param verifyContent whether its source binding, offsets and text are checked against the
     *                      composed view; false for a carried segmentation read only for its
     *                      segment IDs, whose source part is unchanged
     */
    public record PresentSegmentation(Segmentation segmentation, boolean verifyContent) {
        public PresentSegmentation { Objects.requireNonNull(segmentation); }
    }

    /**
     * The composed candidate's semantic content.
     *
     * @param present segmentations whose content is in hand, in candidate order
     * @param carriedDigests carried segmentations not read: ID to stored content digest
     * @param embeddingSets embedding sets whose content is in hand, in candidate order
     * @param carriedBindings carried embedding sets not read: key to the digest they are bound to
     * @param view the composed document, over the registered Document type, holding every
     *             part a {@code field_path} may name
     */
    public record Candidate(List<PresentSegmentation> present, Map<String, String> carriedDigests,
                            List<EmbeddingSet> embeddingSets, Map<EmbeddingSetKey, String> carriedBindings,
                            MessageOrBuilder view) {
        public Candidate {
            present = List.copyOf(present);
            carriedDigests = Map.copyOf(carriedDigests);
            embeddingSets = List.copyOf(embeddingSets);
            carriedBindings = Map.copyOf(carriedBindings);
            Objects.requireNonNull(view);
        }
    }

    /**
     * One failed rule.
     *
     * @param kind the error it maps to
     * @param segmentationId the segmentation it concerns
     * @param embeddingSetId the embedding set it concerns, or empty for a segmentation rule
     * @param fieldPath the field within that segmentation or set, or empty for the whole
     * @param ruleId the rule
     * @param message what was found
     */
    public record Violation(Kind kind, String segmentationId, String embeddingSetId, String fieldPath, String ruleId,
                            String message) {}

    /** The verdict. */
    public sealed interface Result {}

    /**
     * Every rule passed.
     *
     * @param digests the content digest of every present segmentation, by ID
     * @param finalized every present segmentation with its {@code content_digest} set, by ID
     */
    public record Accepted(Map<String, String> digests, Map<String, Segmentation> finalized) implements Result {
        public Accepted { digests = Map.copyOf(digests); finalized = Map.copyOf(finalized); }
    }

    /** At least one rule failed; every violation shares one kind. */
    public record Rejected(Kind kind, List<Violation> violations) implements Result {
        public Rejected {
            violations = List.copyOf(violations);
            if (violations.isEmpty()) throw new IllegalArgumentException("A rejection names its violations");
        }
    }

    /**
     * The part a text source's field path reads from under the default Document layout.
     *
     * @param fieldPath the {@code TextSource.field_path}
     * @return CORE, BLOBS or PARSED; a path into a semantic list is refused by {@link #check}
     */
    public static DocumentPart sourcePart(String fieldPath) {
        return DocumentCandidateAdmission.partOf(fieldPath);
    }

    /**
     * Checks the composed candidate.
     *
     * @param candidate the composed semantic content and document view
     * @return accepted with digests, or rejected with every violation found
     */
    public static Result check(Candidate candidate) {
        Objects.requireNonNull(candidate);
        var mapper = new ProtoFieldMapperImpl(new DescriptorRegistry(false));
        var violations = new ArrayList<Violation>();
        var digests = new LinkedHashMap<String, String>(candidate.carriedDigests());
        var finalized = new LinkedHashMap<String, Segmentation>();
        var segmentIds = new HashMap<String, java.util.Set<String>>();
        for (var present : candidate.present()) {
            var segmentation = present.segmentation();
            var id = segmentation.getSegmentationId();
            if (digests.containsKey(id) || finalized.containsKey(id))
                throw new IllegalArgumentException("Segmentation " + id + " appears twice in the candidate");
            var before = violations.size();
            var ids = checkSegments(segmentation, violations);
            if (present.verifyContent()) checkSource(segmentation, candidate.view(), mapper, violations);
            var digest = SegmentationDigests.digest(segmentation);
            if (!segmentation.getContentDigest().isEmpty() && !segmentation.getContentDigest().equals(digest))
                violations.add(new Violation(Kind.VALIDATION_FAILED, id, "", "content_digest", "segmentation.content-digest",
                        "content_digest " + segmentation.getContentDigest() + " differs from the computed " + digest));
            if (violations.size() == before) {
                digests.put(id, digest);
                finalized.put(id, segmentation.toBuilder().setContentDigest(digest).build());
                segmentIds.put(id, ids);
            }
        }
        if (!violations.isEmpty()) return new Rejected(Kind.VALIDATION_FAILED, violations);

        for (var set : candidate.embeddingSets()) checkBinding(set.getSegmentationId(), set.getEmbeddingSetId(),
                set.getSegmentationDigest(), set, digests, segmentIds, violations);
        for (var carried : candidate.carriedBindings().entrySet()) checkBinding(carried.getKey().getSegmentationId(),
                carried.getKey().getEmbeddingSetId(), carried.getValue(), null, digests, segmentIds, violations);
        if (!violations.isEmpty()) return new Rejected(Kind.SEGMENTATION_MISMATCH, violations);
        return new Accepted(digests, finalized);
    }

    // ------------------------------------------------------------------ segmentations

    /** Contiguous ordinals in list order and unique segment IDs; returns the IDs. */
    private static java.util.Set<String> checkSegments(Segmentation segmentation, List<Violation> violations) {
        var id = segmentation.getSegmentationId();
        var ids = new HashSet<String>();
        for (int i = 0; i < segmentation.getSegmentsCount(); i++) {
            var segment = segmentation.getSegments(i);
            if (segment.getOrdinal() != i)
                violations.add(new Violation(Kind.VALIDATION_FAILED, id, "", "segments[" + i + "].ordinal",
                        "segmentation.ordinal-order", "ordinal " + segment.getOrdinal() + " at position " + i
                        + "; segments are stored in ordinal order 0..N-1 and never reordered"));
            if (!ids.add(segment.getSegmentId()))
                violations.add(new Violation(Kind.VALIDATION_FAILED, id, "", "segments[" + i + "].segment_id",
                        "segmentation.segment-id-unique", "segment_id " + segment.getSegmentId() + " is repeated"));
        }
        return ids;
    }

    /** Resolves the source text, then checks the binding and every segment's span against it. */
    private static void checkSource(Segmentation segmentation, MessageOrBuilder view, ProtoFieldMapperImpl mapper,
                                    List<Violation> violations) {
        var id = segmentation.getSegmentationId();
        var source = segmentation.getSource();
        final String text;
        if (source.hasDerived()) {
            text = source.getDerived().getText();
        } else if (source.hasFieldPath()) {
            switch (resolve(source.getFieldPath(), view, mapper)) {
                case Resolved resolved -> text = resolved.text();
                case Unresolved unresolved -> {
                    violations.add(new Violation(Kind.VALIDATION_FAILED, id, "", "source.field_path",
                            "segmentation.source-resolves", unresolved.reason()));
                    return;
                }
            }
        } else {
            violations.add(new Violation(Kind.VALIDATION_FAILED, id, "", "source", "segmentation.source-resolves",
                    "a text source names a field path or carries derived text"));
            return;
        }
        var utf8 = text.getBytes(StandardCharsets.UTF_8);
        var binding = source.getBinding();
        var sha256 = sha256Hex(utf8);
        if (!binding.getResolvedTextSha256().equals(sha256))
            violations.add(new Violation(Kind.VALIDATION_FAILED, id, "", "source.binding.resolved_text_sha256",
                    "segmentation.binding-sha256", "the resolved text's SHA-256 is " + sha256));
        if (binding.getUtf8Length() != utf8.length)
            violations.add(new Violation(Kind.VALIDATION_FAILED, id, "", "source.binding.utf8_length",
                    "segmentation.binding-utf8-length", "the resolved text is " + utf8.length + " UTF-8 bytes"));
        var unit = segmentation.getOffsetUnit();
        if (unit == OffsetUnit.OFFSET_UNIT_UNSPECIFIED || unit == OffsetUnit.UNRECOGNIZED) {
            violations.add(new Violation(Kind.VALIDATION_FAILED, id, "", "offset_unit", "segmentation.offset-unit",
                    "offset_unit must be UTF8_BYTE, UTF16_CODE_UNIT or CODE_POINT"));
            return;
        }
        for (int i = 0; i < segmentation.getSegmentsCount(); i++)
            checkSpan(id, i, segmentation.getSegments(i), unit, text, utf8, violations);
    }

    private sealed interface Resolution {}
    private record Resolved(String text) implements Resolution {}
    private record Unresolved(String reason) implements Resolution {}

    /** Resolves a field path over the composed view to its text, or to the reason it does not resolve to a string. */
    private static Resolution resolve(String fieldPath, MessageOrBuilder view, ProtoFieldMapperImpl mapper) {
        var owner = sourcePart(fieldPath);
        if (owner == DocumentPart.DOCUMENT_PART_CHUNKS || owner == DocumentPart.DOCUMENT_PART_EMBEDDINGS)
            return new Unresolved("field_path " + fieldPath
                    + " names a semantic part; a segmentation sources text from CORE, BLOBS or PARSED");
        final Object value;
        try {
            value = mapper.getValue(view, fieldPath);
        } catch (MappingException failure) {
            return new Unresolved("field_path " + fieldPath + " does not resolve: " + failure.getMessage());
        }
        if (value == null) return new Unresolved("field_path " + fieldPath + " is not set in the composed document");
        if (!(value instanceof String text)) return new Unresolved("field_path " + fieldPath + " is not a string field");
        return new Resolved(text);
    }

    /** {@code [start, end)} within the text in the declared unit, on character boundaries, with matching text. */
    private static void checkSpan(String id, int index, Segment segment, OffsetUnit unit, String text, byte[] utf8,
                                  List<Violation> violations) {
        var prefix = "segments[" + index + "]";
        long length = switch (unit) {
            case OFFSET_UNIT_UTF8_BYTE -> utf8.length;
            case OFFSET_UNIT_UTF16_CODE_UNIT -> text.length();
            case OFFSET_UNIT_CODE_POINT -> text.codePointCount(0, text.length());
            default -> throw new IllegalStateException("unit checked by caller");
        };
        long start = segment.getStart(), end = segment.getEnd();
        if (start < 0 || end < start || end > length) {
            violations.add(new Violation(Kind.VALIDATION_FAILED, id, "", prefix, "segmentation.segment-range",
                    "[" + start + ", " + end + ") is outside the source text of " + length + " " + unitName(unit)));
            return;
        }
        final String span;
        switch (unit) {
            case OFFSET_UNIT_UTF8_BYTE -> {
                if (continuation(utf8, (int) start) || continuation(utf8, (int) end)) {
                    violations.add(new Violation(Kind.VALIDATION_FAILED, id, "", prefix, "segmentation.segment-boundary",
                            "[" + start + ", " + end + ") splits a multi-byte UTF-8 sequence"));
                    return;
                }
                span = new String(Arrays.copyOfRange(utf8, (int) start, (int) end), StandardCharsets.UTF_8);
            }
            case OFFSET_UNIT_UTF16_CODE_UNIT -> {
                if (splitsPair(text, (int) start) || splitsPair(text, (int) end)) {
                    violations.add(new Violation(Kind.VALIDATION_FAILED, id, "", prefix, "segmentation.segment-boundary",
                            "[" + start + ", " + end + ") splits a surrogate pair"));
                    return;
                }
                span = text.substring((int) start, (int) end);
            }
            case OFFSET_UNIT_CODE_POINT -> {
                int from = text.offsetByCodePoints(0, (int) start);
                span = text.substring(from, text.offsetByCodePoints(from, (int) (end - start)));
            }
            default -> throw new IllegalStateException("unit checked by caller");
        }
        if (!span.equals(segment.getText()))
            violations.add(new Violation(Kind.VALIDATION_FAILED, id, "", prefix + ".text", "segmentation.segment-text",
                    "text differs from the source over [" + start + ", " + end + ")"));
    }

    private static boolean continuation(byte[] utf8, int index) {
        return index > 0 && index < utf8.length && (utf8[index] & 0xC0) == 0x80;
    }

    private static boolean splitsPair(String text, int index) {
        return index > 0 && index < text.length()
                && Character.isHighSurrogate(text.charAt(index - 1)) && Character.isLowSurrogate(text.charAt(index));
    }

    private static String unitName(OffsetUnit unit) {
        return switch (unit) {
            case OFFSET_UNIT_UTF8_BYTE -> "UTF-8 bytes";
            case OFFSET_UNIT_UTF16_CODE_UNIT -> "UTF-16 code units";
            case OFFSET_UNIT_CODE_POINT -> "code points";
            default -> unit.name();
        };
    }

    // ------------------------------------------------------------------ embedding sets

    /**
     * The set's segmentation is in the candidate with the digest the set is bound to; a
     * present set covers a present segmentation's segment IDs exactly once each.
     */
    private static void checkBinding(String segmentationId, String setId, String boundDigest, EmbeddingSet set,
                                     Map<String, String> digests, Map<String, java.util.Set<String>> segmentIds,
                                     List<Violation> violations) {
        var digest = digests.get(segmentationId);
        if (digest == null) {
            violations.add(new Violation(Kind.SEGMENTATION_MISMATCH, segmentationId, setId, "segmentation_id",
                    "embedding-set.segmentation-present", "segmentation " + segmentationId + " is not in the composed document"));
            return;
        }
        if (!digest.equals(boundDigest)) {
            violations.add(new Violation(Kind.SEGMENTATION_MISMATCH, segmentationId, setId, "segmentation_digest",
                    "embedding-set.segmentation-digest", "bound to digest " + boundDigest + " but the segmentation's digest is "
                    + digest + "; vectors are never reattached to new text"));
            return;
        }
        var ids = segmentIds.get(segmentationId);
        if (set == null || ids == null) return;
        var covered = new HashSet<String>();
        for (int i = 0; i < set.getVectorsCount(); i++) {
            var vector = set.getVectors(i);
            if (!ids.contains(vector.getSegmentId()))
                violations.add(new Violation(Kind.SEGMENTATION_MISMATCH, segmentationId, setId, "vectors[" + i + "].segment_id",
                        "embedding-set.vector-coverage", "segment " + vector.getSegmentId() + " is not in the segmentation"));
            else if (!covered.add(vector.getSegmentId()))
                violations.add(new Violation(Kind.SEGMENTATION_MISMATCH, segmentationId, setId, "vectors[" + i + "].segment_id",
                        "embedding-set.vector-coverage", "segment " + vector.getSegmentId() + " has two vectors"));
        }
        if (covered.size() != ids.size()) {
            var missing = new java.util.TreeSet<>(ids);
            missing.removeAll(covered);
            violations.add(new Violation(Kind.SEGMENTATION_MISMATCH, segmentationId, setId, "vectors",
                    "embedding-set.vector-coverage", "no vector for segments " + missing));
        }
    }

    static String sha256Hex(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    /** The source text's part for every present segmentation that names a field path, by segmentation ID. */
    public static Map<String, DocumentPart> sourceParts(List<Segmentation> segmentations) {
        var parts = new LinkedHashMap<String, DocumentPart>();
        for (var segmentation : segmentations) {
            var source = segmentation.getSource();
            if (source.getSourceCase() == TextSource.SourceCase.FIELD_PATH)
                parts.put(segmentation.getSegmentationId(), sourcePart(source.getFieldPath()));
        }
        return parts;
    }
}
