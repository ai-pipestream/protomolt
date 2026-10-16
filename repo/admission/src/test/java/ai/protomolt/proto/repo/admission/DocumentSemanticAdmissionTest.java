package ai.protomolt.proto.repo.admission;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.admission.DocumentSemanticAdmission.Accepted;
import ai.protomolt.proto.repo.admission.DocumentSemanticAdmission.Candidate;
import ai.protomolt.proto.repo.admission.DocumentSemanticAdmission.Kind;
import ai.protomolt.proto.repo.admission.DocumentSemanticAdmission.PresentSegmentation;
import ai.protomolt.proto.repo.admission.DocumentSemanticAdmission.Rejected;
import ai.protomolt.proto.repo.admission.DocumentSemanticAdmission.Violation;
import ai.protomolt.proto.repo.codec.SegmentationDigests;
import ai.protomolt.proto.repo.semantic.v1.DerivedText;
import ai.protomolt.proto.repo.semantic.v1.EmbeddingSet;
import ai.protomolt.proto.repo.semantic.v1.EmbeddingSetKey;
import ai.protomolt.proto.repo.semantic.v1.OffsetUnit;
import ai.protomolt.proto.repo.semantic.v1.Segment;
import ai.protomolt.proto.repo.semantic.v1.SegmentVector;
import ai.protomolt.proto.repo.semantic.v1.Segmentation;
import ai.protomolt.proto.repo.semantic.v1.SegmentationKind;
import ai.protomolt.proto.repo.semantic.v1.Similarity;
import ai.protomolt.proto.repo.semantic.v1.SourceTextBinding;
import ai.protomolt.proto.repo.semantic.v1.TextSource;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DocumentSemanticAdmissionTest {
    /** 24 UTF-8 bytes, 21 UTF-16 code units, 20 code points: "Hello, wörld! 😀 end." */
    static final String TEXT = "Hello, wörld! 😀 end.";
    static final Document VIEW = Document.newBuilder().setDocId("d")
            .setSearchMetadata(SearchMetadata.newBuilder().setTitle("t").setBody(TEXT)).build();

    @Test void acceptsEveryOffsetUnitAndComputesDigests() {
        var utf8 = segmentation("u8", OffsetUnit.OFFSET_UNIT_UTF8_BYTE, List.of(
                segment("a", 0, "Hello,", 0, 6), segment("b", 1, "wörld!", 7, 14), segment("c", 2, "😀", 15, 19),
                segment("d", 3, "end.", 20, 24)));
        var utf16 = segmentation("u16", OffsetUnit.OFFSET_UNIT_UTF16_CODE_UNIT, List.of(
                segment("a", 0, "wörld!", 7, 13), segment("b", 1, "😀", 14, 16), segment("c", 2, "end.", 17, 21)));
        var points = segmentation("cp", OffsetUnit.OFFSET_UNIT_CODE_POINT, List.of(
                segment("a", 0, "😀 end.", 14, 20), segment("b", 1, "", 3, 3)));
        var result = DocumentSemanticAdmission.check(new Candidate(
                List.of(present(utf8), present(utf16), present(points)), Map.of(), List.of(), Map.of(), VIEW));
        var accepted = (Accepted) result;
        assertThat(accepted.digests()).containsOnlyKeys("u8", "u16", "cp");
        assertThat(accepted.digests().get("u8")).isEqualTo(SegmentationDigests.digest(utf8));
        assertThat(accepted.finalized().get("u8").getContentDigest()).isEqualTo(SegmentationDigests.digest(utf8));
        assertThat(accepted.finalized().get("u8").toBuilder().clearContentDigest().build()).isEqualTo(utf8);
    }

    @Test void derivedTextNeedsNoDocumentField() {
        var derived = derived("dv", "normalized text", List.of(segment("a", 0, "normalized", 0, 10)));
        var empty = Document.getDefaultInstance();
        var result = DocumentSemanticAdmission.check(new Candidate(List.of(present(derived)), Map.of(), List.of(), Map.of(), empty));
        assertThat(result).isInstanceOf(Accepted.class);
        var wrongBinding = derived.toBuilder();
        wrongBinding.getSourceBuilder().getBindingBuilder().setUtf8Length(3);
        assertThat(rules(reject(wrongBinding.build(), empty))).containsExactly("segmentation.binding-utf8-length");
    }

    @Test void ordinalOrderAndUniqueIdsAreRequiredAndNeverRepaired() {
        var outOfOrder = segmentation("s", OffsetUnit.OFFSET_UNIT_UTF8_BYTE, List.of(
                segment("b", 1, "wörld!", 7, 14), segment("a", 0, "Hello,", 0, 6)));
        var rejected = reject(outOfOrder, VIEW);
        assertThat(rejected.kind()).isEqualTo(Kind.VALIDATION_FAILED);
        assertThat(rejected.violations()).extracting(Violation::ruleId, Violation::fieldPath).containsExactly(
                org.assertj.core.groups.Tuple.tuple("segmentation.ordinal-order", "segments[0].ordinal"),
                org.assertj.core.groups.Tuple.tuple("segmentation.ordinal-order", "segments[1].ordinal"));
        var duplicate = segmentation("s", OffsetUnit.OFFSET_UNIT_UTF8_BYTE, List.of(
                segment("a", 0, "Hello,", 0, 6), segment("a", 1, "wörld!", 7, 14)));
        assertThat(rules(reject(duplicate, VIEW))).containsExactly("segmentation.segment-id-unique");
        // A valid different order is a different segmentation, not a repair target.
        var swapped = segmentation("s", OffsetUnit.OFFSET_UNIT_UTF8_BYTE, List.of(
                segment("b", 0, "wörld!", 7, 14), segment("a", 1, "Hello,", 0, 6)));
        var forward = segmentation("s", OffsetUnit.OFFSET_UNIT_UTF8_BYTE, List.of(
                segment("a", 0, "Hello,", 0, 6), segment("b", 1, "wörld!", 7, 14)));
        var digests = ((Accepted) DocumentSemanticAdmission.check(new Candidate(List.of(present(swapped)), Map.of(), List.of(), Map.of(), VIEW))).digests();
        var other = ((Accepted) DocumentSemanticAdmission.check(new Candidate(List.of(present(forward)), Map.of(), List.of(), Map.of(), VIEW))).digests();
        assertThat(digests.get("s")).isNotEqualTo(other.get("s"));
    }

    @Test void bindingOffsetsBoundariesAndTextAreChecked() {
        var base = segmentation("s", OffsetUnit.OFFSET_UNIT_UTF8_BYTE, List.of(segment("a", 0, "Hello,", 0, 6)));
        var badSha = base.toBuilder();
        badSha.getSourceBuilder().getBindingBuilder().setResolvedTextSha256("0".repeat(64));
        assertThat(rules(reject(badSha.build(), VIEW))).containsExactly("segmentation.binding-sha256");

        var past = segmentation("s", OffsetUnit.OFFSET_UNIT_UTF8_BYTE, List.of(segment("a", 0, "x", 20, 25)));
        assertThat(rules(reject(past, VIEW))).containsExactly("segmentation.segment-range");
        var splitByte = segmentation("s", OffsetUnit.OFFSET_UNIT_UTF8_BYTE, List.of(segment("a", 0, "wÃ", 7, 9)));
        assertThat(rules(reject(splitByte, VIEW))).containsExactly("segmentation.segment-boundary");
        var splitPair = segmentation("s", OffsetUnit.OFFSET_UNIT_UTF16_CODE_UNIT, List.of(segment("a", 0, "?", 14, 15)));
        assertThat(rules(reject(splitPair, VIEW))).containsExactly("segmentation.segment-boundary");
        var wrongText = segmentation("s", OffsetUnit.OFFSET_UNIT_CODE_POINT, List.of(segment("a", 0, "Hello!", 0, 6)));
        assertThat(rules(reject(wrongText, VIEW))).containsExactly("segmentation.segment-text");
        var unspecified = base.toBuilder().setOffsetUnit(OffsetUnit.OFFSET_UNIT_UNSPECIFIED).build();
        assertThat(rules(reject(unspecified, VIEW))).containsExactly("segmentation.offset-unit");
    }

    @Test void fieldPathsMustResolveToAStringOutsideTheSemanticParts() {
        var missing = withPath(segmentation("s", OffsetUnit.OFFSET_UNIT_UTF8_BYTE, List.of()), "search_metadata.body");
        var noBody = Document.newBuilder().setDocId("d").setSearchMetadata(SearchMetadata.newBuilder().setTitle("t")).build();
        assertThat(reject(missing, noBody).violations()).singleElement().satisfies(v -> {
            assertThat(v.ruleId()).isEqualTo("segmentation.source-resolves");
            assertThat(v.message()).contains("not set");
        });
        var semantic = withPath(missing, "search_metadata.segmentations");
        assertThat(reject(semantic, VIEW).violations().getFirst().message()).contains("semantic part");
        var notString = withPath(missing, "search_metadata");
        assertThat(reject(notString, VIEW).violations().getFirst().message()).contains("not a string");
        var unknown = withPath(missing, "search_metadata.no_such_field");
        assertThat(reject(unknown, VIEW).violations().getFirst().message()).contains("does not resolve");
        assertThat(DocumentSemanticAdmission.sourcePart("search_metadata.body")).isEqualTo(DocumentPart.DOCUMENT_PART_CORE);
        assertThat(DocumentSemanticAdmission.sourcePart("parser_results")).isEqualTo(DocumentPart.DOCUMENT_PART_PARSED);
        assertThat(DocumentSemanticAdmission.sourcePart("blob_bag.blob.filename")).isEqualTo(DocumentPart.DOCUMENT_PART_BLOBS);
    }

    @Test void callerDigestMustMatchAndUnverifiedCarriedContentStillGetsADigest() {
        var s = segmentation("s", OffsetUnit.OFFSET_UNIT_UTF8_BYTE, List.of(segment("a", 0, "Hello,", 0, 6)));
        var digest = SegmentationDigests.digest(s);
        var claimed = s.toBuilder().setContentDigest(digest).build();
        assertThat(DocumentSemanticAdmission.check(new Candidate(List.of(present(claimed)), Map.of(), List.of(), Map.of(), VIEW)))
                .isInstanceOf(Accepted.class);
        var wrong = s.toBuilder().setContentDigest("1".repeat(64)).build();
        assertThat(rules(reject(wrong, VIEW))).containsExactly("segmentation.content-digest");
        // A carried segmentation read only for its segment IDs is not checked against the view.
        var unverified = new PresentSegmentation(s, false);
        var noBody = Document.getDefaultInstance();
        assertThat(((Accepted) DocumentSemanticAdmission.check(new Candidate(List.of(unverified), Map.of(), List.of(), Map.of(), noBody)))
                .digests()).containsEntry("s", digest);
    }

    @Test void embeddingSetsBindToTheSegmentationDigestAndCoverEverySegmentOnce() {
        var s = segmentation("s", OffsetUnit.OFFSET_UNIT_UTF8_BYTE, List.of(segment("a", 0, "Hello,", 0, 6), segment("b", 1, "wörld!", 7, 14)));
        var digest = SegmentationDigests.digest(s);
        var set = set("s", "m", digest, "a", "b");
        assertThat(DocumentSemanticAdmission.check(new Candidate(List.of(present(s)), Map.of(), List.of(set), Map.of(), VIEW)))
                .isInstanceOf(Accepted.class);

        var stale = (Rejected) DocumentSemanticAdmission.check(new Candidate(List.of(present(s)), Map.of(),
                List.of(set.toBuilder().setSegmentationDigest("2".repeat(64)).build()), Map.of(), VIEW));
        assertThat(stale.kind()).isEqualTo(Kind.SEGMENTATION_MISMATCH);
        assertThat(rules(stale)).containsExactly("embedding-set.segmentation-digest");

        var absent = (Rejected) DocumentSemanticAdmission.check(new Candidate(List.of(present(s)), Map.of(),
                List.of(set.toBuilder().setSegmentationId("other").build()), Map.of(), VIEW));
        assertThat(rules(absent)).containsExactly("embedding-set.segmentation-present");

        var missing = (Rejected) DocumentSemanticAdmission.check(new Candidate(List.of(present(s)), Map.of(),
                List.of(set("s", "m", digest, "a")), Map.of(), VIEW));
        assertThat(missing.violations()).singleElement().satisfies(v -> {
            assertThat(v.ruleId()).isEqualTo("embedding-set.vector-coverage");
            assertThat(v.message()).contains("[b]");
        });
        var doubled = (Rejected) DocumentSemanticAdmission.check(new Candidate(List.of(present(s)), Map.of(),
                List.of(set("s", "m", digest, "a", "a", "b")), Map.of(), VIEW));
        assertThat(doubled.violations()).singleElement().satisfies(v -> assertThat(v.message()).contains("two vectors"));
        var foreign = (Rejected) DocumentSemanticAdmission.check(new Candidate(List.of(present(s)), Map.of(),
                List.of(set("s", "m", digest, "a", "b", "zz")), Map.of(), VIEW));
        assertThat(foreign.violations()).singleElement().satisfies(v -> assertThat(v.message()).contains("zz"));

        // Carried sets compare digests only; carried segmentations need no content.
        var key = EmbeddingSetKey.newBuilder().setSegmentationId("c").setEmbeddingSetId("m").build();
        assertThat(DocumentSemanticAdmission.check(new Candidate(List.of(), Map.of("c", "3".repeat(64)), List.of(),
                Map.of(key, "3".repeat(64)), VIEW))).isInstanceOf(Accepted.class);
        var carriedStale = (Rejected) DocumentSemanticAdmission.check(new Candidate(List.of(present(s)), Map.of(), List.of(),
                Map.of(EmbeddingSetKey.newBuilder().setSegmentationId("s").setEmbeddingSetId("m").build(), "3".repeat(64)), VIEW));
        assertThat(rules(carriedStale)).containsExactly("embedding-set.segmentation-digest");
        // A written set bound to a segmentation that is not present at all.
        var orphan = (Rejected) DocumentSemanticAdmission.check(new Candidate(List.of(), Map.of(), List.of(set), Map.of(), VIEW));
        assertThat(rules(orphan)).containsExactly("embedding-set.segmentation-present");
    }

    @Test void segmentationContentFaultsWinOverBindingFaults() {
        var bad = segmentation("s", OffsetUnit.OFFSET_UNIT_UTF8_BYTE, List.of(segment("a", 0, "nope", 0, 6)));
        var rejected = (Rejected) DocumentSemanticAdmission.check(new Candidate(List.of(present(bad)), Map.of(),
                List.of(set("s", "m", "4".repeat(64), "a")), Map.of(), VIEW));
        assertThat(rejected.kind()).isEqualTo(Kind.VALIDATION_FAILED);
        assertThat(rules(rejected)).containsExactly("segmentation.segment-text");
        assertThatThrownBy(() -> DocumentSemanticAdmission.check(new Candidate(List.of(present(bad)), Map.of("s", "x"),
                List.of(), Map.of(), VIEW))).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("twice");
    }

    // ------------------------------------------------------------------ fixtures

    static PresentSegmentation present(Segmentation segmentation) { return new PresentSegmentation(segmentation, true); }

    static Rejected reject(Segmentation segmentation, Document view) {
        var result = DocumentSemanticAdmission.check(new Candidate(List.of(present(segmentation)), Map.of(), List.of(), Map.of(), view));
        assertThat(result).as("%s", result).isInstanceOf(Rejected.class);
        return (Rejected) result;
    }

    static List<String> rules(Rejected rejected) { return rejected.violations().stream().map(Violation::ruleId).toList(); }

    static Segmentation segmentation(String id, OffsetUnit unit, List<Segment> segments) {
        var utf8 = TEXT.getBytes(StandardCharsets.UTF_8);
        return Segmentation.newBuilder().setSegmentationId(id).setKind(SegmentationKind.SEGMENTATION_KIND_SENTENCE)
                .setSource(TextSource.newBuilder().setFieldPath("search_metadata.body").setBinding(SourceTextBinding.newBuilder()
                        .setResolvedTextSha256(DocumentSemanticAdmission.sha256Hex(utf8)).setUtf8Length(utf8.length)))
                .setOffsetUnit(unit).addAllSegments(segments).build();
    }

    static Segmentation derived(String id, String text, List<Segment> segments) {
        var utf8 = text.getBytes(StandardCharsets.UTF_8);
        return Segmentation.newBuilder().setSegmentationId(id).setKind(SegmentationKind.SEGMENTATION_KIND_SEMANTIC_BOUNDARY)
                .setSource(TextSource.newBuilder().setDerived(DerivedText.newBuilder().setDerivationId("norm").setText(text))
                        .setBinding(SourceTextBinding.newBuilder()
                        .setResolvedTextSha256(DocumentSemanticAdmission.sha256Hex(utf8)).setUtf8Length(utf8.length)))
                .setOffsetUnit(OffsetUnit.OFFSET_UNIT_UTF8_BYTE).addAllSegments(segments).build();
    }

    static Segmentation withPath(Segmentation segmentation, String path) {
        var builder = segmentation.toBuilder();
        builder.getSourceBuilder().setFieldPath(path);
        return builder.build();
    }

    static Segment segment(String id, int ordinal, String text, long start, long end) {
        return Segment.newBuilder().setSegmentId(id).setOrdinal(ordinal).setText(text).setStart(start).setEnd(end).build();
    }

    static EmbeddingSet set(String segmentation, String id, String digest, String... segmentIds) {
        var set = EmbeddingSet.newBuilder().setEmbeddingSetId(id).setSegmentationId(segmentation).setSegmentationDigest(digest)
                .setModel("m").setDimensions(1).setSimilarity(Similarity.SIMILARITY_COSINE);
        for (var segmentId : segmentIds) set.addVectors(SegmentVector.newBuilder().setSegmentId(segmentId).addValues(1f));
        return set.build();
    }
}
