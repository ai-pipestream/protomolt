package ai.protomolt.proto.repo.codec;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.repo.semantic.v1.DerivedText;
import ai.protomolt.proto.repo.semantic.v1.OffsetUnit;
import ai.protomolt.proto.repo.semantic.v1.Segment;
import ai.protomolt.proto.repo.semantic.v1.Segmentation;
import ai.protomolt.proto.repo.semantic.v1.SegmentationDigestInput;
import ai.protomolt.proto.repo.semantic.v1.SegmentationKind;
import ai.protomolt.proto.repo.semantic.v1.SourceTextBinding;
import ai.protomolt.proto.repo.semantic.v1.TextSource;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Frozen vectors for the segmentation digest encoding. #319 reuses these bytes
 * for its Go and Rust fixtures, so a change here is a change of the contract.
 */
class SegmentationDigestsTest {
    /** "Hello, wörld! 😀 end." as UTF-8: 24 bytes. */
    static final String TEXT = "Hello, wörld! 😀 end.";
    static final String TEXT_SHA256 = "22c61b3e43bdba6434c3e796ef0f61bdc6ad0df352a87384134d23a2b09c7587";

    /** Metadata keys spanning one, two, three and four UTF-8 bytes; U+E000 and U+1F600 sort differently in UTF-16. */
    static final List<String> KEYS_IN_UTF8_ORDER = List.of("z", "é", "", "😀");

    static final String GOLDEN_INPUT_HEX = "080110011a5c0a147365617263685f6d657461646174612e626f64791a440a4032326336316233653433626462613634"
            + "333463336537393665663066363162646336616430646633353261383733383431333464323361326230396337353837"
            + "101820012a370a01611a0648656c6c6f2c28069a06060a017a1201319a06070a02c3a91201329a06080a03ee80801201"
            + "339a06090a04f09f98801201342a120a016210011a0777c3b6726c64212007280e";
    static final String GOLDEN_DIGEST = "84af0e858571da10a597bf3e68e83f1839997725e5c9af104c13e08ec8184df0";

    @Test void metadataInsertionOrderDoesNotChangeTheDigest() {
        var forward = new LinkedHashMap<String, String>();
        var backward = new LinkedHashMap<String, String>();
        for (int i = 0; i < KEYS_IN_UTF8_ORDER.size(); i++) forward.put(KEYS_IN_UTF8_ORDER.get(i), Integer.toString(i + 1));
        for (int i = KEYS_IN_UTF8_ORDER.size() - 1; i >= 0; i--) backward.put(KEYS_IN_UTF8_ORDER.get(i), Integer.toString(i + 1));

        var a = golden(forward);
        var b = golden(backward);
        assertThat(SegmentationDigests.digestInput(a)).isEqualTo(SegmentationDigests.digestInput(b));
        assertThat(SegmentationDigests.digest(a)).isEqualTo(SegmentationDigests.digest(b));
        // The caller's own digest field is not part of the input.
        assertThat(SegmentationDigests.digest(a.toBuilder().setContentDigest("f".repeat(64)).build()))
                .isEqualTo(SegmentationDigests.digest(a));
    }

    @Test void goldenVectorIsFrozen() {
        var forward = new LinkedHashMap<String, String>();
        for (int i = 0; i < KEYS_IN_UTF8_ORDER.size(); i++) forward.put(KEYS_IN_UTF8_ORDER.get(i), Integer.toString(i + 1));
        var input = SegmentationDigests.digestInput(golden(forward));
        assertThat(HexFormat.of().formatHex(input.toByteArray())).isEqualTo(GOLDEN_INPUT_HEX);
        assertThat(SegmentationDigests.digest(golden(forward))).isEqualTo(GOLDEN_DIGEST);
        assertThat(SegmentationDigests.digest(golden(forward))).isEqualTo(DocumentPartCodec.sha256Hex(input.toByteArray()));
    }

    @Test void encodedInputParsesBackWithEveryFieldAndKeysInUtf8Order() throws Exception {
        var forward = new LinkedHashMap<String, String>();
        for (int i = KEYS_IN_UTF8_ORDER.size() - 1; i >= 0; i--) forward.put(KEYS_IN_UTF8_ORDER.get(i), Integer.toString(i + 1));
        var segmentation = golden(forward);
        var parsed = SegmentationDigestInput.parseFrom(SegmentationDigests.digestInput(segmentation));
        assertThat(parsed.getEncodingVersion()).isEqualTo(1);
        assertThat(parsed.getKind()).isEqualTo(segmentation.getKind());
        assertThat(parsed.getSource()).isEqualTo(segmentation.getSource());
        assertThat(parsed.getOffsetUnit()).isEqualTo(segmentation.getOffsetUnit());
        assertThat(parsed.getSegmentsList()).isEqualTo(segmentation.getSegmentsList());
        // The wire order of the map entries is the unsigned UTF-8 key order, not UTF-16 order.
        var entries = ai.protomolt.proto.repo.semantic.v1.Segment.getDescriptor().findFieldByName("metadata");
        var dynamic = com.google.protobuf.DynamicMessage.parseFrom(Segment.getDescriptor(),
                parsed.getSegments(0).toByteString());
        assertThat(((List<?>) dynamic.getField(entries)).stream()
                .map(entry -> ((com.google.protobuf.Message) entry).getField(entries.getMessageType().findFieldByNumber(1))))
                .containsExactlyElementsOf(KEYS_IN_UTF8_ORDER);
        assertThat(new java.util.ArrayList<>(KEYS_IN_UTF8_ORDER).stream().sorted().toList())
                .as("Java String order puts U+1F600 before U+E000; the digest order must not")
                .isNotEqualTo(KEYS_IN_UTF8_ORDER);
    }

    @Test void asciiOnlyInputMatchesProtobufJavaDeterministicEncoding() {
        // Where UTF-16 and UTF-8 orders agree, the hand encoding equals the generated
        // message's deterministic bytes: field order and presence rules are the standard ones.
        var segmentation = golden(Map.of("b", "2", "a", "1", "c", "3")).toBuilder()
                .setLineage(ai.protomolt.proto.repo.semantic.v1.Lineage.newBuilder().setProducerId("ignored"))
                .putMetadata("top", "ignored").build();
        var generated = SegmentationDigestInput.newBuilder().setEncodingVersion(1).setKind(segmentation.getKind())
                .setSource(segmentation.getSource()).setOffsetUnit(segmentation.getOffsetUnit())
                .addAllSegments(segmentation.getSegmentsList()).build();
        assertThat(SegmentationDigests.digestInput(segmentation).toByteArray())
                .isEqualTo(ai.protomolt.proto.descriptors.MessageFingerprints.deterministicBytes(generated));
    }

    @Test void segmentOrderIsMeaningAndDerivedTextIsPartOfTheDigest() {
        var ordered = golden(Map.of());
        var reordered = ordered.toBuilder().clearSegments()
                .addSegments(ordered.getSegments(1).toBuilder().setOrdinal(0))
                .addSegments(ordered.getSegments(0).toBuilder().setOrdinal(1)).build();
        assertThat(SegmentationDigests.digest(reordered)).isNotEqualTo(SegmentationDigests.digest(ordered));

        var derived = ordered.toBuilder().setSource(TextSource.newBuilder()
                .setDerived(DerivedText.newBuilder().setDerivationId("norm-1").setText(TEXT))
                .setBinding(ordered.getSource().getBinding())).build();
        var derivedOther = derived.toBuilder().setSource(derived.getSource().toBuilder()
                .setDerived(derived.getSource().getDerived().toBuilder().setText(TEXT + "!"))).build();
        assertThat(SegmentationDigests.digest(derived)).isNotEqualTo(SegmentationDigests.digest(ordered));
        assertThat(SegmentationDigests.digest(derivedOther)).isNotEqualTo(SegmentationDigests.digest(derived));
    }

    @Test void utf8OrderIsCodePointOrder() {
        assertThat(SegmentationDigests.sortedUtf8(List.of("😀", "", "é", "z", "")))
                .containsExactly("", "z", "é", "", "😀");
        assertThat(SegmentationDigests.compareUtf8("ab", "a")).isPositive();
        assertThat(SegmentationDigests.compareUtf8("a", "ab")).isNegative();
        assertThat(SegmentationDigests.compareUtf8("same", "same")).isZero();
    }

    static Segmentation golden(Map<String, String> metadata) {
        return Segmentation.newBuilder().setSegmentationId("sentences")
                .setKind(SegmentationKind.SEGMENTATION_KIND_SENTENCE)
                .setSource(TextSource.newBuilder().setFieldPath("search_metadata.body")
                        .setBinding(SourceTextBinding.newBuilder().setResolvedTextSha256(TEXT_SHA256).setUtf8Length(24)))
                .setOffsetUnit(OffsetUnit.OFFSET_UNIT_UTF8_BYTE)
                .addSegments(Segment.newBuilder().setSegmentId("a").setOrdinal(0).setText("Hello,").setStart(0).setEnd(6)
                        .putAllMetadata(metadata))
                .addSegments(Segment.newBuilder().setSegmentId("b").setOrdinal(1).setText("wörld!").setStart(7).setEnd(14))
                .build();
    }

    static ByteString bytes(String hex) { return ByteString.copyFrom(HexFormat.of().parseHex(hex)); }
}
