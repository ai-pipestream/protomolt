package ai.protomolt.proto.repo.codec;

import ai.protomolt.proto.repo.semantic.v1.DerivedText;
import ai.protomolt.proto.repo.semantic.v1.Segment;
import ai.protomolt.proto.repo.semantic.v1.Segmentation;
import ai.protomolt.proto.repo.semantic.v1.SegmentationDigestInput;
import ai.protomolt.proto.repo.semantic.v1.SourceTextBinding;
import ai.protomolt.proto.repo.semantic.v1.TextSource;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The canonical encoding of {@link SegmentationDigestInput} and the
 * {@code Segmentation.content_digest} computed from it.
 * <p>
 * The digest is the lowercase hex SHA-256 of these bytes, written here field by
 * field so that no language's serializer decides the result:
 * <ul>
 *   <li>fields in field-number order, each nested message length-delimited;</li>
 *   <li>proto3 presence: a scalar at its default value is not written, a set
 *       {@code oneof} member and a set message are written whatever their
 *       contents, and a map entry always writes both key and value;</li>
 *   <li>{@code encoding_version} 1, then {@code kind}, {@code source} with its
 *       {@code binding}, {@code offset_unit} and the segments in their stored
 *       order, each with {@code segment_id}, {@code ordinal}, {@code text},
 *       {@code start}, {@code end} and {@code metadata};</li>
 *   <li>{@code Segment.metadata} entries sorted by the unsigned UTF-8 bytes of
 *       their keys. Deterministic serializers disagree here: protobuf-java
 *       sorts string keys in UTF-16 code unit order, C++ and Go by bytes, so
 *       the order is fixed by this class and not left to a serializer;</li>
 *   <li>unknown fields, {@code Lineage} and the top-level
 *       {@code Segmentation.metadata} are excluded.</li>
 * </ul>
 * Segments are never reordered: the stored order is the segmentation's meaning,
 * and a different order gives a different digest.
 */
public final class SegmentationDigests {
    /** The only defined encoding version. */
    public static final int ENCODING_VERSION = 1;

    private SegmentationDigests() {}

    /**
     * The segmentation's content digest.
     *
     * @param segmentation the segmentation, whose own {@code content_digest} is ignored
     * @return lowercase hex SHA-256 of {@link #digestInput(Segmentation)}
     */
    public static String digest(Segmentation segmentation) {
        return DocumentPartCodec.sha256Hex(digestInput(segmentation).toByteArray());
    }

    /**
     * The canonical bytes of the segmentation's {@link SegmentationDigestInput}.
     *
     * @param segmentation the segmentation
     * @return the encoded digest input
     */
    public static ByteString digestInput(Segmentation segmentation) {
        Objects.requireNonNull(segmentation, "segmentation");
        var out = new ByteArrayOutputStream();
        var coded = CodedOutputStream.newInstance(out);
        try {
            coded.writeUInt32(SegmentationDigestInput.ENCODING_VERSION_FIELD_NUMBER, ENCODING_VERSION);
            if (segmentation.getKindValue() != 0)
                coded.writeEnum(SegmentationDigestInput.KIND_FIELD_NUMBER, segmentation.getKindValue());
            if (segmentation.hasSource())
                coded.writeByteArray(SegmentationDigestInput.SOURCE_FIELD_NUMBER, source(segmentation.getSource()));
            if (segmentation.getOffsetUnitValue() != 0)
                coded.writeEnum(SegmentationDigestInput.OFFSET_UNIT_FIELD_NUMBER, segmentation.getOffsetUnitValue());
            for (var segment : segmentation.getSegmentsList())
                coded.writeByteArray(SegmentationDigestInput.SEGMENTS_FIELD_NUMBER, segment(segment));
            coded.flush();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory encoding failed", impossible);
        }
        return ByteString.copyFrom(out.toByteArray());
    }

    private static byte[] source(TextSource source) throws IOException {
        var out = new ByteArrayOutputStream();
        var coded = CodedOutputStream.newInstance(out);
        if (source.hasFieldPath()) coded.writeString(TextSource.FIELD_PATH_FIELD_NUMBER, source.getFieldPath());
        if (source.hasDerived()) coded.writeByteArray(TextSource.DERIVED_FIELD_NUMBER, derived(source.getDerived()));
        if (source.hasBinding()) coded.writeByteArray(TextSource.BINDING_FIELD_NUMBER, binding(source.getBinding()));
        coded.flush();
        return out.toByteArray();
    }

    private static byte[] derived(DerivedText derived) throws IOException {
        var out = new ByteArrayOutputStream();
        var coded = CodedOutputStream.newInstance(out);
        if (!derived.getDerivationId().isEmpty())
            coded.writeString(DerivedText.DERIVATION_ID_FIELD_NUMBER, derived.getDerivationId());
        if (!derived.getText().isEmpty()) coded.writeString(DerivedText.TEXT_FIELD_NUMBER, derived.getText());
        coded.flush();
        return out.toByteArray();
    }

    private static byte[] binding(SourceTextBinding binding) throws IOException {
        var out = new ByteArrayOutputStream();
        var coded = CodedOutputStream.newInstance(out);
        if (!binding.getResolvedTextSha256().isEmpty())
            coded.writeString(SourceTextBinding.RESOLVED_TEXT_SHA256_FIELD_NUMBER, binding.getResolvedTextSha256());
        if (binding.getUtf8Length() != 0) coded.writeInt64(SourceTextBinding.UTF8_LENGTH_FIELD_NUMBER, binding.getUtf8Length());
        coded.flush();
        return out.toByteArray();
    }

    private static byte[] segment(Segment segment) throws IOException {
        var out = new ByteArrayOutputStream();
        var coded = CodedOutputStream.newInstance(out);
        if (!segment.getSegmentId().isEmpty()) coded.writeString(Segment.SEGMENT_ID_FIELD_NUMBER, segment.getSegmentId());
        if (segment.getOrdinal() != 0) coded.writeInt32(Segment.ORDINAL_FIELD_NUMBER, segment.getOrdinal());
        if (!segment.getText().isEmpty()) coded.writeString(Segment.TEXT_FIELD_NUMBER, segment.getText());
        if (segment.getStart() != 0) coded.writeInt64(Segment.START_FIELD_NUMBER, segment.getStart());
        if (segment.getEnd() != 0) coded.writeInt64(Segment.END_FIELD_NUMBER, segment.getEnd());
        for (var key : sortedUtf8(segment.getMetadataMap().keySet())) {
            var entry = new ByteArrayOutputStream();
            var entryCoded = CodedOutputStream.newInstance(entry);
            entryCoded.writeString(1, key);
            entryCoded.writeString(2, segment.getMetadataMap().get(key));
            entryCoded.flush();
            coded.writeByteArray(Segment.METADATA_FIELD_NUMBER, entry.toByteArray());
        }
        coded.flush();
        return out.toByteArray();
    }

    /**
     * Unsigned UTF-8 byte order, which equals Unicode code point order.
     *
     * @param left one key
     * @param right another key
     * @return negative, zero or positive as {@code left} sorts before, with or after {@code right}
     */
    public static int compareUtf8(String left, String right) {
        int i = 0, j = 0;
        while (i < left.length() && j < right.length()) {
            int x = left.codePointAt(i), y = right.codePointAt(j);
            if (x != y) return Integer.compare(x, y);
            i += Character.charCount(x);
            j += Character.charCount(y);
        }
        return Integer.compare(left.length() - i, right.length() - j);
    }

    /**
     * The keys in unsigned UTF-8 order.
     *
     * @param keys the keys of a map
     * @return a new sorted list
     */
    public static List<String> sortedUtf8(java.util.Collection<String> keys) {
        var sorted = new ArrayList<>(keys);
        sorted.sort(SegmentationDigests::compareUtf8);
        return sorted;
    }

}
