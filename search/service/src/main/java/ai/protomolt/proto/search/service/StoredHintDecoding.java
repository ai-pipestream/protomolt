package ai.protomolt.proto.search.service;

import ai.protomolt.proto.search.index.spi.DateResolution;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import ai.protomolt.proto.search.v1.StoredValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;

/**
 * Package-private stored-hint decoding for {@link LuceneSearchStore}.
 *
 * <p>Separates typing stored Lucene fields back into stored-value cells
 * (driven by the mapping's declared field hints) from the store's
 * writer/searcher acquisition, snapshot refresh, and close lifecycle.
 * Decoding is stateless: the mapping's hints arrive as parameters.
 */
final class StoredHintDecoding {

    private StoredHintDecoding() {}

    /** The mapping's field hints by index field name, for typing stored values. */
    static Map<String, ResolvedFieldHint> storedHints(IndexMapping mapping) {
        Map<String, ResolvedFieldHint> hints = new LinkedHashMap<>();
        for (IndexMapping.IndexedField field : mapping.indexable()) {
            hints.put(field.fieldName(), field.hint());
        }
        return hints;
    }

    /**
     * One stored Lucene field as a typed cell. The mapping's declared kind
     * decides the arm; a field the mapping does not declare (chunk text,
     * chunk identity) or whose stored form disagrees with its kind falls
     * back to the runtime form. Returns {@code null} only when the field
     * carries nothing storable.
     */
    static StoredValue storedValue(ResolvedFieldHint hint, IndexableField field) {
        Number number = field.numericValue();
        String string = field.stringValue();
        BytesRef binary = field.binaryValue();
        if (hint != null) {
            switch (hint.type()) {
                case DATE -> {
                    if (number != null) {
                        long epoch = number.longValue();
                        long millis = hint.dateResolution() == DateResolution.SECONDS
                                ? epoch * 1000L
                                : epoch;
                        return StoredValue.newBuilder()
                                .setTimestampValue(Timestamp.newBuilder()
                                        .setSeconds(Math.floorDiv(millis, 1000L))
                                        .setNanos((int) Math.floorMod(millis, 1000L) * 1_000_000))
                                .build();
                    }
                }
                case INT32, INT64 -> {
                    if (number != null) {
                        return StoredValue.newBuilder()
                                .setInt64Value(number.longValue()).build();
                    }
                }
                case FLOAT, DOUBLE -> {
                    if (number != null) {
                        return StoredValue.newBuilder()
                                .setDoubleValue(number.doubleValue()).build();
                    }
                }
                case BOOLEAN -> {
                    if (string != null) {
                        return StoredValue.newBuilder()
                                .setBoolValue(Boolean.parseBoolean(string)).build();
                    }
                }
                case BINARY -> {
                    if (binary != null) {
                        return StoredValue.newBuilder()
                                .setBytesValue(ByteString.copyFrom(
                                        binary.bytes, binary.offset, binary.length))
                                .build();
                    }
                }
                default -> {
                    // TEXT, KEYWORD, and JSON-folded fields land below.
                }
            }
        }
        if (string != null) {
            return StoredValue.newBuilder().setStringValue(string).build();
        }
        if (number instanceof Float || number instanceof Double) {
            return StoredValue.newBuilder().setDoubleValue(number.doubleValue()).build();
        }
        if (number != null) {
            return StoredValue.newBuilder().setInt64Value(number.longValue()).build();
        }
        if (binary != null) {
            return StoredValue.newBuilder()
                    .setBytesValue(ByteString.copyFrom(binary.bytes, binary.offset, binary.length))
                    .build();
        }
        return null;
    }
}
