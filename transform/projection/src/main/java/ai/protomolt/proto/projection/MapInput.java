package ai.protomolt.proto.projection;

import com.google.protobuf.Struct;

import java.util.List;

/**
 * Typed carrier distinguishing protobuf {@code map<K, V>} entries and {@code struct_value} map
 * literals from ordinary {@link List} or {@link Struct} message values.
 */
sealed interface MapInput {

    record RawEntry(Object key, Object value) {
    }

    record ProtoEntries(List<RawEntry> entries) implements MapInput {
    }

    record StructLiteral(Struct struct) implements MapInput {
    }
}
