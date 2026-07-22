package ai.pipestream.proto.lake.iceberg;

import ai.pipestream.proto.meta.DescriptorMetadata;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Partitioning for a descriptor-backed Iceberg table: turning declared partition columns into a
 * {@link PartitionSpec}, and reading a proto message's source-column values so the sink can route
 * each row to its partition.
 *
 * <p>The spec is built <b>by column name</b>, never by a raw field id. A REST catalog assigns
 * fresh field ids when it creates the table, so a spec bound to pre-creation ids would point at
 * the wrong columns; binding by name lets Iceberg remap to the ids it actually stored. Supported
 * transforms are {@code identity}, {@code year}/{@code month}/{@code day}/{@code hour} (on a
 * timestamp source), {@code bucket[N]}, and {@code truncate[W]}.</p>
 */
public final class IcebergPartitions {

    private IcebergPartitions() {
    }

    /** One partition column: a top-level source column and the transform applied to it. */
    public record PartitionField(String sourceColumn, String transform) {
    }

    /** The label a descriptor field carries to declare itself a partition source. */
    public static final String PARTITION_HINT = "iceberg.partition";

    /**
     * Builds a {@link PartitionSpec} over {@code schema} from {@code fields}, resolving each
     * source by name. Iceberg validates that a transform fits its source type (e.g. {@code day}
     * needs a timestamp), so an ill-typed partition fails here rather than at write time.
     */
    public static PartitionSpec bind(Schema schema, List<PartitionField> fields) {
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        for (PartitionField field : fields) {
            String transform = field.transform().trim().toLowerCase(Locale.ROOT);
            String column = field.sourceColumn();
            switch (transform) {
                case "identity" -> builder.identity(column);
                case "year" -> builder.year(column);
                case "month" -> builder.month(column);
                case "day" -> builder.day(column);
                case "hour" -> builder.hour(column);
                default -> {
                    if (transform.startsWith("bucket")) {
                        builder.bucket(column, width(transform, "bucket"));
                    } else if (transform.startsWith("truncate")) {
                        builder.truncate(column, width(transform, "truncate"));
                    } else {
                        throw new IllegalArgumentException("Unsupported partition transform '"
                                + field.transform() + "' on column '" + column + "'");
                    }
                }
            }
        }
        return builder.build();
    }

    /**
     * The partition columns a descriptor declares through the {@value #PARTITION_HINT} field
     * label (value = the transform), in field order. Empty when none is declared, so a caller
     * can hand the result straight to {@link #bind} for an unpartitioned or partitioned table
     * alike. Requires the descriptor's options to have been parsed with the metadata extensions.
     */
    public static List<PartitionField> fromHints(Descriptor descriptor) {
        List<PartitionField> fields = new ArrayList<>();
        for (FieldDescriptor field : descriptor.getFields()) {
            DescriptorMetadata.field(field)
                    .map(meta -> meta.getLabelsMap().get(PARTITION_HINT))
                    .filter(value -> value != null && !value.isBlank())
                    .ifPresent(value -> fields.add(new PartitionField(field.getName(), value)));
        }
        return fields;
    }

    private static int width(String transform, String name) {
        int open = transform.indexOf('[');
        int close = transform.indexOf(']');
        if (open < 0 || close <= open) {
            throw new IllegalArgumentException("The '" + name + "' transform needs a width in "
                    + "brackets, e.g. " + name + "[16]: got '" + transform + "'");
        }
        try {
            return Integer.parseInt(transform.substring(open + 1, close).trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid '" + name + "' width in '" + transform
                    + "'");
        }
    }

    /**
     * A {@link StructLike} view of {@code message} against {@code schema}, returning each
     * top-level column's value in Iceberg's internal representation so a {@code PartitionKey}
     * can apply the spec's transforms. Only the columns a partition spec reads are ever asked
     * for; unset message fields read as null.
     */
    static StructLike row(Message message, Schema schema) {
        return new ProtoPartitionRow(message, schema);
    }

    private record ProtoPartitionRow(Message message, Schema schema) implements StructLike {

        @Override
        public int size() {
            return schema.columns().size();
        }

        @Override
        public <T> T get(int pos, Class<T> javaClass) {
            Types.NestedField column = schema.columns().get(pos);
            FieldDescriptor field = message.getDescriptorForType().findFieldByName(column.name());
            if (field == null) {
                return null;
            }
            if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE && !message.hasField(field)) {
                return null;
            }
            return javaClass.cast(internal(column.type(), field, message.getField(field)));
        }

        @Override
        public <T> void set(int pos, T value) {
            throw new UnsupportedOperationException("partition source row is read-only");
        }

        private static Object internal(Type type, FieldDescriptor field, Object raw) {
            return switch (type.typeId()) {
                case BOOLEAN -> (Boolean) raw;
                case INTEGER -> ((Number) raw).intValue();
                case LONG -> longValue(field, raw);
                case FLOAT -> ((Number) raw).floatValue();
                case DOUBLE -> ((Number) raw).doubleValue();
                case STRING -> field.getJavaType() == FieldDescriptor.JavaType.ENUM
                        ? ((EnumValueDescriptor) raw).getName()
                        : raw.toString();
                case BINARY -> ByteBuffer.wrap(((ByteString) raw).toByteArray());
                case TIMESTAMP -> timestampMicros((Message) raw);
                default -> throw new IllegalArgumentException("Column '" + field.getName()
                        + "' has type " + type + ", which cannot be a partition source");
            };
        }

        /**
         * uint32 and fixed32 widen to a long column and are written unsigned, so the partition
         * value has to widen the same way. Sign-extending here would route a row whose column
         * reads 4294967295 into the partition for -1.
         */
        private static long longValue(FieldDescriptor field, Object raw) {
            return switch (field.getType()) {
                case UINT32, FIXED32 -> Integer.toUnsignedLong((Integer) raw);
                default -> ((Number) raw).longValue();
            };
        }

        private static long timestampMicros(Message timestamp) {
            Descriptor type = timestamp.getDescriptorForType();
            long seconds = (long) timestamp.getField(type.findFieldByName("seconds"));
            int nanos = (int) timestamp.getField(type.findFieldByName("nanos"));
            return seconds * 1_000_000L + nanos / 1_000L;
        }
    }
}
