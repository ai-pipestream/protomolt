package ai.pipestream.proto.emit.parquet;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Streams protobuf messages (dynamic or generated — only the descriptor matters) into a
 * Parquet {@link RecordConsumer}, mirroring {@link ProtoParquetSchemas}' spec-compliant
 * shapes exactly: required scalars always write (proto3 defaults are values), optional
 * scalars and message fields write only when present, repeated fields write as three-level
 * lists, maps as annotated key/value groups, {@code google.protobuf.Timestamp} as
 * microsecond timestamps, and the JSON well-known types as JSON strings.
 */
final class ProtoParquetWriteSupport extends WriteSupport<Message> {

    private final Descriptor descriptor;
    private final Set<String> projection;
    private final MessageType schema;
    private RecordConsumer consumer;

    ProtoParquetWriteSupport(Descriptor descriptor) {
        this(descriptor, ProtoParquetSchemas.FieldIdResolver.NONE);
    }

    ProtoParquetWriteSupport(Descriptor descriptor,
                             ProtoParquetSchemas.FieldIdResolver ids) {
        this(descriptor, ids, Set.of());
    }

    ProtoParquetWriteSupport(Descriptor descriptor, ProtoParquetSchemas.FieldIdResolver ids,
                             Set<String> projection) {
        this.descriptor = Objects.requireNonNull(descriptor, "descriptor");
        this.projection = projection == null ? Set.of() : Set.copyOf(projection);
        this.schema = ProtoParquetSchemas.schema(descriptor, ids, this.projection);
    }

    @Override
    public WriteContext init(Configuration configuration) {
        return context();
    }

    /**
     * The Hadoop-free path: overriding this stops the default delegation from materializing
     * a {@code org.apache.hadoop.conf.Configuration}, which would demand the full Hadoop
     * client runtime just to write bytes.
     */
    @Override
    public WriteContext init(org.apache.parquet.conf.ParquetConfiguration configuration) {
        return context();
    }

    private WriteContext context() {
        return new WriteContext(schema, Map.of(
                "protomolt.proto.message", descriptor.getFullName()));
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.consumer = recordConsumer;
    }

    @Override
    public String getName() {
        return "protomolt";
    }

    @Override
    public void write(Message record) {
        consumer.startMessage();
        writeTopFields(record);
        consumer.endMessage();
    }

    /**
     * The top-level fields, honoring the projection. The Parquet index advances only for kept
     * columns, so it stays aligned with the projected schema's column order - a projected-out
     * column must not leave a gap that shifts every following column's index.
     */
    private void writeTopFields(Message message) {
        int index = 0;
        for (FieldDescriptor field : message.getDescriptorForType().getFields()) {
            if (!projection.isEmpty() && !projection.contains(field.getName())) {
                continue;
            }
            writeField(message, field, index++);
        }
    }

    private void writeFields(Message message) {
        int index = 0;
        for (FieldDescriptor field : message.getDescriptorForType().getFields()) {
            writeField(message, field, index++);
        }
    }

    private void writeField(Message message, FieldDescriptor field, int index) {
        if (field.isMapField()) {
            writeMap(message, field, index);
            return;
        }
        if (field.isRepeated()) {
            writeList(message, field, index);
            return;
        }
        // Must match ProtoParquetSchemas' repetition exactly: a field the schema declared
        // optional has to be skipped when unset, never written as its default.
        if (field.hasPresence() && !message.hasField(field)) {
            return;
        }
        consumer.startField(field.getName(), index);
        writeValue(field, message.getField(field));
        consumer.endField(field.getName(), index);
    }

    /** The spec's three-level list: name (LIST) > repeated "list" > "element". */
    private void writeList(Message message, FieldDescriptor field, int index) {
        int count = message.getRepeatedFieldCount(field);
        if (count == 0) {
            return;
        }
        consumer.startField(field.getName(), index);
        consumer.startGroup();
        consumer.startField("list", 0);
        for (int i = 0; i < count; i++) {
            consumer.startGroup();
            consumer.startField("element", 0);
            writeValue(field, message.getRepeatedField(field, i));
            consumer.endField("element", 0);
            consumer.endGroup();
        }
        consumer.endField("list", 0);
        consumer.endGroup();
        consumer.endField(field.getName(), index);
    }

    /** The spec's map: name (MAP) > repeated "key_value" > required key, value. */
    private void writeMap(Message message, FieldDescriptor field, int index) {
        int count = message.getRepeatedFieldCount(field);
        if (count == 0) {
            return;
        }
        FieldDescriptor key = field.getMessageType().findFieldByName("key");
        FieldDescriptor value = field.getMessageType().findFieldByName("value");
        consumer.startField(field.getName(), index);
        consumer.startGroup();
        consumer.startField("key_value", 0);
        for (int i = 0; i < count; i++) {
            Message entry = (Message) message.getRepeatedField(field, i);
            consumer.startGroup();
            consumer.startField("key", 0);
            writePrimitive(key, entry.getField(key));
            consumer.endField("key", 0);
            boolean plainMessageValue = value.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                    && !special(value);
            if (!plainMessageValue || entry.hasField(value)) {
                consumer.startField("value", 1);
                writeValue(value, entry.getField(value));
                consumer.endField("value", 1);
            }
            consumer.endGroup();
        }
        consumer.endField("key_value", 0);
        consumer.endGroup();
        consumer.endField(field.getName(), index);
    }

    private void writeValue(FieldDescriptor field, Object value) {
        if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE && !special(field)) {
            consumer.startGroup();
            writeFields((Message) value);
            consumer.endGroup();
            return;
        }
        writePrimitive(field, value);
    }

    private static boolean special(FieldDescriptor field) {
        String fullName = field.getMessageType().getFullName();
        return ProtoParquetSchemas.TIMESTAMP.equals(fullName)
                || ProtoParquetSchemas.JSON_TYPES.contains(fullName);
    }

    private void writePrimitive(FieldDescriptor field, Object value) {
        if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
            Message message = (Message) value;
            if (ProtoParquetSchemas.TIMESTAMP.equals(
                    field.getMessageType().getFullName())) {
                Descriptor type = message.getDescriptorForType();
                long seconds = (Long) message.getField(type.findFieldByName("seconds"));
                int nanos = (Integer) message.getField(type.findFieldByName("nanos"));
                consumer.addLong(seconds * 1_000_000L + nanos / 1_000L);
                return;
            }
            try {
                consumer.addBinary(Binary.fromString(JsonFormat.printer()
                        .omittingInsignificantWhitespace().print(message)));
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                throw new IllegalStateException("Unprintable JSON well-known type: "
                        + e.getMessage(), e);
            }
            return;
        }
        switch (field.getType()) {
            case INT32, SINT32, SFIXED32 -> consumer.addInteger((Integer) value);
            case UINT32, FIXED32 -> consumer.addLong(Integer.toUnsignedLong((Integer) value));
            case INT64, SINT64, SFIXED64, UINT64, FIXED64 -> consumer.addLong((Long) value);
            case FLOAT -> consumer.addFloat((Float) value);
            case DOUBLE -> consumer.addDouble((Double) value);
            case BOOL -> consumer.addBoolean((Boolean) value);
            case STRING -> consumer.addBinary(Binary.fromString((String) value));
            case BYTES -> consumer.addBinary(Binary.fromConstantByteArray(
                    ((ByteString) value).toByteArray()));
            case ENUM -> consumer.addBinary(Binary.fromString(
                    ((EnumValueDescriptor) value).getName()));
            case MESSAGE, GROUP -> throw new IllegalStateException(
                    "Not a primitive field: " + field.getFullName());
        }
    }
}
