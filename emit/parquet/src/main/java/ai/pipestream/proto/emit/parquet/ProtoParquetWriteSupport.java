package ai.pipestream.proto.emit.parquet;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.Map;
import java.util.Objects;

/**
 * Streams protobuf messages (dynamic or generated — only the descriptor matters) into a
 * Parquet {@link RecordConsumer}, mirroring {@link ProtoParquetSchemas}' shape exactly:
 * required scalars always write (proto3 defaults are values), optional scalars and message
 * fields write only when present, repeated fields and map entries write per element.
 */
final class ProtoParquetWriteSupport extends WriteSupport<Message> {

    private final Descriptor descriptor;
    private final MessageType schema;
    private RecordConsumer consumer;

    ProtoParquetWriteSupport(Descriptor descriptor) {
        this.descriptor = Objects.requireNonNull(descriptor, "descriptor");
        this.schema = ProtoParquetSchemas.schema(descriptor);
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
        writeFields(record);
        consumer.endMessage();
    }

    private void writeFields(Message message) {
        int index = 0;
        for (FieldDescriptor field : message.getDescriptorForType().getFields()) {
            writeField(message, field, index++);
        }
    }

    private void writeField(Message message, FieldDescriptor field, int index) {
        if (field.isMapField()) {
            int count = message.getRepeatedFieldCount(field);
            if (count == 0) {
                return;
            }
            consumer.startField(field.getName(), index);
            FieldDescriptor key = field.getMessageType().findFieldByName("key");
            FieldDescriptor value = field.getMessageType().findFieldByName("value");
            for (int i = 0; i < count; i++) {
                Message entry = (Message) message.getRepeatedField(field, i);
                consumer.startGroup();
                consumer.startField("key", 0);
                writePrimitive(key, entry.getField(key));
                consumer.endField("key", 0);
                if (value.getJavaType() != FieldDescriptor.JavaType.MESSAGE
                        || entry.hasField(value)) {
                    consumer.startField("value", 1);
                    writeValue(value, entry.getField(value));
                    consumer.endField("value", 1);
                }
                consumer.endGroup();
            }
            consumer.endField(field.getName(), index);
            return;
        }
        if (field.isRepeated()) {
            int count = message.getRepeatedFieldCount(field);
            if (count == 0) {
                return;
            }
            consumer.startField(field.getName(), index);
            for (int i = 0; i < count; i++) {
                writeValue(field, message.getRepeatedField(field, i));
            }
            consumer.endField(field.getName(), index);
            return;
        }
        boolean tracksPresence = field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                || field.toProto().getProto3Optional();
        if (tracksPresence && !message.hasField(field)) {
            return;
        }
        consumer.startField(field.getName(), index);
        writeValue(field, message.getField(field));
        consumer.endField(field.getName(), index);
    }

    private void writeValue(FieldDescriptor field, Object value) {
        if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
            consumer.startGroup();
            writeFields((Message) value);
            consumer.endGroup();
            return;
        }
        writePrimitive(field, value);
    }

    private void writePrimitive(FieldDescriptor field, Object value) {
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
