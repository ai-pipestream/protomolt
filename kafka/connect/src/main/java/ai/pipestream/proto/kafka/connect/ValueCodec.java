package ai.pipestream.proto.kafka.connect;

import ai.pipestream.proto.kafka.wire.ConfluentWireFormat;
import ai.pipestream.proto.kafka.wire.ConfluentWireFormatException;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.DataException;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * Decodes record values into {@link DynamicMessage}s of a configured type and encodes
 * messages back — the value-format contract the transforms share (and the sink mirrors):
 * raw {@code protobuf} bytes, {@code confluent} wire format, or proto3 {@code json} text.
 *
 * <p>Confluent-framed values re-encode under their original frame prefix: the transforms
 * keep the message type, so the schema id the frame carries stays true. JSON values
 * re-encode as the same Java type they arrived as ({@code byte[]} or {@code String}), so
 * the worker's converter round-trips.</p>
 */
final class ValueCodec {

    static final String DESCRIPTOR_SET = "schema.descriptor.set.base64";
    static final String MESSAGE_TYPE = "message.type";
    static final String VALUE_FORMAT = "value.format";

    enum Format { PROTOBUF, CONFLUENT, JSON }

    private final Descriptor type;
    private final Format format;

    ValueCodec(Descriptor type, String format) {
        this.type = type;
        this.format = Format.valueOf(format.toUpperCase(Locale.ROOT));
    }

    /** The config keys every descriptor-driven transform shares. */
    static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(DESCRIPTOR_SET, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Base64-encoded serialized google.protobuf.FileDescriptorSet declaring "
                                + "the message type (e.g. from ProtoMolt's compile or reflect "
                                + "verbs, or a registry descriptor-set endpoint).")
                .define(MESSAGE_TYPE, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Fully qualified message type of the record values, "
                                + "e.g. 'shop.v1.Order'.")
                .define(VALUE_FORMAT, ConfigDef.Type.STRING, "protobuf",
                        ConfigDef.CaseInsensitiveValidString.in("protobuf", "confluent", "json"),
                        ConfigDef.Importance.MEDIUM,
                        "How record values decode: raw 'protobuf' bytes, 'confluent' wire "
                                + "format (framed with a schema id), or proto3 'json' text.");
    }

    static ValueCodec fromConfig(AbstractConfig config) {
        Descriptor type = GrpcConnectorSupport.messageType(
                GrpcConnectorSupport.linkedFiles(config.getString(DESCRIPTOR_SET)),
                config.getString(MESSAGE_TYPE));
        return new ValueCodec(type, config.getString(VALUE_FORMAT));
    }

    Descriptor type() {
        return type;
    }

    /** Decodes a record value; {@code where} contextualizes failures (e.g. "topic orders"). */
    DynamicMessage decode(Object value, String where) {
        try {
            switch (format) {
                case PROTOBUF -> {
                    return DynamicMessage.parseFrom(type, asBytes(value));
                }
                case CONFLUENT -> {
                    return DynamicMessage.parseFrom(type,
                            ConfluentWireFormat.payload(asBytes(value)));
                }
                default -> {
                    String json = value instanceof byte[] bytes
                            ? new String(bytes, StandardCharsets.UTF_8)
                            : value.toString();
                    DynamicMessage.Builder builder = DynamicMessage.newBuilder(type);
                    JsonFormat.parser().ignoringUnknownFields().merge(json, builder);
                    return builder.build();
                }
            }
        } catch (DataException e) {
            throw e;
        } catch (Exception e) {
            throw new DataException("Record value does not decode as " + type.getFullName()
                    + " (" + format.name().toLowerCase(Locale.ROOT) + ", " + where + "): "
                    + e.getMessage(), e);
        }
    }

    /** Encodes a message back into the shape the record value arrived in. */
    Object encode(Message message, Object originalValue) {
        switch (format) {
            case PROTOBUF -> {
                return message.toByteArray();
            }
            case CONFLUENT -> {
                byte[] original = asBytes(originalValue);
                int offset;
                // The reader is framework-neutral; re-wrap so the transform still fails a
                // malformed frame as a DataException the worker can route.
                try {
                    offset = ConfluentWireFormat.payloadOffset(original);
                } catch (ConfluentWireFormatException e) {
                    throw new DataException("Record value is not a Confluent frame, so its "
                            + "prefix cannot be reused: " + e.getMessage(), e);
                }
                byte[] payload = message.toByteArray();
                byte[] framed = new byte[offset + payload.length];
                System.arraycopy(original, 0, framed, 0, offset);
                System.arraycopy(payload, 0, framed, offset, payload.length);
                return framed;
            }
            default -> {
                String json;
                try {
                    json = JsonFormat.printer().print(message);
                } catch (Exception e) {
                    throw new DataException("Mapped message does not print as proto3 JSON: "
                            + e.getMessage(), e);
                }
                return originalValue instanceof byte[]
                        ? json.getBytes(StandardCharsets.UTF_8)
                        : json;
            }
        }
    }

    private static byte[] asBytes(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        throw new DataException("Record value must be byte[] for this format; got "
                + value.getClass().getName()
                + " (use the ByteArrayConverter for value.converter)");
    }
}
