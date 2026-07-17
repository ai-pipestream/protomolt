package ai.pipestream.proto.kafka.serde;

import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A Kafka protobuf serializer that writes the Confluent wire format and enforces the schema's own
 * rules on the way out.
 *
 * <p>The rules are the ones already declared on the descriptor, so a producer cannot forget to
 * call a validator: validation is not the application's code path any more, it is the
 * serializer's, and a message that violates its contract is rejected instead of written. Because
 * the schema comes from a descriptor set the deployment packages rather than from a registry
 * lookup, there is no per-message network hop and no registry to be down.</p>
 *
 * @see ProtoMoltSerdeConfig
 */
public class ProtoMoltProtobufSerializer implements Serializer<Message> {

    private ProtoValidator validator;
    private Descriptor messageType;
    private List<Integer> indexPath;
    private int schemaId;
    private boolean validateOnWrite;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        ProtoMoltSerdeConfig config = new ProtoMoltSerdeConfig(configs);
        List<FileDescriptor> files = Descriptors.load(config, getClass().getClassLoader());
        messageType = SerdeDescriptors.messageType(files, config.getString(
                ProtoMoltSerdeConfig.MESSAGE_TYPE));
        indexPath = ConfluentWireFormat.indexPath(messageType);
        schemaId = config.getInt(ProtoMoltSerdeConfig.SCHEMA_ID);
        validateOnWrite = config.getBoolean(ProtoMoltSerdeConfig.VALIDATE_ON_WRITE);
        validator = ProtoValidator.create();
    }

    @Override
    public byte[] serialize(String topic, Message data) {
        if (data == null) {
            return null;
        }
        if (!data.getDescriptorForType().getFullName().equals(messageType.getFullName())) {
            throw new SerializationException("This serializer is configured for "
                    + messageType.getFullName() + " but was handed a "
                    + data.getDescriptorForType().getFullName());
        }
        byte[] payload = data.toByteArray();
        if (validateOnWrite) {
            ValidationResult result = validator.validate(asConfiguredType(data, payload));
            if (!result.valid()) {
                throw new SerializationException("Message violates the schema's declared rules, "
                        + "so it was not written to " + topic + ": " + describe(result));
            }
        }
        return ConfluentWireFormat.frame(schemaId, indexPath, payload);
    }

    /**
     * The rules enforced are the configured schema's, not the caller's.
     *
     * <p>A message arrives carrying whatever descriptor the application built it from, and that
     * descriptor only knows its declared rules if the options were parsed with the extensions
     * registered. A caller whose descriptor lost them would otherwise sail through: the validator
     * would find a schema with no rules and pronounce every message clean, which is the failure
     * that reports success. Re-reading the payload under the configured type costs one parse and
     * buys the guarantee that the contract enforced is the one this serde was configured with.</p>
     */
    private Message asConfiguredType(Message data, byte[] payload) {
        if (data.getDescriptorForType() == messageType) {
            return data;
        }
        try {
            return DynamicMessage.parseFrom(messageType, payload);
        } catch (InvalidProtocolBufferException e) {
            throw new SerializationException("A " + messageType.getFullName()
                    + " could not be read back under the configured schema: " + e.getMessage(), e);
        }
    }

    /** Every violation, not just the first: a producer fixing them one at a time is a slow loop. */
    private static String describe(ValidationResult result) {
        return result.violations().stream()
                .map(violation -> violation.path() + " " + violation.message()
                        + " (" + violation.ruleId() + ")")
                .collect(Collectors.joining("; "));
    }

    /** Shared config-to-descriptors handling, so both serdes fail the same way. */
    static final class Descriptors {

        private Descriptors() {
        }

        static List<FileDescriptor> load(ProtoMoltSerdeConfig config, ClassLoader loader) {
            String resource = config.getString(ProtoMoltSerdeConfig.DESCRIPTOR_SET_RESOURCE);
            var base64 = config.getPassword(ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64);
            if ((resource == null) == (base64 == null)) {
                throw new ConfigException("Exactly one of "
                        + ProtoMoltSerdeConfig.DESCRIPTOR_SET_RESOURCE + " or "
                        + ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64 + " must be set");
            }
            return resource != null
                    ? SerdeDescriptors.fromClasspath(resource, loader)
                    : SerdeDescriptors.fromBase64(base64.value());
        }
    }
}
