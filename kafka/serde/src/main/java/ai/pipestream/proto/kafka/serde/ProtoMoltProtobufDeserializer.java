package ai.pipestream.proto.kafka.serde;

import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A Kafka protobuf deserializer that reads the Confluent wire format and resolves the message
 * type from the descriptor set the deployment packages, not from a registry lookup per record.
 *
 * <p>The frame carries a schema id and an index path. With a registry configured, the id and
 * index resolve to the actual type the writer framed, and its <em>name</em> is checked against
 * the configured type: an index path is a position in the writer's file, and two files can
 * declare the same message at different positions, so comparing positions across files would
 * refuse records that are exactly the configured type. Without a registry answer the index path
 * is all there is, and it is compared against the configured type's position in the packaged
 * file. Either way, a frame carrying some other type is a topic this consumer was not configured
 * for, and saying so beats parsing the bytes as the wrong message and returning nonsense.</p>
 *
 * <p>Validation on read is off by default. It is worth turning on for a topic whose producers do
 * not all go through this serde, which is the only way invalid data gets in.</p>
 *
 * @see ProtoMoltSerdeConfig
 */
public class ProtoMoltProtobufDeserializer implements Deserializer<Message> {

    private ProtoValidator validator;
    private Descriptor messageType;
    private List<Integer> indexPath;
    private boolean validateOnRead;
    private SchemaIds schemaIds;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        ProtoMoltSerdeConfig config = new ProtoMoltSerdeConfig(configs);
        List<FileDescriptor> files =
                ProtoMoltProtobufSerializer.Descriptors.load(config, getClass().getClassLoader());
        messageType = SerdeDescriptors.messageType(files,
                config.getString(ProtoMoltSerdeConfig.MESSAGE_TYPE));
        indexPath = ConfluentWireFormat.indexPath(messageType);
        validateOnRead = config.getBoolean(ProtoMoltSerdeConfig.VALIDATE_ON_READ);
        validator = ProtoValidator.create();
        schemaIds = SchemaIds.create(config.getString(ProtoMoltSerdeConfig.REGISTRY_URL),
                config.getLong(ProtoMoltSerdeConfig.REGISTRY_RETRY_BACKOFF_MS));
    }

    @Override
    public void close() {
        if (schemaIds != null) {
            schemaIds.close();
        }
    }

    @Override
    public Message deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        Descriptor type = resolve(topic, ConfluentWireFormat.schemaId(data),
                ConfluentWireFormat.messageIndex(data));
        Message message;
        try {
            message = DynamicMessage.parseFrom(type, ConfluentWireFormat.payload(data));
        } catch (InvalidProtocolBufferException e) {
            throw new SerializationException("A record on " + topic + " is not a valid "
                    + type.getFullName() + ": " + e.getMessage(), e);
        }
        if (validateOnRead) {
            ValidationResult result = validator.validate(message);
            if (!result.valid()) {
                throw new SerializationException("A record on " + topic + " violates the schema's "
                        + "declared rules: " + describe(result));
            }
        }
        return message;
    }

    /**
     * The schema the frame's id names, when a registry can say, and the packaged one otherwise.
     *
     * <p>The registry is the better answer twice over. It describes what the writer actually
     * wrote, which is how a consumer follows a topic whose producers moved on — and it names the
     * framed type, so the type check compares names rather than file positions. A writer whose
     * file declares the configured type at a different index is still writing the configured
     * type; only the registry can say so.</p>
     *
     * <p>Falling back rather than failing keeps the registry a metadata service instead of a
     * runtime dependency of every consumer: an unreachable registry should not stop a consumer
     * whose packaged schema still reads the bytes in front of it. The fallback check is
     * positional, because without the writer's file an index path cannot be turned into a
     * name.</p>
     */
    private Descriptor resolve(String topic, int schemaId, List<Integer> framedIndex) {
        if (schemaIds != null) {
            Descriptor written = schemaIds.messageFor(schemaId, framedIndex);
            if (written != null) {
                if (!written.getFullName().equals(messageType.getFullName())) {
                    throw new SerializationException("A record on " + topic + " is a "
                            + written.getFullName() + " according to schema id " + schemaId
                            + ", but this deserializer is configured for "
                            + messageType.getFullName()
                            + "; the topic is carrying a type this consumer does not expect");
                }
                return written;
            }
        }
        if (!framedIndex.equals(indexPath)) {
            throw new SerializationException("A record on " + topic + " points at message index "
                    + framedIndex + ", but this deserializer is configured for "
                    + messageType.getFullName() + " at index " + indexPath
                    + "; the topic is carrying a type this consumer does not expect");
        }
        return messageType;
    }

    private static String describe(ValidationResult result) {
        return result.violations().stream()
                .map(violation -> violation.path() + " " + violation.message()
                        + " (" + violation.ruleId() + ")")
                .collect(Collectors.joining("; "));
    }
}
