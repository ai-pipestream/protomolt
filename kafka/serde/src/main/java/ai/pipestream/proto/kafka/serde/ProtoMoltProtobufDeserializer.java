package ai.pipestream.proto.kafka.serde;

import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.apache.kafka.common.config.ConfigException;
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
 * <p>With {@code protomolt.message.type} unset the deserializer is unpinned: each frame's type is
 * whatever the registry says it is, which is how several event types share one topic. Unpinned
 * requires the registry, because without one an index path cannot be turned into a name — and an
 * id the registry cannot resolve fails that record rather than guessing. Ids resolve once and are
 * cached, so this degrades only for ids never seen before the outage.</p>
 *
 * <p>Records come back as the application's own generated classes when they are on the classpath
 * (see {@link GeneratedMessages}), and as {@code DynamicMessage} otherwise. Validation on read is
 * off by default; it is worth turning on for a topic whose producers do not all go through this
 * serde, which is the only way invalid data gets in.</p>
 *
 * @see ProtoMoltSerdeConfig
 */
public class ProtoMoltProtobufDeserializer implements Deserializer<Message> {

    private ProtoValidator validator;
    private Descriptor pinnedType;
    private List<Integer> pinnedIndexPath;
    private boolean validateOnRead;
    private SchemaIds schemaIds;
    private GeneratedMessages generated;
    private SerdeMetricsListener metrics;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        ProtoMoltSerdeConfig config = new ProtoMoltSerdeConfig(configs);
        List<FileDescriptor> files =
                ProtoMoltProtobufSerializer.Descriptors.load(config, getClass().getClassLoader());
        metrics = SerdeMetricsListeners.load(getClass().getClassLoader());
        schemaIds = SchemaIds.create(config.getString(ProtoMoltSerdeConfig.REGISTRY_URL),
                config.getLong(ProtoMoltSerdeConfig.REGISTRY_RETRY_BACKOFF_MS), metrics);
        String pinned = config.getString(ProtoMoltSerdeConfig.MESSAGE_TYPE);
        if (pinned == null && schemaIds == null) {
            throw new ConfigException(ProtoMoltSerdeConfig.MESSAGE_TYPE + " is required when no "
                    + "registry is configured: a frame's index path is a position, not a name, "
                    + "and only a registry or a pinned type can turn it into one");
        }
        pinnedType = pinned != null ? SerdeDescriptors.messageType(files, pinned) : null;
        pinnedIndexPath = pinnedType != null ? ConfluentWireFormat.indexPath(pinnedType) : null;
        validateOnRead = config.getBoolean(ProtoMoltSerdeConfig.VALIDATE_ON_READ);
        validator = ProtoValidator.create();
        generated = new GeneratedMessages(files,
                config.getBoolean(ProtoMoltSerdeConfig.GENERATED_CLASSES),
                getClass().getClassLoader());
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
            message = generated.parse(type, ConfluentWireFormat.payload(data));
        } catch (InvalidProtocolBufferException e) {
            throw new SerializationException("A record on " + topic + " is not a valid "
                    + type.getFullName() + ": " + e.getMessage(), e);
        }
        if (validateOnRead) {
            ValidationResult result = validator.validate(message);
            if (!result.valid()) {
                metrics.onValidationRejected(topic, type.getFullName(), false,
                        result.violations().stream()
                                .map(ValidationResult.Violation::ruleId).toList());
                throw new SerializationException("A record on " + topic + " violates the schema's "
                        + "declared rules: " + describe(result));
            }
        }
        metrics.onDeserialized(topic, type.getFullName());
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
     * whose packaged schema still reads the bytes in front of it. The pinned fallback check is
     * positional, because without the writer's file an index path cannot be turned into a name;
     * unpinned, there is no fallback type to check against, so an unresolvable id fails the
     * record rather than guessing.</p>
     */
    private Descriptor resolve(String topic, int schemaId, List<Integer> framedIndex) {
        Descriptor written = schemaIds != null
                ? schemaIds.messageFor(schemaId, framedIndex)
                : null;
        if (pinnedType == null) {
            if (written == null) {
                metrics.onTypeRefused(topic, SerdeMetricsListener.REASON_UNRESOLVED_ID);
                throw new SerializationException("A record on " + topic + " carries schema id "
                        + schemaId + ", which the registry could not resolve; without a "
                        + "configured " + ProtoMoltSerdeConfig.MESSAGE_TYPE
                        + " there is no packaged type to fall back to");
            }
            return written;
        }
        if (written != null) {
            if (!written.getFullName().equals(pinnedType.getFullName())) {
                metrics.onTypeRefused(topic, SerdeMetricsListener.REASON_WRONG_TYPE);
                throw new SerializationException("A record on " + topic + " is a "
                        + written.getFullName() + " according to schema id " + schemaId
                        + ", but this deserializer is configured for " + pinnedType.getFullName()
                        + "; the topic is carrying a type this consumer does not expect");
            }
            return written;
        }
        if (!framedIndex.equals(pinnedIndexPath)) {
            metrics.onTypeRefused(topic, SerdeMetricsListener.REASON_WRONG_TYPE);
            throw new SerializationException("A record on " + topic + " points at message index "
                    + framedIndex + ", but this deserializer is configured for "
                    + pinnedType.getFullName() + " at index " + pinnedIndexPath
                    + "; the topic is carrying a type this consumer does not expect");
        }
        return pinnedType;
    }

    private static String describe(ValidationResult result) {
        return result.violations().stream()
                .map(violation -> violation.path() + " " + violation.message()
                        + " (" + violation.ruleId() + ")")
                .collect(Collectors.joining("; "));
    }
}
