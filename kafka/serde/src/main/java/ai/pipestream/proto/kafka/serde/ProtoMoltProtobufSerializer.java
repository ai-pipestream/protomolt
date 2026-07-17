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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

    /** Bound on the caller-descriptor trust cache; a producer usually has exactly one. */
    private static final int MAX_TRUSTED_CALLERS = 64;

    private ProtoValidator validator;
    private Descriptor messageType;
    private List<Integer> indexPath;
    private int configuredSchemaId;
    private boolean validateOnWrite;
    private SchemaIds schemaIds;
    private String subject;
    private boolean isKey;
    // Caller descriptors already proven byte-identical to the configured schema (see below).
    private final ConcurrentMap<Descriptor, Boolean> sameSchema = new ConcurrentHashMap<>();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        ProtoMoltSerdeConfig config = new ProtoMoltSerdeConfig(configs);
        List<FileDescriptor> files = Descriptors.load(config, getClass().getClassLoader());
        messageType = SerdeDescriptors.messageType(files, config.getString(
                ProtoMoltSerdeConfig.MESSAGE_TYPE));
        indexPath = ConfluentWireFormat.indexPath(messageType);
        configuredSchemaId = config.getInt(ProtoMoltSerdeConfig.SCHEMA_ID);
        validateOnWrite = config.getBoolean(ProtoMoltSerdeConfig.VALIDATE_ON_WRITE);
        validator = ProtoValidator.create();
        schemaIds = SchemaIds.create(config.getString(ProtoMoltSerdeConfig.REGISTRY_URL),
                config.getLong(ProtoMoltSerdeConfig.REGISTRY_RETRY_BACKOFF_MS));
        subject = config.getString(ProtoMoltSerdeConfig.SUBJECT);
        this.isKey = isKey;
    }

    /**
     * The id to stamp: the registry's, when it has one for this subject, and the configured one
     * otherwise. {@link SchemaIds} caches the answer, so this is a map read per record — and a
     * lookup that failed during an outage is retried after the backoff rather than standing for
     * the life of the producer, so a registry that recovers repairs the stamped id.
     */
    private int schemaIdFor(String topic) {
        if (schemaIds == null) {
            return configuredSchemaId;
        }
        return schemaIds.idForSubject(subject != null ? subject : Subjects.of(topic, isKey))
                .orElse(configuredSchemaId);
    }

    @Override
    public void close() {
        if (schemaIds != null) {
            schemaIds.close();
        }
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
        return ConfluentWireFormat.frame(schemaIdFor(topic), indexPath, payload);
    }

    /**
     * The rules enforced are the configured schema's, not the caller's.
     *
     * <p>A message arrives carrying whatever descriptor the application built it from — often a
     * generated class compiled from a {@code .proto} that may or may not still match the
     * descriptor set this serde was configured with. When the caller's schema is byte-identical
     * to the configured one (its file and every transitive import), validating the message
     * directly enforces the same contract, so that proof is cached per descriptor and the common
     * case costs nothing per record. A caller whose schema differs — stale protos, or options
     * that lost their extensions — is re-read under the configured type: one parse, in exchange
     * for the guarantee that the contract enforced is the one this serde was configured with.</p>
     */
    private Message asConfiguredType(Message data, byte[] payload) {
        if (sharesConfiguredSchema(data.getDescriptorForType())) {
            return data;
        }
        try {
            return DynamicMessage.parseFrom(messageType, payload);
        } catch (InvalidProtocolBufferException e) {
            throw new SerializationException("A " + messageType.getFullName()
                    + " could not be read back under the configured schema: " + e.getMessage(), e);
        }
    }

    /** Whether {@code caller}'s schema is the configured schema, byte for byte. Cached. */
    private boolean sharesConfiguredSchema(Descriptor caller) {
        if (caller == messageType) {
            return true;
        }
        Boolean known = sameSchema.get(caller);
        if (known != null) {
            return known;
        }
        if (sameSchema.size() >= MAX_TRUSTED_CALLERS) {
            sameSchema.clear();
        }
        return sameSchema.computeIfAbsent(caller,
                d -> fileClosure(d.getFile()).equals(fileClosure(messageType.getFile())));
    }

    /**
     * The file and its transitive imports, keyed by file name, in comparable serialized form.
     * Imports matter: rules on an imported message type live in the imported file's options, so
     * equality of the root file alone would not prove the schemas agree. Source info is stripped
     * because descriptor sets built with {@code --include_source_info} differ from runtime
     * descriptors only in that metadata.
     */
    private static Map<String, com.google.protobuf.ByteString> fileClosure(FileDescriptor root) {
        Map<String, com.google.protobuf.ByteString> files = new java.util.HashMap<>();
        java.util.ArrayDeque<FileDescriptor> queue = new java.util.ArrayDeque<>(List.of(root));
        while (!queue.isEmpty()) {
            FileDescriptor file = queue.pop();
            com.google.protobuf.ByteString stripped = file.toProto().toBuilder()
                    .clearSourceCodeInfo().build().toByteString();
            if (files.put(file.getName(), stripped) == null) {
                queue.addAll(file.getDependencies());
            }
        }
        return files;
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
