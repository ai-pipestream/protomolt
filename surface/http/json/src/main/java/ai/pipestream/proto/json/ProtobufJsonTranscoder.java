package ai.pipestream.proto.json;

import ai.pipestream.proto.descriptors.DescriptorRegistry;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import java.util.Collection;
import java.util.Objects;

/**
 * Framework-agnostic protobuf ↔ JSON transcoder.
 *
 * <p>Handles {@link DynamicMessage} via a {@link DescriptorRegistry}-backed type registry
 * (Apicurio / Confluent / classpath descriptors).
 */
public final class ProtobufJsonTranscoder {

    private final DescriptorRegistry descriptorRegistry;
    /**
     * Printer/parser pair snapshot; rebuilt when the (lazily loaded) registry grows.
     * Races are benign: rebuilds are idempotent and readers always see a consistent pair.
     */
    private volatile Codecs codecs;

    public ProtobufJsonTranscoder() {
        this(null);
    }

    public ProtobufJsonTranscoder(DescriptorRegistry descriptorRegistry) {
        this.descriptorRegistry = descriptorRegistry;
        this.codecs = buildCodecs(descriptorRegistry);
    }

    public String toJson(Message message) {
        Objects.requireNonNull(message, "message");
        try {
            return currentCodecs().printer().print(message);
        } catch (InvalidProtocolBufferException e) {
            throw new ProtobufJsonException("Failed to serialize protobuf message to JSON", e);
        }
    }

    /**
     * Parses JSON into a generated message type that exposes {@code newBuilder()}.
     */
    @SuppressWarnings("unchecked")
    public <T extends Message> T fromJson(String json, Class<T> messageType) {
        Objects.requireNonNull(json, "json");
        Objects.requireNonNull(messageType, "messageType");
        try {
            Message.Builder builder = (Message.Builder) messageType.getMethod("newBuilder").invoke(null);
            currentCodecs().parser().merge(json, builder);
            return (T) builder.build();
        } catch (InvalidProtocolBufferException e) {
            throw new MalformedProtobufJsonException(
                    "Failed to deserialize JSON to " + messageType.getSimpleName(), json, e);
        } catch (ReflectiveOperationException e) {
            throw new ProtobufJsonException(
                    "Type " + messageType.getName() + " is not a protobuf Message with newBuilder()", e);
        }
    }

    /**
     * Parses JSON into a {@link DynamicMessage} for the given descriptor full name,
     * resolving the descriptor through the configured {@link DescriptorRegistry}.
     */
    public DynamicMessage fromJsonDynamic(String json, String messageFullName) {
        Objects.requireNonNull(json, "json");
        Objects.requireNonNull(messageFullName, "messageFullName");
        if (descriptorRegistry == null) {
            throw new ProtobufJsonException(
                    "Dynamic JSON parsing requires a DescriptorRegistry (Apicurio/Confluent/classpath)");
        }
        Descriptor descriptor = descriptorRegistry.findDescriptor(messageFullName);
        if (descriptor == null) {
            throw new ProtobufJsonException("Unknown message type: " + messageFullName);
        }
        return fromJsonDynamic(json, descriptor);
    }

    public DynamicMessage fromJsonDynamic(String json, Descriptor descriptor) {
        Objects.requireNonNull(json, "json");
        Objects.requireNonNull(descriptor, "descriptor");
        try {
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
            currentCodecs().parser().merge(json, builder);
            return builder.build();
        } catch (InvalidProtocolBufferException e) {
            throw new MalformedProtobufJsonException(
                    "Failed to deserialize JSON to " + descriptor.getFullName(), json, e);
        }
    }

    public DescriptorRegistry getDescriptorRegistry() {
        return descriptorRegistry;
    }

    private Codecs currentCodecs() {
        Codecs current = codecs;
        if (descriptorRegistry != null && descriptorRegistry.size() != current.descriptorCount()) {
            current = buildCodecs(descriptorRegistry);
            codecs = current;
        }
        return current;
    }

    private static Codecs buildCodecs(DescriptorRegistry registry) {
        int descriptorCount = 0;
        JsonFormat.TypeRegistry.Builder builder = JsonFormat.TypeRegistry.newBuilder();
        if (registry != null) {
            Collection<Descriptor> descriptors = registry.registeredDescriptors();
            descriptorCount = descriptors.size();
            if (!descriptors.isEmpty()) {
                builder.add(descriptors);
            }
        }
        JsonFormat.TypeRegistry typeRegistry = builder.build();
        JsonFormat.Printer printer = JsonFormat.printer()
                .usingTypeRegistry(typeRegistry)
                .alwaysPrintFieldsWithNoPresence()
                .sortingMapKeys();
        JsonFormat.Parser parser = JsonFormat.parser()
                .usingTypeRegistry(typeRegistry)
                .ignoringUnknownFields();
        return new Codecs(printer, parser, descriptorCount);
    }

    private record Codecs(JsonFormat.Printer printer, JsonFormat.Parser parser, int descriptorCount) {
    }
}
