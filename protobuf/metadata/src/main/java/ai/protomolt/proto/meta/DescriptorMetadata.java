package ai.protomolt.proto.meta;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.ExtensionRegistry;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.WeakHashMap;

/**
 * Reads {@code (ai.protomolt.proto.meta.v1.field|message)} options from descriptors.
 * Register extensions before parsing/building descriptor sets that carry them.
 */
public final class DescriptorMetadata {

    /** Reads the metadata options when a descriptor was built without them registered. */
    private static final ExtensionRegistry EXTENSIONS = ExtensionRegistry.newInstance();
    private static final Map<FieldDescriptor, Optional<FieldMeta>> FIELD_META =
            Collections.synchronizedMap(new WeakHashMap<>());
    private static final Map<Descriptor, Optional<MessageMeta>> MESSAGE_META =
            Collections.synchronizedMap(new WeakHashMap<>());

    static {
        MetadataProto.registerAllExtensions(EXTENSIONS);
    }

    private DescriptorMetadata() {
    }

    public static void registerExtensions(ExtensionRegistry registry) {
        MetadataProto.registerAllExtensions(Objects.requireNonNull(registry, "registry"));
    }

    /**
     * Materializes annotation-carried JSON names into real {@code json_name}s: the
     * descriptor's own {@code json_name} does not survive every text round-trip (Wire's
     * encoder drops it), but the {@code meta.v1} option does — so loaders call this after
     * parsing a descriptor set (with the extensions registered) and fields regain the
     * original keys their documents use.
     */
    public static com.google.protobuf.DescriptorProtos.FileDescriptorSet materializeJsonNames(
            com.google.protobuf.DescriptorProtos.FileDescriptorSet set) {
        var out = set.toBuilder();
        for (var file : out.getFileBuilderList()) {
            for (var message : file.getMessageTypeBuilderList()) {
                materialize(message);
            }
        }
        return out.build();
    }

    private static void materialize(
            com.google.protobuf.DescriptorProtos.DescriptorProto.Builder message) {
        for (var field : message.getFieldBuilderList()) {
            if (!field.hasJsonName()
                    && field.getOptions().hasExtension(MetadataProto.field)) {
                String original = field.getOptions()
                        .getExtension(MetadataProto.field).getJsonName();
                if (!original.isEmpty()) {
                    field.setJsonName(original);
                }
            }
        }
        for (var nested : message.getNestedTypeBuilderList()) {
            materialize(nested);
        }
    }

    /**
     * The field's metadata option. A descriptor compiled or parsed without the metadata
     * extensions registered keeps the option as an unknown field; it is read from there
     * too, so callers such as {@link SensitivityMasker} never treat an annotated field as
     * unannotated because of how its descriptor was loaded.
     *
     * @param field the field descriptor to inspect
     * @return the field metadata option, if present
     */
    public static Optional<FieldMeta> field(FieldDescriptor field) {
        Objects.requireNonNull(field, "field");
        Optional<FieldMeta> cached = FIELD_META.get(field);
        if (cached != null) {
            return cached;
        }
        Optional<FieldMeta> resolved = readFieldMeta(field);
        FIELD_META.put(field, resolved);
        return resolved;
    }

    private static Optional<FieldMeta> readFieldMeta(FieldDescriptor field) {
        var options = field.getOptions();
        if (!options.hasExtension(MetadataProto.field)
                && options.getUnknownFields().hasField(MetadataProto.field.getNumber())) {
            options = reparse(options.toByteString(), com.google.protobuf.DescriptorProtos.FieldOptions.parser());
        }
        if (!options.hasExtension(MetadataProto.field)) {
            return Optional.empty();
        }
        return Optional.of(options.getExtension(MetadataProto.field));
    }

    /**
     * The message's metadata option, including when the descriptor was built without
     * metadata extensions registered.
     *
     * @param descriptor the message descriptor to inspect
     * @return the message metadata option, if present
     */
    public static Optional<MessageMeta> message(Descriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");
        Optional<MessageMeta> cached = MESSAGE_META.get(descriptor);
        if (cached != null) {
            return cached;
        }
        Optional<MessageMeta> resolved = readMessageMeta(descriptor);
        MESSAGE_META.put(descriptor, resolved);
        return resolved;
    }

    private static Optional<MessageMeta> readMessageMeta(Descriptor descriptor) {
        var options = descriptor.getOptions();
        if (!options.hasExtension(MetadataProto.message)
                && options.getUnknownFields().hasField(MetadataProto.message.getNumber())) {
            options = reparse(options.toByteString(), com.google.protobuf.DescriptorProtos.MessageOptions.parser());
        }
        if (!options.hasExtension(MetadataProto.message)) {
            return Optional.empty();
        }
        return Optional.of(options.getExtension(MetadataProto.message));
    }

    private static <T> T reparse(com.google.protobuf.ByteString options, com.google.protobuf.Parser<T> parser) {
        try {
            return parser.parseFrom(options, EXTENSIONS);
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalStateException("descriptor options are not valid protobuf", failure);
        }
    }

    /**
     * Flattens message + field metadata into a bag suitable for logs, headers, or CEL.
     * Keys: {@code message.*} and {@code field.&lt;name&gt;.*}.
     */
    public static Map<String, Object> asBag(Descriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");
        Map<String, Object> bag = new LinkedHashMap<>();
        message(descriptor).ifPresent(meta -> putMessage(bag, "message", meta));
        for (FieldDescriptor field : descriptor.getFields()) {
            field(field).ifPresent(meta -> putField(bag, "field." + field.getName(), meta));
        }
        return Map.copyOf(bag);
    }

    private static void putMessage(Map<String, Object> bag, String prefix, MessageMeta meta) {
        if (!meta.getDescription().isEmpty()) {
            bag.put(prefix + ".description", meta.getDescription());
        }
        if (!meta.getOwner().isEmpty()) {
            bag.put(prefix + ".owner", meta.getOwner());
        }
        if (!meta.getSensitivity().isEmpty()) {
            bag.put(prefix + ".sensitivity", meta.getSensitivity());
        }
        meta.getLabelsMap().forEach((k, v) -> bag.put(prefix + ".labels." + k, v));
    }

    private static void putField(Map<String, Object> bag, String prefix, FieldMeta meta) {
        if (!meta.getDescription().isEmpty()) {
            bag.put(prefix + ".description", meta.getDescription());
        }
        if (!meta.getDisplayName().isEmpty()) {
            bag.put(prefix + ".display_name", meta.getDisplayName());
        }
        if (!meta.getOwner().isEmpty()) {
            bag.put(prefix + ".owner", meta.getOwner());
        }
        if (!meta.getSensitivity().isEmpty()) {
            bag.put(prefix + ".sensitivity", meta.getSensitivity());
        }
        meta.getLabelsMap().forEach((k, v) -> bag.put(prefix + ".labels." + k, v));
    }
}
