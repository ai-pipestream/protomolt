package ai.pipestream.proto.meta;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.ExtensionRegistry;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Reads {@code (ai.pipestream.proto.meta.v1.field|message)} options from descriptors.
 * Register extensions before parsing/building descriptor sets that carry them.
 */
public final class DescriptorMetadata {

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

    public static Optional<FieldMeta> field(FieldDescriptor field) {
        Objects.requireNonNull(field, "field");
        var options = field.getOptions();
        if (!options.hasExtension(MetadataProto.field)) {
            return Optional.empty();
        }
        return Optional.of(options.getExtension(MetadataProto.field));
    }

    public static Optional<MessageMeta> message(Descriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");
        var options = descriptor.getOptions();
        if (!options.hasExtension(MetadataProto.message)) {
            return Optional.empty();
        }
        return Optional.of(options.getExtension(MetadataProto.message));
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
