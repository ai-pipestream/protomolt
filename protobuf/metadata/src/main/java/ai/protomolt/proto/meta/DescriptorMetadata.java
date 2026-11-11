package ai.protomolt.proto.meta;

import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.ApiStabilityResolver;
import ai.protomolt.proto.descriptors.StabilityTier;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.GeneratedMessage.GeneratedExtension;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.WeakHashMap;

/**
 * Reads {@code (ai.protomolt.proto.meta.v1.*)} metadata and stability options from descriptors.
 * Register extensions before parsing or building descriptor sets that carry them.
 */
@ApiStability(StabilityTier.STABLE)
public final class DescriptorMetadata {

    /** Label key recognized inside {@link FieldMeta#getLabelsMap()} and {@link MessageMeta#getLabelsMap()}. */
    public static final String STABILITY_LABEL_KEY = "stability";

    /** Default stability tier for unannotated public protobuf definitions. */
    public static final StabilityTier DEFAULT_PROTO_STABILITY = StabilityTier.STABLE;

    /** Reads the metadata options when a descriptor was built without them registered. */
    private static final ExtensionRegistry EXTENSIONS = ExtensionRegistry.newInstance();
    private static final Map<FieldDescriptor, Optional<FieldMeta>> FIELD_META =
            Collections.synchronizedMap(new WeakHashMap<>());
    private static final Map<Descriptor, Optional<MessageMeta>> MESSAGE_META =
            Collections.synchronizedMap(new WeakHashMap<>());

    static {
        MetadataProto.registerAllExtensions(EXTENSIONS);
        StabilityProto.registerAllExtensions(EXTENSIONS);
    }

    private DescriptorMetadata() {
    }

    /**
     * Registers all {@code ai.protomolt.proto.meta.v1} metadata and stability extensions on
     * the supplied registry.
     *
     * @param registry the extension registry to populate
     */
    public static void registerExtensions(ExtensionRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        MetadataProto.registerAllExtensions(registry);
        StabilityProto.registerAllExtensions(registry);
    }

    /**
     * Materializes annotation-carried JSON names into real {@code json_name}s: the
     * descriptor's own {@code json_name} does not survive every text round-trip (Wire's
     * encoder drops it), but the {@code meta.v1} option does — so loaders call this after
     * parsing a descriptor set (with the extensions registered) and fields regain the
     * original keys their documents use.
     */
    public static DescriptorProtos.FileDescriptorSet materializeJsonNames(
            DescriptorProtos.FileDescriptorSet set) {
        var out = set.toBuilder();
        for (var file : out.getFileBuilderList()) {
            for (var message : file.getMessageTypeBuilderList()) {
                materialize(message);
            }
        }
        return out.build();
    }

    private static void materialize(
            DescriptorProtos.DescriptorProto.Builder message) {
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
        Optional<FieldMeta> resolved = readExtension(
                field.getOptions(), MetadataProto.field, DescriptorProtos.FieldOptions.parser());
        FIELD_META.put(field, resolved);
        return resolved;
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
        Optional<MessageMeta> resolved = readExtension(
                descriptor.getOptions(), MetadataProto.message, DescriptorProtos.MessageOptions.parser());
        MESSAGE_META.put(descriptor, resolved);
        return resolved;
    }

    /**
     * Reads the explicit {@link StabilityOption} on a file descriptor, if present.
     *
     * @param file the file descriptor to inspect
     * @return the declared stability option, if present
     */
    public static Optional<StabilityOption> fileStabilityOption(FileDescriptor file) {
        Objects.requireNonNull(file, "file");
        return readExtension(
                file.getOptions(), StabilityProto.fileStability, DescriptorProtos.FileOptions.parser());
    }

    /**
     * Reads the explicit {@link StabilityOption} on a message descriptor, if present.
     *
     * @param descriptor the message descriptor to inspect
     * @return the declared stability option, if present
     */
    public static Optional<StabilityOption> messageStabilityOption(Descriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");
        return readExtension(
                descriptor.getOptions(), StabilityProto.messageStability, DescriptorProtos.MessageOptions.parser());
    }

    /**
     * Reads the explicit {@link StabilityOption} on a field descriptor, if present.
     *
     * @param field the field descriptor to inspect
     * @return the declared stability option, if present
     */
    public static Optional<StabilityOption> fieldStabilityOption(FieldDescriptor field) {
        Objects.requireNonNull(field, "field");
        return readExtension(
                field.getOptions(), StabilityProto.fieldStability, DescriptorProtos.FieldOptions.parser());
    }

    /**
     * Reads the explicit {@link StabilityOption} on an enum descriptor, if present.
     *
     * @param enumDescriptor the enum descriptor to inspect
     * @return the declared stability option, if present
     */
    public static Optional<StabilityOption> enumStabilityOption(EnumDescriptor enumDescriptor) {
        Objects.requireNonNull(enumDescriptor, "enumDescriptor");
        return readExtension(
                enumDescriptor.getOptions(), StabilityProto.enumStability, DescriptorProtos.EnumOptions.parser());
    }

    /**
     * Reads the explicit {@link StabilityOption} on an enum value descriptor, if present.
     *
     * @param valueDescriptor the enum value descriptor to inspect
     * @return the declared stability option, if present
     */
    public static Optional<StabilityOption> enumValueStabilityOption(EnumValueDescriptor valueDescriptor) {
        Objects.requireNonNull(valueDescriptor, "valueDescriptor");
        return readExtension(
                valueDescriptor.getOptions(),
                StabilityProto.enumValueStability,
                DescriptorProtos.EnumValueOptions.parser());
    }

    /**
     * Reads the explicit {@link StabilityOption} on a service descriptor, if present.
     *
     * @param serviceDescriptor the service descriptor to inspect
     * @return the declared stability option, if present
     */
    public static Optional<StabilityOption> serviceStabilityOption(ServiceDescriptor serviceDescriptor) {
        Objects.requireNonNull(serviceDescriptor, "serviceDescriptor");
        return readExtension(
                serviceDescriptor.getOptions(),
                StabilityProto.serviceStability,
                DescriptorProtos.ServiceOptions.parser());
    }

    /**
     * Reads the explicit {@link StabilityOption} on an RPC method descriptor, if present.
     *
     * @param methodDescriptor the RPC method descriptor to inspect
     * @return the declared stability option, if present
     */
    public static Optional<StabilityOption> methodStabilityOption(MethodDescriptor methodDescriptor) {
        Objects.requireNonNull(methodDescriptor, "methodDescriptor");
        return readExtension(
                methodDescriptor.getOptions(),
                StabilityProto.methodStability,
                DescriptorProtos.MethodOptions.parser());
    }

    /**
     * Resolves the effective {@link StabilityTier} of a protobuf file.
     *
     * @param file the file descriptor to inspect
     * @return the resolved stability tier
     */
    public static StabilityTier stability(FileDescriptor file) {
        return stability(file, DEFAULT_PROTO_STABILITY);
    }

    /**
     * Resolves the effective {@link StabilityTier} of a protobuf file.
     *
     * @param file the file descriptor to inspect
     * @param fallback fallback tier when no option or convention applies
     * @return the resolved stability tier
     */
    public static StabilityTier stability(FileDescriptor file, StabilityTier fallback) {
        Objects.requireNonNull(file, "file");
        Objects.requireNonNull(fallback, "fallback");
        Optional<StabilityTier> explicit = fileStabilityOption(file)
                .flatMap(DescriptorMetadata::toStabilityTier);
        if (explicit.isPresent()) {
            if (explicit.get() == StabilityTier.STABLE && file.getOptions().getDeprecated()) {
                return StabilityTier.DEPRECATED;
            }
            return explicit.get();
        }
        if (file.getOptions().getDeprecated()) {
            return StabilityTier.DEPRECATED;
        }
        if (ApiStabilityResolver.isInternalPackageName(file.getPackage())) {
            return StabilityTier.INTERNAL;
        }
        return fallback;
    }

    /**
     * Resolves the effective {@link StabilityTier} of a protobuf message descriptor.
     *
     * @param descriptor the message descriptor to inspect
     * @return the resolved stability tier
     */
    public static StabilityTier stability(Descriptor descriptor) {
        return stability(descriptor, DEFAULT_PROTO_STABILITY);
    }

    /**
     * Resolves the effective {@link StabilityTier} of a protobuf message descriptor.
     *
     * @param descriptor the message descriptor to inspect
     * @param fallback fallback tier when no option or convention applies
     * @return the resolved stability tier
     */
    public static StabilityTier stability(Descriptor descriptor, StabilityTier fallback) {
        Objects.requireNonNull(descriptor, "descriptor");
        Objects.requireNonNull(fallback, "fallback");
        Optional<StabilityTier> explicit = messageStabilityOption(descriptor)
                .flatMap(DescriptorMetadata::toStabilityTier);
        if (explicit.isEmpty()) {
            explicit = message(descriptor)
                    .map(meta -> meta.getLabelsMap().get(STABILITY_LABEL_KEY))
                    .flatMap(StabilityTier::parseExplicit);
        }
        if (explicit.isPresent()) {
            if (explicit.get() == StabilityTier.STABLE && descriptor.getOptions().getDeprecated()) {
                return StabilityTier.DEPRECATED;
            }
            return explicit.get();
        }
        if (descriptor.getOptions().getDeprecated()) {
            return StabilityTier.DEPRECATED;
        }
        if (descriptor.getContainingType() != null) {
            return stability(descriptor.getContainingType(), fallback);
        }
        return stability(descriptor.getFile(), fallback);
    }

    /**
     * Resolves the effective {@link StabilityTier} of a protobuf field descriptor.
     *
     * @param field the field descriptor to inspect
     * @return the resolved stability tier
     */
    public static StabilityTier stability(FieldDescriptor field) {
        return stability(field, DEFAULT_PROTO_STABILITY);
    }

    /**
     * Resolves the effective {@link StabilityTier} of a protobuf field descriptor.
     *
     * @param field the field descriptor to inspect
     * @param fallback fallback tier when no option or convention applies
     * @return the resolved stability tier
     */
    public static StabilityTier stability(FieldDescriptor field, StabilityTier fallback) {
        Objects.requireNonNull(field, "field");
        Objects.requireNonNull(fallback, "fallback");
        Optional<StabilityTier> explicit = fieldStabilityOption(field)
                .flatMap(DescriptorMetadata::toStabilityTier);
        if (explicit.isEmpty()) {
            explicit = field(field)
                    .map(meta -> meta.getLabelsMap().get(STABILITY_LABEL_KEY))
                    .flatMap(StabilityTier::parseExplicit);
        }
        if (explicit.isPresent()) {
            if (explicit.get() == StabilityTier.STABLE && field.getOptions().getDeprecated()) {
                return StabilityTier.DEPRECATED;
            }
            return explicit.get();
        }
        if (field.getOptions().getDeprecated()) {
            return StabilityTier.DEPRECATED;
        }
        if (!field.isExtension() && field.getContainingType() != null) {
            return stability(field.getContainingType(), fallback);
        }
        return stability(field.getFile(), fallback);
    }

    /**
     * Resolves the effective {@link StabilityTier} of a protobuf enum descriptor.
     *
     * @param enumDescriptor the enum descriptor to inspect
     * @return the resolved stability tier
     */
    public static StabilityTier stability(EnumDescriptor enumDescriptor) {
        return stability(enumDescriptor, DEFAULT_PROTO_STABILITY);
    }

    /**
     * Resolves the effective {@link StabilityTier} of a protobuf enum descriptor.
     *
     * @param enumDescriptor the enum descriptor to inspect
     * @param fallback fallback tier when no option or convention applies
     * @return the resolved stability tier
     */
    public static StabilityTier stability(EnumDescriptor enumDescriptor, StabilityTier fallback) {
        Objects.requireNonNull(enumDescriptor, "enumDescriptor");
        Objects.requireNonNull(fallback, "fallback");
        Optional<StabilityTier> explicit = enumStabilityOption(enumDescriptor)
                .flatMap(DescriptorMetadata::toStabilityTier);
        if (explicit.isPresent()) {
            if (explicit.get() == StabilityTier.STABLE && enumDescriptor.getOptions().getDeprecated()) {
                return StabilityTier.DEPRECATED;
            }
            return explicit.get();
        }
        if (enumDescriptor.getOptions().getDeprecated()) {
            return StabilityTier.DEPRECATED;
        }
        if (enumDescriptor.getContainingType() != null) {
            return stability(enumDescriptor.getContainingType(), fallback);
        }
        return stability(enumDescriptor.getFile(), fallback);
    }

    /**
     * Resolves the effective {@link StabilityTier} of a protobuf enum value descriptor.
     *
     * @param valueDescriptor the enum value descriptor to inspect
     * @return the resolved stability tier
     */
    public static StabilityTier stability(EnumValueDescriptor valueDescriptor) {
        return stability(valueDescriptor, DEFAULT_PROTO_STABILITY);
    }

    /**
     * Resolves the effective {@link StabilityTier} of a protobuf enum value descriptor.
     *
     * @param valueDescriptor the enum value descriptor to inspect
     * @param fallback fallback tier when no option or convention applies
     * @return the resolved stability tier
     */
    public static StabilityTier stability(EnumValueDescriptor valueDescriptor, StabilityTier fallback) {
        Objects.requireNonNull(valueDescriptor, "valueDescriptor");
        Objects.requireNonNull(fallback, "fallback");
        Optional<StabilityTier> explicit = enumValueStabilityOption(valueDescriptor)
                .flatMap(DescriptorMetadata::toStabilityTier);
        if (explicit.isPresent()) {
            if (explicit.get() == StabilityTier.STABLE && valueDescriptor.getOptions().getDeprecated()) {
                return StabilityTier.DEPRECATED;
            }
            return explicit.get();
        }
        if (valueDescriptor.getOptions().getDeprecated()) {
            return StabilityTier.DEPRECATED;
        }
        return stability(valueDescriptor.getType(), fallback);
    }

    /**
     * Resolves the effective {@link StabilityTier} of a protobuf service descriptor.
     *
     * @param serviceDescriptor the service descriptor to inspect
     * @return the resolved stability tier
     */
    public static StabilityTier stability(ServiceDescriptor serviceDescriptor) {
        return stability(serviceDescriptor, DEFAULT_PROTO_STABILITY);
    }

    /**
     * Resolves the effective {@link StabilityTier} of a protobuf service descriptor.
     *
     * @param serviceDescriptor the service descriptor to inspect
     * @param fallback fallback tier when no option or convention applies
     * @return the resolved stability tier
     */
    public static StabilityTier stability(ServiceDescriptor serviceDescriptor, StabilityTier fallback) {
        Objects.requireNonNull(serviceDescriptor, "serviceDescriptor");
        Objects.requireNonNull(fallback, "fallback");
        Optional<StabilityTier> explicit = serviceStabilityOption(serviceDescriptor)
                .flatMap(DescriptorMetadata::toStabilityTier);
        if (explicit.isPresent()) {
            if (explicit.get() == StabilityTier.STABLE && serviceDescriptor.getOptions().getDeprecated()) {
                return StabilityTier.DEPRECATED;
            }
            return explicit.get();
        }
        if (serviceDescriptor.getOptions().getDeprecated()) {
            return StabilityTier.DEPRECATED;
        }
        return stability(serviceDescriptor.getFile(), fallback);
    }

    /**
     * Resolves the effective {@link StabilityTier} of a protobuf RPC method descriptor.
     *
     * @param methodDescriptor the RPC method descriptor to inspect
     * @return the resolved stability tier
     */
    public static StabilityTier stability(MethodDescriptor methodDescriptor) {
        return stability(methodDescriptor, DEFAULT_PROTO_STABILITY);
    }

    /**
     * Resolves the effective {@link StabilityTier} of a protobuf RPC method descriptor.
     *
     * @param methodDescriptor the RPC method descriptor to inspect
     * @param fallback fallback tier when no option or convention applies
     * @return the resolved stability tier
     */
    public static StabilityTier stability(MethodDescriptor methodDescriptor, StabilityTier fallback) {
        Objects.requireNonNull(methodDescriptor, "methodDescriptor");
        Objects.requireNonNull(fallback, "fallback");
        Optional<StabilityTier> explicit = methodStabilityOption(methodDescriptor)
                .flatMap(DescriptorMetadata::toStabilityTier);
        if (explicit.isPresent()) {
            if (explicit.get() == StabilityTier.STABLE && methodDescriptor.getOptions().getDeprecated()) {
                return StabilityTier.DEPRECATED;
            }
            return explicit.get();
        }
        if (methodDescriptor.getOptions().getDeprecated()) {
            return StabilityTier.DEPRECATED;
        }
        return stability(methodDescriptor.getService(), fallback);
    }

    /**
     * Converts a protobuf {@link StabilityOption} to a {@link StabilityTier}, failing closed
     * when the option carries an unrecognized {@link StabilityLevel} wire value.
     *
     * @param option the protobuf stability option (may be null)
     * @return the matching tier, or empty when {@code option} is null or its level is
     *         {@link StabilityLevel#STABILITY_LEVEL_UNSPECIFIED}
     * @throws IllegalArgumentException if {@code option} carries {@link StabilityLevel#UNRECOGNIZED}
     */
    public static Optional<StabilityTier> toStabilityTier(StabilityOption option) {
        if (option == null) {
            return Optional.empty();
        }
        if (option.getLevel() == StabilityLevel.UNRECOGNIZED) {
            throw new IllegalArgumentException(
                    "Unrecognized protobuf StabilityLevel wire value: " + option.getLevelValue());
        }
        return toStabilityTier(option.getLevel());
    }

    /**
     * Converts a protobuf {@link StabilityLevel} enum value to a {@link StabilityTier}.
     *
     * @param level the protobuf stability level
     * @return the matching tier, or empty for {@link StabilityLevel#STABILITY_LEVEL_UNSPECIFIED}
     * @throws IllegalArgumentException if {@code level} is {@link StabilityLevel#UNRECOGNIZED}
     */
    public static Optional<StabilityTier> toStabilityTier(StabilityLevel level) {
        if (level == null) {
            return Optional.empty();
        }
        return switch (level) {
            case STABILITY_LEVEL_STABLE -> Optional.of(StabilityTier.STABLE);
            case STABILITY_LEVEL_EVOLVING -> Optional.of(StabilityTier.EVOLVING);
            case STABILITY_LEVEL_EXPERIMENTAL -> Optional.of(StabilityTier.EXPERIMENTAL);
            case STABILITY_LEVEL_INTERNAL -> Optional.of(StabilityTier.INTERNAL);
            case STABILITY_LEVEL_DEPRECATED -> Optional.of(StabilityTier.DEPRECATED);
            case STABILITY_LEVEL_UNSPECIFIED -> Optional.empty();
            case UNRECOGNIZED -> throw new IllegalArgumentException(
                    "Unrecognized protobuf StabilityLevel enum constant");
        };
    }

    /**
     * Converts a {@link StabilityTier} to its protobuf {@link StabilityLevel} counterpart.
     *
     * @param tier the stability tier
     * @return the protobuf stability level
     */
    public static StabilityLevel toStabilityLevel(StabilityTier tier) {
        if (tier == null) {
            return StabilityLevel.STABILITY_LEVEL_UNSPECIFIED;
        }
        return switch (tier) {
            case STABLE -> StabilityLevel.STABILITY_LEVEL_STABLE;
            case EVOLVING -> StabilityLevel.STABILITY_LEVEL_EVOLVING;
            case EXPERIMENTAL -> StabilityLevel.STABILITY_LEVEL_EXPERIMENTAL;
            case INTERNAL -> StabilityLevel.STABILITY_LEVEL_INTERNAL;
            case DEPRECATED -> StabilityLevel.STABILITY_LEVEL_DEPRECATED;
        };
    }

    private static <M extends Message, T> Optional<T> readExtension(
            M options,
            GeneratedExtension<M, T> extension,
            Parser<M> parser) {
        M resolved = options;
        if (!resolved.hasField(extension.getDescriptor())
                && resolved.getUnknownFields().hasField(extension.getNumber())) {
            resolved = reparse(resolved.toByteString(), parser);
        }
        if (!resolved.hasField(extension.getDescriptor())) {
            return Optional.empty();
        }
        @SuppressWarnings("unchecked")
        T value = (T) resolved.getField(extension.getDescriptor());
        return Optional.of(value);
    }

    private static <T> T reparse(com.google.protobuf.ByteString options, Parser<T> parser) {
        try {
            return parser.parseFrom(options, EXTENSIONS);
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Descriptor options are not valid protobuf", failure);
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
