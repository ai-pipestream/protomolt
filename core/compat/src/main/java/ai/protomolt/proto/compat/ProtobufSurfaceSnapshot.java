package ai.protomolt.proto.compat;

import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.ApiStabilityResolver;
import ai.protomolt.proto.descriptors.StabilityTier;
import ai.protomolt.proto.meta.DescriptorMetadata;
import ai.protomolt.proto.meta.FieldMeta;
import ai.protomolt.proto.meta.MessageMeta;
import ai.protomolt.proto.meta.MetadataProto;
import ai.protomolt.proto.meta.StabilityOption;
import ai.protomolt.proto.meta.StabilityProto;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumOptions;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueOptions;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.FileOptions;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.DescriptorProtos.MethodDescriptorProto;
import com.google.protobuf.DescriptorProtos.MethodOptions;
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto;
import com.google.protobuf.DescriptorProtos.ServiceOptions;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.GeneratedMessage.GeneratedExtension;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Snapshot of a protobuf contract surface: a {@link FileDescriptorSet} paired with the
 * resolved {@link StabilityTier} for every message, field, oneof, enum, enum value,
 * service, and RPC method.
 *
 * @param descriptorSet    the protobuf descriptor set
 * @param elementStability map of canonical element paths to their resolved stability tiers
 */
@ApiStability(StabilityTier.STABLE)
public record ProtobufSurfaceSnapshot(
        FileDescriptorSet descriptorSet,
        Map<String, StabilityTier> elementStability) {

    private static final ExtensionRegistry EXTENSIONS = ExtensionRegistry.newInstance();

    static {
        DescriptorMetadata.registerExtensions(EXTENSIONS);
    }

    public ProtobufSurfaceSnapshot {
        Objects.requireNonNull(descriptorSet, "descriptorSet");
        Map<String, StabilityTier> derived = deriveStabilityMap(
                descriptorSet, DescriptorMetadata.DEFAULT_PROTO_STABILITY);
        if (elementStability != null && !elementStability.isEmpty()) {
            derived.putAll(elementStability);
        }
        elementStability = Collections.unmodifiableMap(new TreeMap<>(derived));
    }

    /**
     * Creates a snapshot from a {@link FileDescriptorSet}, resolving stability tiers from
     * descriptor options and package conventions.
     *
     * @param descriptorSet the protobuf descriptor set
     * @return the protobuf surface snapshot
     */
    public static ProtobufSurfaceSnapshot of(FileDescriptorSet descriptorSet) {
        return new ProtobufSurfaceSnapshot(descriptorSet, Map.of());
    }

    /**
     * Creates a snapshot from linked {@link FileDescriptor}s and their transitive imports.
     *
     * @param files linked file descriptors
     * @return the protobuf surface snapshot
     */
    public static ProtobufSurfaceSnapshot fromFiles(Collection<FileDescriptor> files) {
        Objects.requireNonNull(files, "files");
        Map<String, FileDescriptorProto> byName = new LinkedHashMap<>();
        for (FileDescriptor file : files) {
            collectFile(file, byName);
        }
        FileDescriptorSet set = FileDescriptorSet.newBuilder()
                .addAllFile(byName.values())
                .build();
        return of(set);
    }

    /**
     * Resolves the effective {@link StabilityTier} for a {@link SchemaDiff} path.
     *
     * @param path the schema diff element path
     * @return the resolved stability tier
     */
    public StabilityTier stabilityForPath(String path) {
        if (path == null || path.isEmpty()) {
            return DescriptorMetadata.DEFAULT_PROTO_STABILITY;
        }
        String normalized = stripMapSuffix(path);
        StabilityTier direct = elementStability.get(normalized);
        if (direct != null) {
            return direct;
        }
        int lastDot = normalized.lastIndexOf('.');
        while (lastDot > 0) {
            String parent = normalized.substring(0, lastDot);
            StabilityTier parentTier = elementStability.get(parent);
            if (parentTier != null) {
                return parentTier;
            }
            if (ApiStabilityResolver.isInternalPackageName(parent)) {
                return StabilityTier.INTERNAL;
            }
            lastDot = parent.lastIndexOf('.');
        }
        return DescriptorMetadata.DEFAULT_PROTO_STABILITY;
    }

    private static String stripMapSuffix(String path) {
        int paren = path.indexOf(" (map ");
        return paren > 0 ? path.substring(0, paren) : path;
    }

    private static void collectFile(FileDescriptor file, Map<String, FileDescriptorProto> out) {
        if (out.putIfAbsent(file.getName(), file.toProto()) != null) {
            return;
        }
        for (FileDescriptor dep : file.getDependencies()) {
            collectFile(dep, out);
        }
    }

    private static Map<String, StabilityTier> deriveStabilityMap(
            FileDescriptorSet set,
            StabilityTier defaultTier) {
        Map<String, StabilityTier> result = new LinkedHashMap<>();
        for (FileDescriptorProto file : set.getFileList()) {
            String pkg = file.getPackage();
            StabilityTier fileTier = resolveFileStability(file, defaultTier);
            if (!pkg.isEmpty()) {
                result.putIfAbsent(pkg, fileTier);
            }
            for (DescriptorProto message : file.getMessageTypeList()) {
                String fqn = qualify(pkg, message.getName());
                indexMessage(fqn, message, fileTier, result);
            }
            for (EnumDescriptorProto enumProto : file.getEnumTypeList()) {
                String fqn = qualify(pkg, enumProto.getName());
                indexEnum(fqn, enumProto, fileTier, result);
            }
            for (ServiceDescriptorProto service : file.getServiceList()) {
                String fqn = qualify(pkg, service.getName());
                indexService(fqn, service, fileTier, result);
            }
        }
        return result;
    }

    private static void indexMessage(
            String fqn,
            DescriptorProto message,
            StabilityTier parentTier,
            Map<String, StabilityTier> result) {
        if (message.getOptions().getMapEntry()) {
            return;
        }
        StabilityTier msgTier = resolveMessageStability(message, parentTier);
        result.put(fqn, msgTier);

        for (FieldDescriptorProto field : message.getFieldList()) {
            StabilityTier fieldTier = resolveFieldStability(field, msgTier);
            result.put(fqn + "." + field.getName(), fieldTier);
        }
        for (OneofDescriptorProto oneof : message.getOneofDeclList()) {
            result.put(fqn + "." + oneof.getName(), msgTier);
        }
        for (DescriptorProto nested : message.getNestedTypeList()) {
            indexMessage(fqn + "." + nested.getName(), nested, msgTier, result);
        }
        for (EnumDescriptorProto nestedEnum : message.getEnumTypeList()) {
            indexEnum(fqn + "." + nestedEnum.getName(), nestedEnum, msgTier, result);
        }
    }

    private static void indexEnum(
            String fqn,
            EnumDescriptorProto enumProto,
            StabilityTier parentTier,
            Map<String, StabilityTier> result) {
        StabilityTier enumTier = resolveEnumStability(enumProto, parentTier);
        result.put(fqn, enumTier);
        for (EnumValueDescriptorProto value : enumProto.getValueList()) {
            StabilityTier valueTier = resolveEnumValueStability(value, enumTier);
            result.put(fqn + "." + value.getName(), valueTier);
        }
    }

    private static void indexService(
            String fqn,
            ServiceDescriptorProto service,
            StabilityTier parentTier,
            Map<String, StabilityTier> result) {
        StabilityTier serviceTier = resolveServiceStability(service, parentTier);
        result.put(fqn, serviceTier);
        for (MethodDescriptorProto method : service.getMethodList()) {
            StabilityTier methodTier = resolveMethodStability(method, serviceTier);
            result.put(fqn + "." + method.getName(), methodTier);
        }
    }

    private static StabilityTier resolveFileStability(FileDescriptorProto file, StabilityTier fallback) {
        FileOptions options = file.getOptions();
        Optional<StabilityTier> explicit = readOption(
                options, StabilityProto.fileStability, FileOptions.parser())
                .flatMap(opt -> DescriptorMetadata.toStabilityTier(opt.getLevel()));
        if (explicit.isPresent()) {
            return applyDeprecated(explicit.get(), options.getDeprecated());
        }
        if (options.getDeprecated()) {
            return StabilityTier.DEPRECATED;
        }
        if (ApiStabilityResolver.isInternalPackageName(file.getPackage())) {
            return StabilityTier.INTERNAL;
        }
        return fallback;
    }

    private static StabilityTier resolveMessageStability(DescriptorProto message, StabilityTier fallback) {
        MessageOptions options = message.getOptions();
        Optional<StabilityTier> explicit = readOption(
                options, StabilityProto.messageStability, MessageOptions.parser())
                .flatMap(opt -> DescriptorMetadata.toStabilityTier(opt.getLevel()));
        if (explicit.isEmpty()) {
            explicit = readOption(options, MetadataProto.message, MessageOptions.parser())
                    .map(MessageMeta::getLabelsMap)
                    .map(labels -> labels.get(DescriptorMetadata.STABILITY_LABEL_KEY))
                    .flatMap(StabilityTier::parseOptional);
        }
        if (explicit.isPresent()) {
            return applyDeprecated(explicit.get(), options.getDeprecated());
        }
        if (options.getDeprecated()) {
            return StabilityTier.DEPRECATED;
        }
        return fallback;
    }

    private static StabilityTier resolveFieldStability(FieldDescriptorProto field, StabilityTier fallback) {
        FieldOptions options = field.getOptions();
        Optional<StabilityTier> explicit = readOption(
                options, StabilityProto.fieldStability, FieldOptions.parser())
                .flatMap(opt -> DescriptorMetadata.toStabilityTier(opt.getLevel()));
        if (explicit.isEmpty()) {
            explicit = readOption(options, MetadataProto.field, FieldOptions.parser())
                    .map(FieldMeta::getLabelsMap)
                    .map(labels -> labels.get(DescriptorMetadata.STABILITY_LABEL_KEY))
                    .flatMap(StabilityTier::parseOptional);
        }
        if (explicit.isPresent()) {
            return applyDeprecated(explicit.get(), options.getDeprecated());
        }
        if (options.getDeprecated()) {
            return StabilityTier.DEPRECATED;
        }
        return fallback;
    }

    private static StabilityTier resolveEnumStability(EnumDescriptorProto enumProto, StabilityTier fallback) {
        EnumOptions options = enumProto.getOptions();
        Optional<StabilityTier> explicit = readOption(
                options, StabilityProto.enumStability, EnumOptions.parser())
                .flatMap(opt -> DescriptorMetadata.toStabilityTier(opt.getLevel()));
        if (explicit.isPresent()) {
            return applyDeprecated(explicit.get(), options.getDeprecated());
        }
        if (options.getDeprecated()) {
            return StabilityTier.DEPRECATED;
        }
        return fallback;
    }

    private static StabilityTier resolveEnumValueStability(
            EnumValueDescriptorProto valueProto,
            StabilityTier fallback) {
        EnumValueOptions options = valueProto.getOptions();
        Optional<StabilityTier> explicit = readOption(
                options, StabilityProto.enumValueStability, EnumValueOptions.parser())
                .flatMap(opt -> DescriptorMetadata.toStabilityTier(opt.getLevel()));
        if (explicit.isPresent()) {
            return applyDeprecated(explicit.get(), options.getDeprecated());
        }
        if (options.getDeprecated()) {
            return StabilityTier.DEPRECATED;
        }
        return fallback;
    }

    private static StabilityTier resolveServiceStability(
            ServiceDescriptorProto serviceProto,
            StabilityTier fallback) {
        ServiceOptions options = serviceProto.getOptions();
        Optional<StabilityTier> explicit = readOption(
                options, StabilityProto.serviceStability, ServiceOptions.parser())
                .flatMap(opt -> DescriptorMetadata.toStabilityTier(opt.getLevel()));
        if (explicit.isPresent()) {
            return applyDeprecated(explicit.get(), options.getDeprecated());
        }
        if (options.getDeprecated()) {
            return StabilityTier.DEPRECATED;
        }
        return fallback;
    }

    private static StabilityTier resolveMethodStability(
            MethodDescriptorProto methodProto,
            StabilityTier fallback) {
        MethodOptions options = methodProto.getOptions();
        Optional<StabilityTier> explicit = readOption(
                options, StabilityProto.methodStability, MethodOptions.parser())
                .flatMap(opt -> DescriptorMetadata.toStabilityTier(opt.getLevel()));
        if (explicit.isPresent()) {
            return applyDeprecated(explicit.get(), options.getDeprecated());
        }
        if (options.getDeprecated()) {
            return StabilityTier.DEPRECATED;
        }
        return fallback;
    }

    private static StabilityTier applyDeprecated(StabilityTier tier, boolean deprecated) {
        if (deprecated && tier == StabilityTier.STABLE) {
            return StabilityTier.DEPRECATED;
        }
        return tier;
    }

    private static <M extends Message, T> Optional<T> readOption(
            M options,
            GeneratedExtension<M, T> extension,
            Parser<M> parser) {
        M resolved = options;
        if (!resolved.hasField(extension.getDescriptor())
                && resolved.getUnknownFields().hasField(extension.getNumber())) {
            try {
                resolved = parser.parseFrom(resolved.toByteString(), EXTENSIONS);
            } catch (InvalidProtocolBufferException invalid) {
                throw new IllegalArgumentException(
                        "Invalid protobuf stability option " + extension.getDescriptor().getFullName(), invalid);
            }
        }
        if (!resolved.hasField(extension.getDescriptor())) {
            return Optional.empty();
        }
        @SuppressWarnings("unchecked")
        T value = (T) resolved.getField(extension.getDescriptor());
        return Optional.of(value);
    }

    private static String qualify(String pkg, String name) {
        return pkg == null || pkg.isEmpty() ? name : pkg + "." + name;
    }
}
