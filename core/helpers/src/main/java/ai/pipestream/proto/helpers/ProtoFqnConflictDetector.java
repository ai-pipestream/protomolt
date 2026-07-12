package ai.pipestream.proto.helpers;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto;
import com.google.protobuf.Message;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Detects cross-file fully-qualified-name conflicts among protobuf descriptors.
 *
 * <p>Comparison uses compiled descriptor equality ({@link Message#equals(Object)}), so
 * comments and whitespace are irrelevant; wire layout (fields, types, tags, labels,
 * oneofs, reserved ranges, options) drives equality.
 *
 * <p>Nested messages are compared shallowly (nested types cleared) so a conflict on an
 * inner type reports the inner FQN rather than its wrapper.
 */
public final class ProtoFqnConflictDetector {

    public static final String MAIN_SOURCE_NAME = "<main>";

    private ProtoFqnConflictDetector() {
    }

    /**
     * Walks every source and rejects incompatible redefinitions of the same FQN.
     * Identical definitions across sources are allowed (benign duplicates).
     * Same-source redefinitions are ignored (left to the normal protobuf validators).
     *
     * @param sources insertion-ordered map of logical source name → {@link FileDescriptorProto}
     *                (references should be listed before main when attribution matters)
     * @throws ProtoSchemaValidationException on a conflicting FQN
     */
    public static void assertNoConflicts(Map<String, FileDescriptorProto> sources)
            throws ProtoSchemaValidationException {
        Objects.requireNonNull(sources, "sources");
        Map<String, Definition> known = new HashMap<>();
        for (Map.Entry<String, FileDescriptorProto> entry : sources.entrySet()) {
            walkSource(entry.getKey(), entry.getValue(), known);
        }
    }

    /**
     * Convenience for main + references. References are walked first.
     */
    public static void assertNoConflicts(
            String mainName,
            FileDescriptorProto main,
            Map<String, FileDescriptorProto> references) throws ProtoSchemaValidationException {
        Map<String, FileDescriptorProto> sources = new LinkedHashMap<>();
        if (references != null) {
            sources.putAll(references);
        }
        sources.put(mainName == null ? MAIN_SOURCE_NAME : mainName, main);
        assertNoConflicts(sources);
    }

    /**
     * Validates binary identifier grammar for each source that is a binary descriptor,
     * then runs FQN conflict detection.
     */
    public static void validateAndAssertNoConflicts(Map<String, FileDescriptorProto> sources)
            throws ProtoSchemaValidationException {
        Objects.requireNonNull(sources, "sources");
        for (Map.Entry<String, FileDescriptorProto> entry : sources.entrySet()) {
            BinaryProtobufIdentifierValidator.validate(entry.getKey(), entry.getValue());
        }
        assertNoConflicts(sources);
    }

    private static void walkSource(
            String sourceName, FileDescriptorProto fdp, Map<String, Definition> known)
            throws ProtoSchemaValidationException {
        if (fdp == null) {
            return;
        }
        String pkg = fdp.hasPackage() ? fdp.getPackage() : "";
        for (DescriptorProto message : fdp.getMessageTypeList()) {
            recordMessage(message, pkg, "", sourceName, known);
        }
        for (EnumDescriptorProto enumeration : fdp.getEnumTypeList()) {
            recordDefinition(toFqn(pkg, enumeration.getName()), enumeration, sourceName, known);
        }
        for (ServiceDescriptorProto service : fdp.getServiceList()) {
            recordDefinition(toFqn(pkg, service.getName()), service, sourceName, known);
        }
    }

    private static void recordMessage(
            DescriptorProto message,
            String packageName,
            String scope,
            String sourceName,
            Map<String, Definition> known) throws ProtoSchemaValidationException {
        String scopedName = scope.isEmpty() ? message.getName() : scope + "." + message.getName();
        recordDefinition(toFqn(packageName, scopedName), shallow(message), sourceName, known);
        for (DescriptorProto nested : message.getNestedTypeList()) {
            recordMessage(nested, packageName, scopedName, sourceName, known);
        }
        for (EnumDescriptorProto nestedEnum : message.getEnumTypeList()) {
            recordDefinition(
                    toFqn(packageName, scopedName + "." + nestedEnum.getName()),
                    nestedEnum,
                    sourceName,
                    known);
        }
    }

    private static DescriptorProto shallow(DescriptorProto message) {
        return message.toBuilder().clearNestedType().clearEnumType().build();
    }

    private static void recordDefinition(
            String fqn,
            Message descriptor,
            String sourceName,
            Map<String, Definition> known) throws ProtoSchemaValidationException {
        Definition existing = known.get(fqn);
        if (existing == null) {
            known.put(fqn, new Definition(descriptor, sourceName));
            return;
        }
        if (existing.sourceName.equals(sourceName)) {
            return;
        }
        if (!existing.descriptor.equals(descriptor)) {
            throw new ProtoSchemaValidationException(
                    "Conflicting Protobuf type definition detected for " + fqn
                            + " (first defined in " + existing.sourceName
                            + ", redefined in " + sourceName + ").",
                    fqn,
                    existing.sourceName,
                    sourceName);
        }
    }

    private static String toFqn(String packageName, String scopeName) {
        if (packageName == null || packageName.isEmpty()) {
            return scopeName;
        }
        if (scopeName == null || scopeName.isEmpty()) {
            return packageName;
        }
        return packageName + "." + scopeName;
    }

    private record Definition(Message descriptor, String sourceName) {
    }
}
