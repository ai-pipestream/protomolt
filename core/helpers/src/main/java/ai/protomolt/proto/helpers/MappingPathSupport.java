package ai.protomolt.proto.helpers;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Package-private field path discovery and dot-path validation support for {@link MappingHelper}.
 */
final class MappingPathSupport {

    private static final String GOOGLE_PROTOBUF_PREFIX = "google.protobuf.";
    private static final Set<String> TERMINAL_TYPES = Set.of(
            "google.protobuf.Struct",
            "google.protobuf.Value",
            "google.protobuf.Any",
            "google.protobuf.Timestamp",
            "google.protobuf.Duration"
    );

    private MappingPathSupport() {}

    /**
     * Collects and sorts all reachable field paths up to {@code maxDepth}.
     */
    static List<String> getAllFieldPaths(Descriptor descriptor, int maxDepth) {
        List<String> paths = new ArrayList<>();
        collectFieldPaths(descriptor, "", 0, maxDepth, paths, new HashSet<>());
        Collections.sort(paths);
        return paths;
    }

    /**
     * Collects field metadata in descriptor declaration order up to {@code maxDepth}.
     */
    static List<MappingHelper.FieldInfo> getFieldInfos(Descriptor descriptor, int maxDepth) {
        List<MappingHelper.FieldInfo> infos = new ArrayList<>();
        collectFieldInfos(descriptor, "", 0, maxDepth, infos, new HashSet<>());
        return infos;
    }

    /**
     * Validates a dot-separated field path against {@code descriptor}.
     */
    static PathValidation validatePath(String path, Descriptor descriptor) {
        try {
            List<String> parts = splitDotSegments(path);
            if (parts == null) {
                return new PathValidation(false, "Path '" + path + "' contains an empty segment", null);
            }

            Descriptor currentDescriptor = descriptor;
            FieldDescriptor lastField = null;

            for (int i = 0; i < parts.size(); i++) {
                String part = parts.get(i);
                FieldDescriptor field = currentDescriptor.findFieldByName(part);

                if (field == null) {
                    return new PathValidation(
                            false,
                            "Field '" + part + "' not found in " + currentDescriptor.getName(),
                            null);
                }

                lastField = field;

                if (i < parts.size() - 1) {
                    // Not the last part, must be a singular message to continue
                    if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                        return new PathValidation(
                                false,
                                "Field '" + part + "' is not a message type",
                                null);
                    }
                    if (field.isRepeated()) {
                        return new PathValidation(
                                false,
                                "Cannot traverse through repeated field '" + part + "'",
                                null);
                    }
                    currentDescriptor = field.getMessageType();
                }
            }

            return new PathValidation(true, null, lastField);
        } catch (Exception e) {
            return new PathValidation(false, e.getMessage(), null);
        }
    }

    /**
     * Returns true when {@code descriptor} is a well-known terminal protobuf message whose
     * internal fields should not be expanded during UI field discovery or schema export.
     */
    static boolean isTerminalType(Descriptor descriptor) {
        String fullName = descriptor.getFullName();
        return fullName.startsWith(GOOGLE_PROTOBUF_PREFIX) && TERMINAL_TYPES.contains(fullName);
    }

    /**
     * Splits {@code path} on {@code '.'} while preserving leading, trailing, and consecutive
     * empty segments, returning {@code null} if any segment is empty.
     */
    private static List<String> splitDotSegments(String path) {
        if (path.isEmpty()) {
            return null;
        }
        List<String> parts = new ArrayList<>();
        int start = 0;
        while (true) {
            int dot = path.indexOf('.', start);
            String part = dot < 0 ? path.substring(start) : path.substring(start, dot);
            if (part.isEmpty()) {
                return null;
            }
            parts.add(part);
            if (dot < 0) {
                return parts;
            }
            start = dot + 1;
        }
    }

    private static void collectFieldPaths(
            Descriptor descriptor,
            String prefix,
            int depth,
            int maxDepth,
            List<String> paths,
            Set<String> visited) {

        if (depth > maxDepth || visited.contains(descriptor.getFullName())) {
            return;
        }
        visited.add(descriptor.getFullName());

        for (FieldDescriptor field : descriptor.getFields()) {
            String fieldPath = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
            paths.add(fieldPath);

            if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE && !field.isRepeated()) {
                Descriptor fieldType = field.getMessageType();
                if (!isTerminalType(fieldType)) {
                    collectFieldPaths(fieldType, fieldPath, depth + 1, maxDepth, paths, new HashSet<>(visited));
                }
            }
        }
    }

    private static void collectFieldInfos(
            Descriptor descriptor,
            String prefix,
            int depth,
            int maxDepth,
            List<MappingHelper.FieldInfo> infos,
            Set<String> visited) {

        if (depth > maxDepth || visited.contains(descriptor.getFullName())) {
            return;
        }
        visited.add(descriptor.getFullName());

        for (FieldDescriptor field : descriptor.getFields()) {
            String fieldPath = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();

            MappingHelper.FieldInfo info = new MappingHelper.FieldInfo(
                    fieldPath,
                    field.getName(),
                    field.getJavaType().name(),
                    field.isRepeated(),
                    field.isRequired(),
                    field.hasDefaultValue(),
                    depth
            );
            infos.add(info);

            if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE && !field.isRepeated()) {
                Descriptor fieldType = field.getMessageType();
                if (!isTerminalType(fieldType)) {
                    collectFieldInfos(fieldType, fieldPath, depth + 1, maxDepth, infos, new HashSet<>(visited));
                }
            }
        }
    }

    /**
     * Result of validating a dot-separated field path against a message descriptor.
     */
    record PathValidation(boolean isValid, String error, FieldDescriptor fieldDescriptor) {}
}
