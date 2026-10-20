package ai.protomolt.proto.helpers;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

import java.util.HashSet;
import java.util.Set;

/**
 * Package-private schema tree exporter for {@link MappingHelper}.
 */
final class MappingSchemaExporter {

    private static final String MESSAGE_NODE_TYPE = "MESSAGE";

    private MappingSchemaExporter() {}

    /**
     * Exports {@code descriptor} as a hierarchical {@link MappingHelper.SchemaNode} tree up to
     * {@code maxDepth}.
     */
    static MappingHelper.SchemaNode exportSchema(Descriptor descriptor, int maxDepth) {
        return buildSchemaNode(descriptor, "", 0, maxDepth, new HashSet<>());
    }

    private static MappingHelper.SchemaNode buildSchemaNode(
            Descriptor descriptor,
            String path,
            int depth,
            int maxDepth,
            Set<String> visited) {

        int lastDot = path.lastIndexOf('.');
        String fieldName = path.isEmpty()
                ? descriptor.getName()
                : (lastDot >= 0 ? path.substring(lastDot + 1) : path);
        MappingHelper.SchemaNode node = new MappingHelper.SchemaNode(fieldName, path, MESSAGE_NODE_TYPE);

        if (depth > maxDepth || visited.contains(descriptor.getFullName())) {
            return node;
        }
        visited.add(descriptor.getFullName());

        for (FieldDescriptor field : descriptor.getFields()) {
            String fieldPath = path.isEmpty() ? field.getName() : path + "." + field.getName();

            if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE && !field.isRepeated()) {
                Descriptor fieldType = field.getMessageType();
                if (!MappingPathSupport.isTerminalType(fieldType)) {
                    MappingHelper.SchemaNode childNode = buildSchemaNode(
                            fieldType,
                            fieldPath,
                            depth + 1,
                            maxDepth,
                            new HashSet<>(visited)
                    );
                    node.children.add(childNode);
                } else {
                    node.children.add(new MappingHelper.SchemaNode(
                            field.getName(),
                            fieldPath,
                            field.getJavaType().name(),
                            field.isRepeated()
                    ));
                }
            } else {
                node.children.add(new MappingHelper.SchemaNode(
                        field.getName(),
                        fieldPath,
                        field.getJavaType().name(),
                        field.isRepeated()
                ));
            }
        }

        return node;
    }
}
