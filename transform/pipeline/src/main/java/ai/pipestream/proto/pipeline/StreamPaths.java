package ai.pipestream.proto.pipeline;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

/**
 * Resolves dotted repeated-field paths (unnest paths, fan-out items paths) against a
 * message type. The walk mirrors the workflow's edge flow exactly: every intermediate segment
 * must be a singular message field and the final segment a repeated message field, so a
 * malformed path fails identically in workflows and pipelines.
 */
final class StreamPaths {

    private StreamPaths() {
    }

    /**
     * Resolves {@code path} against {@code type}.
     *
     * @return the repeated message field the path ends at
     * @throws IllegalArgumentException naming the segment that breaks the walk
     */
    static FieldDescriptor repeatedField(Descriptor type, String path) {
        String[] segments = path.split("\\.");
        Descriptor current = type;
        for (int i = 0; i < segments.length; i++) {
            FieldDescriptor field = current.findFieldByName(segments[i]);
            if (field == null) {
                throw new IllegalArgumentException("path '" + path + "' does not resolve: "
                        + current.getFullName() + " has no field '" + segments[i] + "'");
            }
            boolean last = i == segments.length - 1;
            if (last) {
                if (!field.isRepeated()) {
                    throw new IllegalArgumentException("path '" + path
                            + "' ends at a non-repeated field");
                }
                if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                    throw new IllegalArgumentException("path '" + path
                            + "' ends at a non-message field; streams carry messages");
                }
                return field;
            }
            if (field.isRepeated()
                    || field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                throw new IllegalArgumentException("path '" + path + "' walks through '"
                        + segments[i] + "', which is not a singular message field");
            }
            current = field.getMessageType();
        }
        throw new IllegalArgumentException("path must not be empty");
    }

    /** The element type of the repeated message field {@code path} ends at. */
    static Descriptor elementType(Descriptor type, String path) {
        return repeatedField(type, path).getMessageType();
    }
}
