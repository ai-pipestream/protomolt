package ai.protomolt.proto.pipeline;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;

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
        List<String> segments = segments(path);
        Descriptor current = type;
        for (int i = 0; i < segments.size(); i++) {
            FieldDescriptor field = current.findFieldByName(segments.get(i));
            if (field == null) {
                throw new IllegalArgumentException("path '" + path + "' does not resolve: "
                        + current.getFullName() + " has no field '" + segments.get(i) + "'");
            }
            boolean last = i == segments.size() - 1;
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
                        + segments.get(i) + "', which is not a singular message field");
            }
            current = field.getMessageType();
        }
        throw new IllegalArgumentException("path must not be empty");
    }

    /** The element type of the repeated message field {@code path} ends at. */
    static Descriptor elementType(Descriptor type, String path) {
        return repeatedField(type, path).getMessageType();
    }

    /**
     * Reads the repeated field {@code path} ends at from one message value. The path is
     * resolved with {@link #repeatedField} first, so a value walk only ever follows singular
     * message fields that the descriptor walk already accepted.
     *
     * @return the repeated field's elements, in field order
     * @throws IllegalArgumentException when the path does not resolve against the value's type
     */
    static List<?> repeatedValues(Message holder, String path) {
        FieldDescriptor field = repeatedField(holder.getDescriptorForType(), path);
        List<String> segments = segments(path);
        Message current = holder;
        for (int i = 0; i < segments.size() - 1; i++) {
            current = (Message) current.getField(
                    current.getDescriptorForType().findFieldByName(segments.get(i)));
        }
        return (List<?>) current.getField(field);
    }

    /**
     * Splits a dotted path on {@code '.'} with the segment rules the path walk has always
     * applied: a path without a dot is its own single segment (so the empty path is one empty
     * segment), interior and leading empty segments are kept, and trailing empty segments are
     * dropped.
     *
     * @return the path's segments, in order
     */
    static List<String> segments(String path) {
        List<String> segments = new ArrayList<>();
        int start = 0;
        int dot = path.indexOf('.');
        if (dot < 0) {
            segments.add(path);
            return segments;
        }
        while (dot >= 0) {
            segments.add(path.substring(start, dot));
            start = dot + 1;
            dot = path.indexOf('.', start);
        }
        segments.add(path.substring(start));
        while (!segments.isEmpty() && segments.getLast().isEmpty()) {
            segments.removeLast();
        }
        return segments;
    }
}
