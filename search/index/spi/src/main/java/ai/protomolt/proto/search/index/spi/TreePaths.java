package ai.protomolt.proto.search.index.spi;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.MessageOrBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The taxonomy-path shape behind {@link IndexFieldKind#TREE_PATH}.
 *
 * <p>The canonical type is {@code ai.pipestream.proto.types.v1.TreePath}, recognized by
 * name so a field of that type indexes as a tree path with no hint at all. Any other
 * message duck-types by declaring a repeated string field named {@code segments}.
 *
 * <p>Emission is the ancestor chain: segments {@code [a, b, c]} become the terms
 * {@code "a"}, {@code "a/b"}, {@code "a/b/c"}. Every level is a keyword term, so
 * drill-down faceting counts at any depth and a path-prefix filter is an exact match
 * on the prefix's own term. The chain is inherently multi-valued, which is why engines
 * treat a TREE_PATH field as multi-valued even when the proto field is singular.
 */
public final class TreePaths {

    /** The canonical taxonomy path type, recognized by name. */
    public static final String CANONICAL = "ai.pipestream.proto.types.v1.TreePath";

    /** Delimiter joining segments in rendered terms; segments must not contain it. */
    public static final String DELIMITER = "/";

    private TreePaths() {
    }

    /** Whether this message is the canonical types.v1 TreePath. */
    public static boolean isCanonical(Descriptor message) {
        return CANONICAL.equals(message.getFullName());
    }

    /** The repeated string {@code segments} field, when the message has the shape. */
    public static Optional<FieldDescriptor> resolve(Descriptor message) {
        FieldDescriptor segments = message.findFieldByName("segments");
        if (segments == null
                || !segments.isRepeated()
                || segments.getJavaType() != FieldDescriptor.JavaType.STRING) {
            return Optional.empty();
        }
        return Optional.of(segments);
    }

    /**
     * The ancestor chain of one path value, root first: {@code ["a", "a/b", "a/b/c"]}.
     * Empty when the value has no segments (nothing to index).
     */
    public static List<String> ancestorPaths(MessageOrBuilder path, FieldDescriptor segments) {
        int count = path.getRepeatedFieldCount(segments);
        List<String> ancestors = new ArrayList<>(count);
        StringBuilder joined = new StringBuilder();
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                joined.append(DELIMITER);
            }
            joined.append((String) path.getRepeatedField(segments, i));
            ancestors.add(joined.toString());
        }
        return ancestors;
    }
}
