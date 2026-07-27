package ai.pipestream.proto.repo.container.codec;

import ai.pipestream.proto.repo.v1.DocumentPart;
import com.google.protobuf.Descriptors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Descriptor-driven mapping from a protobuf message type to its addressable
 * storage parts (docs: four-part addressable document model).
 *
 * <p>A layout declares, over the message's {@link Descriptors.Descriptor}:
 * <ul>
 *   <li>an optional <b>identity field</b> (e.g. {@code doc_id}) that every
 *       split fragment carries for self-description;</li>
 *   <li>zero or more <b>part fields</b> — top-level fields hoisted into their
 *       own part object (e.g. {@code blob_bag} → BLOBS);</li>
 *   <li>zero or more <b>chunked parts</b> — a repeated message field nested
 *       one level down (e.g. {@code search_metadata.semantic_results}) that is
 *       split into one fragment per consecutive run of elements sharing a key
 *       field's value (e.g. {@code result_id}).</li>
 * </ul>
 *
 * <p>CORE is implicit: every field NOT mapped to another part lands in CORE.
 * Layouts are immutable once built; field names are resolved against the
 * descriptor at {@link Builder#build()} time, so a typo fails fast.
 */
public final class PartLayout {

    /** A top-level field hoisted into its own part object. */
    public record PartField(DocumentPart part, Descriptors.FieldDescriptor field) {
    }

    /**
     * A repeated message field (nested one level below a parent message field)
     * split into one fragment per consecutive run of equal key-field values.
     */
    public record ChunkedField(DocumentPart part,
                               Descriptors.FieldDescriptor parent,
                               Descriptors.FieldDescriptor repeatedField,
                               Descriptors.FieldDescriptor keyField) {
    }

    private final Descriptors.Descriptor messageType;
    private final Descriptors.FieldDescriptor identityField;
    private final List<PartField> partFields;
    private final List<ChunkedField> chunkedFields;

    private PartLayout(Descriptors.Descriptor messageType,
                       Descriptors.FieldDescriptor identityField,
                       List<PartField> partFields,
                       List<ChunkedField> chunkedFields) {
        this.messageType = messageType;
        this.identityField = identityField;
        this.partFields = List.copyOf(partFields);
        this.chunkedFields = List.copyOf(chunkedFields);
    }

    /**
     * Starts a layout over the given message type.
     *
     * @param messageType the descriptor of the message type to split
     * @return a new builder
     */
    public static Builder builder(Descriptors.Descriptor messageType) {
        return new Builder(messageType);
    }

    /** The message type this layout splits. */
    public Descriptors.Descriptor messageType() {
        return messageType;
    }

    /** The identity field carried by every fragment, or {@code null} if none. */
    public Descriptors.FieldDescriptor identityField() {
        return identityField;
    }

    /** The single-field part mappings, in declaration order. */
    public List<PartField> partFields() {
        return partFields;
    }

    /** The chunked part mappings, in declaration order. */
    public List<ChunkedField> chunkedFields() {
        return chunkedFields;
    }

    /** Builder for {@link PartLayout}; resolves field names at build time. */
    public static final class Builder {

        private final Descriptors.Descriptor messageType;
        private Descriptors.FieldDescriptor identityField;
        private final List<PartField> partFields = new ArrayList<>();
        private final List<ChunkedField> chunkedFields = new ArrayList<>();

        private Builder(Descriptors.Descriptor messageType) {
            this.messageType = messageType;
        }

        /**
         * Declares the identity field (e.g. {@code "doc_id"}) copied into
         * every fragment.
         */
        public Builder identityField(String fieldName) {
            this.identityField = requireField(messageType, fieldName);
            return this;
        }

        /**
         * Maps a top-level field (e.g. {@code "blob_bag"}) to its own part
         * object.
         */
        public Builder partField(DocumentPart part, String fieldPath) {
            if (fieldPath.contains(".")) {
                throw new IllegalArgumentException(
                        "partField takes a top-level field name, got path '" + fieldPath + "'");
            }
            partFields.add(new PartField(part, requireField(messageType, fieldPath)));
            return this;
        }

        /**
         * Maps a repeated message field nested one level down (e.g.
         * {@code "search_metadata.semantic_results"}) to a chunked part whose
         * fragments are keyed by the elements' {@code keyFieldName} (e.g.
         * {@code "result_id"}).
         */
        public Builder chunkedPart(DocumentPart part, String repeatedFieldPath, String keyFieldName) {
            String[] segments = repeatedFieldPath.split("\\.");
            if (segments.length != 2) {
                throw new IllegalArgumentException(
                        "chunkedPart takes a 'parent.repeated' path, got '" + repeatedFieldPath + "'");
            }
            Descriptors.FieldDescriptor parent = requireField(messageType, segments[0]);
            if (parent.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE || parent.isRepeated()) {
                throw new IllegalArgumentException(
                        "chunkedPart parent '" + segments[0] + "' must be a singular message field");
            }
            Descriptors.FieldDescriptor repeated = requireField(parent.getMessageType(), segments[1]);
            if (!repeated.isRepeated()
                    || repeated.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                throw new IllegalArgumentException(
                        "chunkedPart field '" + repeatedFieldPath + "' must be a repeated message field");
            }
            Descriptors.FieldDescriptor key = requireField(repeated.getMessageType(), keyFieldName);
            chunkedFields.add(new ChunkedField(part, parent, repeated, key));
            return this;
        }

        /** Builds the immutable layout, validating all field references. */
        public PartLayout build() {
            return new PartLayout(messageType, identityField,
                    Collections.unmodifiableList(new ArrayList<>(partFields)),
                    Collections.unmodifiableList(new ArrayList<>(chunkedFields)));
        }

        private static Descriptors.FieldDescriptor requireField(Descriptors.Descriptor type, String name) {
            Descriptors.FieldDescriptor fd = type.findFieldByName(name);
            if (fd == null) {
                throw new IllegalArgumentException(
                        "No field '" + name + "' on message type " + type.getFullName());
            }
            return fd;
        }
    }
}
