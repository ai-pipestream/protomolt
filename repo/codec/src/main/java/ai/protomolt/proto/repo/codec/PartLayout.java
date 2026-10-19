package ai.protomolt.proto.repo.codec;

import ai.protomolt.proto.repo.v1.DocumentPart;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.ArrayList;
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
 *       field's value (e.g. {@code result_id});</li>
 *   <li>zero or more <b>keyed parts</b> — a repeated message field nested one
 *       level down (e.g. {@code search_metadata.segmentations}) whose every
 *       element is its own fragment, identified by the element's key fields
 *       (e.g. {@code segmentation_id}). Keys are unique within a document.</li>
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

    /**
     * A repeated message field (nested one level below a parent message field)
     * whose every element is one fragment, identified by the element's key
     * fields in declaration order.
     */
    public record KeyedField(DocumentPart part,
                             Descriptors.FieldDescriptor parent,
                             Descriptors.FieldDescriptor repeatedField,
                             List<Descriptors.FieldDescriptor> keyFields) {
        public KeyedField {
            keyFields = List.copyOf(keyFields);
            if (keyFields.isEmpty()) throw new IllegalArgumentException("A keyed part names at least one key field");
        }

        /** The element's key values, as strings, in key-field order. */
        public List<String> keyOf(Message element) {
            var values = new ArrayList<String>(keyFields.size());
            for (var key : keyFields) values.add(element.getField(key).toString());
            return List.copyOf(values);
        }
    }

    private final Descriptors.Descriptor messageType;
    private final Descriptors.FieldDescriptor identityField;
    private final List<PartField> partFields;
    private final List<ChunkedField> chunkedFields;
    private final List<KeyedField> keyedFields;

    private PartLayout(Descriptors.Descriptor messageType,
                       Descriptors.FieldDescriptor identityField,
                       List<PartField> partFields,
                       List<ChunkedField> chunkedFields,
                       List<KeyedField> keyedFields) {
        this.messageType = messageType;
        this.identityField = identityField;
        this.partFields = List.copyOf(partFields);
        this.chunkedFields = List.copyOf(chunkedFields);
        this.keyedFields = List.copyOf(keyedFields);
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

    /** The keyed part mappings, in declaration order. */
    public List<KeyedField> keyedFields() {
        return keyedFields;
    }

    /** Builder for {@link PartLayout}; resolves field names at build time. */
    public static final class Builder {

        private final Descriptors.Descriptor messageType;
        private Descriptors.FieldDescriptor identityField;
        private final List<PartField> partFields = new ArrayList<>();
        private final List<ChunkedField> chunkedFields = new ArrayList<>();
        private final List<KeyedField> keyedFields = new ArrayList<>();

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

        /**
         * Maps a repeated message field nested one level down (e.g.
         * {@code "search_metadata.segmentations"}) to a keyed part: every
         * element is its own fragment, identified by the element's
         * {@code keyFieldNames} (e.g. {@code "segmentation_id"}), which must
         * be scalar fields.
         */
        public Builder keyedPart(DocumentPart part, String repeatedFieldPath, String... keyFieldNames) {
            String[] segments = repeatedFieldPath.split("\\.");
            if (segments.length != 2) {
                throw new IllegalArgumentException(
                        "keyedPart takes a 'parent.repeated' path, got '" + repeatedFieldPath + "'");
            }
            Descriptors.FieldDescriptor parent = requireField(messageType, segments[0]);
            if (parent.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE || parent.isRepeated()) {
                throw new IllegalArgumentException(
                        "keyedPart parent '" + segments[0] + "' must be a singular message field");
            }
            Descriptors.FieldDescriptor repeated = requireField(parent.getMessageType(), segments[1]);
            if (!repeated.isRepeated()
                    || repeated.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                throw new IllegalArgumentException(
                        "keyedPart field '" + repeatedFieldPath + "' must be a repeated message field");
            }
            var keys = new ArrayList<Descriptors.FieldDescriptor>();
            for (String name : keyFieldNames) {
                Descriptors.FieldDescriptor key = requireField(repeated.getMessageType(), name);
                if (key.isRepeated() || key.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                    throw new IllegalArgumentException(
                            "keyedPart key '" + name + "' on " + repeatedFieldPath + " must be a scalar field");
                }
                keys.add(key);
            }
            keyedFields.add(new KeyedField(part, parent, repeated, keys));
            return this;
        }

        /** Builds the immutable layout, validating all field references. */
        public PartLayout build() {
            return new PartLayout(messageType, identityField,
                    List.copyOf(partFields), List.copyOf(chunkedFields), List.copyOf(keyedFields));
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
