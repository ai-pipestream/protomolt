package ai.pipestream.proto.search.index.spi;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Engine-agnostic index mapping derived from a message descriptor + hints.
 * Lucene / OpenSearch / Solr plugins interpret {@link IndexFieldKind}; NDJSON ignores this.
 */
public record IndexMapping(String messageFullName, List<IndexedField> fields) {
    public IndexMapping {
        Objects.requireNonNull(messageFullName, "messageFullName");
        fields = List.copyOf(Objects.requireNonNull(fields, "fields"));
    }

    public Optional<IndexedField> find(String path) {
        return fields.stream().filter(f -> f.path().equals(path)).findFirst();
    }

    public List<IndexedField> indexable() {
        return fields.stream().filter(f -> !f.hint().isSkip()).toList();
    }

    /**
     * @param path protobuf path (dot-separated)
     * @param fieldName name written to the engine document
     * @param hint resolved indexing hint
     * @param repeated whether the protobuf field is repeated (map fields included). Engines
     *        with a declared schema reject a second value for a field declared singular, so
     *        they need this to size the field they create.
     * @param sensitivity the field's declared {@code meta.v1} sensitivity class, or the
     *        empty string when it declares none. Carried here because the mapping is the
     *        descriptor-derived contract the whole search path already reads, and because
     *        consumers downstream of the descriptor — the embedder above all — must be able
     *        to refuse restricted content without re-resolving the path.
     */
    public record IndexedField(String path, String fieldName, ResolvedFieldHint hint,
                               boolean repeated, String sensitivity) {
        public IndexedField {
            Objects.requireNonNull(path, "path");
            Objects.requireNonNull(fieldName, "fieldName");
            Objects.requireNonNull(hint, "hint");
            sensitivity = sensitivity == null ? "" : sensitivity;
        }

        /** Field with no declared sensitivity. */
        public IndexedField(String path, String fieldName, ResolvedFieldHint hint,
                            boolean repeated) {
            this(path, fieldName, hint, repeated, "");
        }

        /** Singular field with no declared sensitivity. */
        public IndexedField(String path, String fieldName, ResolvedFieldHint hint) {
            this(path, fieldName, hint, false, "");
        }

        /** Whether this field declares a sensitivity class. */
        public boolean classified() {
            return !sensitivity.isEmpty();
        }

        public IndexFieldKind type() {
            return hint.type();
        }

        public boolean stored() {
            return hint.stored();
        }

        public boolean indexed() {
            return hint.indexed();
        }
    }
}
