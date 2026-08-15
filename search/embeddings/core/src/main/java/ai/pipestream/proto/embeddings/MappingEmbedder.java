package ai.pipestream.proto.embeddings;

import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexMapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Fills the VECTOR field of an engine-neutral mapped document by embedding a TEXT source
 * field, using the shared {@link IndexMapping} to locate and validate both fields.
 *
 * <p>Documents are the {@code Map} form a search-engine mapper produced; field lookups use
 * the mapping's engine-document field names, and the vector is written as a {@code List} of
 * boxed {@code Float}s — the same shape engine mappers emit for a repeated float field.
 * The document map is mutated in place and returned.
 *
 * <p>Field resolution and the dimension check run before the document is consulted, so a
 * misconfigured embedder fails on every call, not only on documents that carry text. A text
 * field that is absent from the document or holds an empty string leaves the document
 * unchanged: there is nothing to embed, and a placeholder vector would poison similarity
 * scores.
 */
public final class MappingEmbedder {

    private final EmbeddingProvider provider;
    private final IndexMapping mapping;

    public MappingEmbedder(EmbeddingProvider provider, IndexMapping mapping) {
        this.provider = Objects.requireNonNull(provider, "provider");
        this.mapping = Objects.requireNonNull(mapping, "mapping");
    }

    /**
     * Embeds the mapping's single TEXT field into its single VECTOR field.
     *
     * @throws IllegalStateException when the mapping does not have exactly one TEXT field and
     *         exactly one VECTOR field, on a provider/hint dimension mismatch, or when the
     *         text field holds a non-String value
     */
    public Map<String, Object> embed(Map<String, Object> document) {
        IndexMapping.IndexedField textField = only(IndexFieldKind.TEXT);
        IndexMapping.IndexedField vectorField = only(IndexFieldKind.VECTOR);
        return embed(document, textField, vectorField);
    }

    /**
     * Embeds the TEXT field named {@code textFieldName} into the VECTOR field named
     * {@code vectorFieldName}. Names are engine-document field names, i.e.
     * {@link IndexMapping.IndexedField#fieldName()}.
     *
     * @throws IllegalArgumentException when either name has no field of the required kind
     *         in the mapping
     * @throws IllegalStateException on a provider/hint dimension mismatch, or when the text
     *         field holds a non-String value
     */
    public Map<String, Object> embed(Map<String, Object> document, String textFieldName, String vectorFieldName) {
        return embed(document,
                named(IndexFieldKind.TEXT, textFieldName),
                named(IndexFieldKind.VECTOR, vectorFieldName));
    }

    private Map<String, Object> embed(
            Map<String, Object> document,
            IndexMapping.IndexedField textField,
            IndexMapping.IndexedField vectorField) {
        Objects.requireNonNull(document, "document");
        checkDimension(vectorField);
        Object value = document.get(textField.fieldName());
        if (value == null) {
            return document; // no text, nothing to embed
        }
        if (!(value instanceof String text)) {
            throw new IllegalStateException("Field '" + textField.fieldName() + "' holds "
                    + value.getClass().getName() + ", not the String a TEXT source requires");
        }
        if (text.isEmpty()) {
            return document;
        }
        float[] embedding = provider.embed(text);
        List<Float> vector = new ArrayList<>(embedding.length);
        for (float component : embedding) {
            vector.add(component);
        }
        document.put(vectorField.fieldName(), vector);
        return document;
    }

    /**
     * {@code vector_dims} of 0 means the hint left the dimension unset (engine default),
     * so only a positive hint is checked against the provider.
     */
    private void checkDimension(IndexMapping.IndexedField vectorField) {
        int dims = vectorField.hint().vectorDims();
        if (dims > 0 && dims != provider.dimension()) {
            throw new IllegalStateException("Provider '" + provider.providerId() + "' produces "
                    + provider.dimension() + "-dimensional vectors, but field '"
                    + vectorField.fieldName() + "' is hinted vector_dims=" + dims);
        }
    }

    private IndexMapping.IndexedField only(IndexFieldKind kind) {
        List<IndexMapping.IndexedField> matches = fields(kind);
        if (matches.size() != 1) {
            String detail = matches.isEmpty()
                    ? "no " + kind + " field"
                    : matches.size() + " " + kind + " fields (" + names(matches) + ")";
            throw new IllegalStateException("Mapping for " + mapping.messageFullName() + " has "
                    + detail + "; name the fields explicitly with"
                    + " embed(document, textFieldName, vectorFieldName)");
        }
        return matches.get(0);
    }

    private IndexMapping.IndexedField named(IndexFieldKind kind, String fieldName) {
        Objects.requireNonNull(fieldName, "fieldName");
        List<IndexMapping.IndexedField> matches = fields(kind);
        return matches.stream()
                .filter(field -> field.fieldName().equals(fieldName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No " + kind + " field named '"
                        + fieldName + "' in the mapping for " + mapping.messageFullName()
                        + (matches.isEmpty()
                                ? "; the mapping has no " + kind + " fields"
                                : "; " + kind + " fields: " + names(matches))));
    }

    private List<IndexMapping.IndexedField> fields(IndexFieldKind kind) {
        return mapping.indexable().stream().filter(field -> field.type() == kind).toList();
    }

    private static String names(List<IndexMapping.IndexedField> fields) {
        return fields.stream().map(IndexMapping.IndexedField::fieldName).collect(Collectors.joining(", "));
    }
}
