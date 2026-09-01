package ai.pipestream.proto.search.embedding;

import ai.pipestream.proto.search.index.spi.IndexFieldKind;
import ai.pipestream.proto.search.index.spi.IndexMapping;

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
 * <p>Field resolution, the dimension check, and the {@link VectorizationPolicy} check all
 * run before the document is consulted, so a misconfigured embedder fails on every call,
 * not only on documents that carry text. A text field that is absent from the document or
 * holds an empty string leaves the document unchanged: there is nothing to embed, and a
 * placeholder vector would poison similarity scores.
 *
 * <p>This is the only path from a mapped document to an embedding provider, which is why
 * the sensitivity gate lives here: a TEXT field carrying a {@code meta.v1} sensitivity
 * class is refused unless the deployment's policy names that class.
 */
public final class MappingEmbedder {

    private final EmbeddingProvider provider;
    private final IndexMapping mapping;
    private final VectorizationPolicy policy;

    /** An embedder that may vectorize unclassified content only. */
    public MappingEmbedder(EmbeddingProvider provider, IndexMapping mapping) {
        this(provider, mapping, VectorizationPolicy.unclassifiedOnly());
    }

    /**
     * An embedder governed by {@code policy}.
     *
     * @param provider the embedding provider
     * @param mapping the index mapping locating the TEXT source and VECTOR target
     * @param policy which sensitivity classes may reach the provider
     */
    public MappingEmbedder(EmbeddingProvider provider, IndexMapping mapping,
                           VectorizationPolicy policy) {
        this.provider = Objects.requireNonNull(provider, "provider");
        this.mapping = Objects.requireNonNull(mapping, "mapping");
        this.policy = Objects.requireNonNull(policy, "policy");
    }

    /**
     * Embeds the mapping's single TEXT field into its single VECTOR field.
     *
     * @throws IllegalStateException when the mapping does not have exactly one TEXT field and
     *         exactly one VECTOR field, on a provider/hint dimension mismatch, when the text
     *         field holds a non-String value, or when the provider returns a vector that is
     *         null or not its declared dimension
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
     * @throws IllegalStateException on a provider/hint dimension mismatch, when the text
     *         field holds a non-String value, or when the provider returns a vector that is
     *         null or not its declared dimension
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
        checkSensitivity(textField);
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
        if (embedding == null || embedding.length != provider.dimension()) {
            throw new IllegalStateException("Provider '" + provider.providerId() + "' returned "
                    + (embedding == null ? "null" : embedding.length + " components")
                    + " for a " + provider.dimension() + "-dimensional provider");
        }
        List<Float> vector = new ArrayList<>(embedding.length);
        for (float component : embedding) {
            vector.add(component);
        }
        document.put(vectorField.fieldName(), vector);
        return document;
    }

    /**
     * Refuses to send a classified source field to the provider unless the policy names
     * its class. The refusal names the field and the class, never the content.
     */
    private void checkSensitivity(IndexMapping.IndexedField textField) {
        if (policy.permits(textField.sensitivity())) {
            return;
        }
        throw new IllegalStateException("Field '" + textField.fieldName()
                + "' is classified '" + textField.sensitivity() + "' and this embedder"
                + " vectorizes " + (policy.permitted().isEmpty()
                        ? "unclassified content only"
                        : "unclassified content and " + policy.permitted())
                + "; permit the class explicitly with VectorizationPolicy.permitting(...)"
                + " or mask the field before it reaches the embedder");
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
        return matches.getFirst();
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
