package ai.pipestream.proto.search.embedding;

import ai.pipestream.proto.search.index.spi.IndexFieldKind;
import ai.pipestream.proto.search.index.spi.IndexMapping;
import ai.pipestream.proto.search.index.spi.ResolvedFieldHint;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MappingEmbedderTest {

    private final EmbeddingProvider provider = new FixedTableEmbeddingProvider();

    private static IndexMapping.IndexedField text(String name) {
        return new IndexMapping.IndexedField(name, name, ResolvedFieldHint.of(IndexFieldKind.TEXT));
    }

    private static IndexMapping.IndexedField vector(String name, int dims) {
        return new IndexMapping.IndexedField(name, name,
                ResolvedFieldHint.builder(IndexFieldKind.VECTOR).vectorDims(dims).build(), true);
    }

    /** One TEXT field, one VECTOR field: the shape the no-args selection requires. */
    private static IndexMapping singleTextMapping(int dims) {
        return new IndexMapping("library.Book", List.of(
                text("title"),
                new IndexMapping.IndexedField("genre", "genre", ResolvedFieldHint.of(IndexFieldKind.KEYWORD)),
                vector("embedding", dims)));
    }

    private static IndexMapping twoTextMapping() {
        return new IndexMapping("library.Book", List.of(
                text("title"),
                text("body"),
                vector("embedding", 3)));
    }

    @Test
    void autoSelectionEmbedsTheOnlyTextFieldIntoTheOnlyVectorField() {
        Map<String, Object> document = new LinkedHashMap<>(Map.of("title", "hello world", "genre", "memoir"));

        Map<String, Object> embedded = new MappingEmbedder(provider, singleTextMapping(3)).embed(document);

        assertThat(embedded).isSameAs(document);
        assertThat(embedded.get("embedding")).isEqualTo(List.of(0.1f, 0.2f, 0.3f));
    }

    @Test
    void explicitFieldNamesPickTheSourceFromAMappingWithSeveralTextFields() {
        Map<String, Object> document = new LinkedHashMap<>(Map.of("title", "hello world", "body", "a memoir"));

        new MappingEmbedder(provider, twoTextMapping()).embed(document, "body", "embedding");

        assertThat(document.get("embedding")).isEqualTo(List.of(0.4f, 0.5f, 0.6f));
    }

    @Test
    void dimensionMismatchAgainstTheVectorDimsHintFails() {
        MappingEmbedder embedder = new MappingEmbedder(provider, singleTextMapping(4));
        Map<String, Object> document = new LinkedHashMap<>(Map.of("title", "hello world"));

        assertThatThrownBy(() -> embedder.embed(document))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Provider 'fixed-table' produces 3-dimensional vectors,"
                        + " but field 'embedding' is hinted vector_dims=4");
    }

    @Test
    void autoSelectionRejectsAMappingWithMoreThanOneTextField() {
        MappingEmbedder embedder = new MappingEmbedder(provider, twoTextMapping());
        Map<String, Object> document = new LinkedHashMap<>(Map.of("title", "hello world"));

        assertThatThrownBy(() -> embedder.embed(document))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Mapping for library.Book has 2 TEXT fields (title, body); name the"
                        + " fields explicitly with embed(document, textFieldName, vectorFieldName)");
    }

    @Test
    void absentTextFieldLeavesTheDocumentUnchanged() {
        Map<String, Object> document = new LinkedHashMap<>(Map.of("genre", "memoir"));

        Map<String, Object> embedded = new MappingEmbedder(provider, singleTextMapping(3)).embed(document);

        assertThat(embedded).isSameAs(document);
        assertThat(embedded).containsOnlyKeys("genre");
    }

    @Test
    void emptyTextFieldLeavesTheDocumentUnchanged() {
        Map<String, Object> document = new LinkedHashMap<>(Map.of("title", ""));

        assertThat(new MappingEmbedder(provider, singleTextMapping(3)).embed(document))
                .containsOnlyKeys("title");
    }

    /**
     * A provider whose vectors do not match its declared dimension is broken;
     * writing such a vector would poison the index, so the embedder refuses it
     * even when the field hint leaves the dimension unset. Declares 3, returns 4.
     */
    @Test
    void aVectorLongerOrShorterThanTheDeclaredDimensionFails() {
        EmbeddingProvider lying = new EmbeddingProvider() {
            @Override
            public String providerId() {
                return "lying";
            }

            @Override
            public int dimension() {
                return 3;
            }

            @Override
            public float[] embed(String text) {
                return new float[] {0.1f, 0.2f, 0.3f, 0.4f};
            }
        };
        Map<String, Object> document = new LinkedHashMap<>(Map.of("title", "hello world"));

        assertThatThrownBy(() -> new MappingEmbedder(lying, singleTextMapping(3)).embed(document))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("lying")
                .hasMessageContaining("3");
        assertThat(document).containsOnlyKeys("title");
    }

    @Test
    void aNullVectorFailsInsteadOfThrowingNpe() {
        EmbeddingProvider nullReturning = new EmbeddingProvider() {
            @Override
            public String providerId() {
                return "null-returning";
            }

            @Override
            public int dimension() {
                return 3;
            }

            @Override
            public float[] embed(String text) {
                return null;
            }
        };
        Map<String, Object> document = new LinkedHashMap<>(Map.of("title", "hello world"));

        assertThatThrownBy(() -> new MappingEmbedder(nullReturning, singleTextMapping(3)).embed(document))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("null-returning");
        assertThat(document).containsOnlyKeys("title");
    }

    private static IndexMapping classifiedTextMapping(String sensitivity) {
        return new IndexMapping("library.Book", List.of(
                new IndexMapping.IndexedField("title", "title",
                        ResolvedFieldHint.of(IndexFieldKind.TEXT), false, sensitivity),
                vector("embedding", 3)));
    }

    @Test
    void classifiedTextIsRefusedByDefaultNamingTheFieldAndClass() {
        Map<String, Object> document = new LinkedHashMap<>(Map.of("title", "hello world"));

        assertThatThrownBy(() ->
                new MappingEmbedder(provider, classifiedTextMapping("pii")).embed(document))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("title")
                .hasMessageContaining("pii")
                .hasMessageContaining("unclassified content only");
        assertThat(document).doesNotContainKey("embedding");
    }

    @Test
    void theRefusalNeverEchoesTheContent() {
        Map<String, Object> document = new LinkedHashMap<>(Map.of("title", "sekret-value"));

        assertThatThrownBy(() ->
                new MappingEmbedder(provider, classifiedTextMapping("pii")).embed(document))
                .hasMessageNotContaining("sekret-value");
    }

    @Test
    void aPermittedClassEmbedsVerbatim() {
        Map<String, Object> document = new LinkedHashMap<>(Map.of("title", "hello world"));

        Map<String, Object> embedded = new MappingEmbedder(provider,
                classifiedTextMapping("internal"),
                VectorizationPolicy.permitting(java.util.Set.of("internal"))).embed(document);

        assertThat(embedded.get("embedding")).isEqualTo(List.of(0.1f, 0.2f, 0.3f));
    }

    @Test
    void aPermittedClassDoesNotPermitADifferentOne() {
        Map<String, Object> document = new LinkedHashMap<>(Map.of("title", "hello world"));

        assertThatThrownBy(() -> new MappingEmbedder(provider,
                classifiedTextMapping("pii"),
                VectorizationPolicy.permitting(java.util.Set.of("internal"))).embed(document))
                .hasMessageContaining("pii")
                .hasMessageContaining("internal");
    }

    @Test
    void unrestrictedEmbedsAnyClassAndUnclassifiedIsAlwaysAllowed() {
        Map<String, Object> classified = new LinkedHashMap<>(Map.of("title", "hello world"));
        assertThat(new MappingEmbedder(provider, classifiedTextMapping("secret"),
                VectorizationPolicy.unrestricted()).embed(classified))
                .containsKey("embedding");

        Map<String, Object> plain = new LinkedHashMap<>(Map.of("title", "hello world"));
        assertThat(new MappingEmbedder(provider, classifiedTextMapping(""))
                .embed(plain)).containsKey("embedding");
    }

    @Test
    void theGateRunsBeforeTheDocumentIsRead() {
        // No text in the document at all: an ungated embedder would return it
        // untouched, so a refusal here proves the gate precedes the read.
        assertThatThrownBy(() -> new MappingEmbedder(provider, classifiedTextMapping("pii"))
                .embed(new LinkedHashMap<>()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("pii");
    }

    @Test
    void aPolicyMustNameSomethingAndNeverABlankClass() {
        assertThatThrownBy(() -> VectorizationPolicy.permitting(java.util.Set.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unclassifiedOnly()");
        assertThatThrownBy(() -> VectorizationPolicy.permitting(java.util.Set.of(" ")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unclassified case");
    }
}
