package ai.pipestream.proto.embeddings;

import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PlanEmbedderTest {

    private final EmbeddingProvider provider = new FixedTableEmbeddingProvider();

    private static IndexingPlan.IndexedField text(String name) {
        return new IndexingPlan.IndexedField(name, name, ResolvedFieldHint.of(IndexFieldKind.TEXT));
    }

    private static IndexingPlan.IndexedField vector(String name, int dims) {
        return new IndexingPlan.IndexedField(name, name,
                ResolvedFieldHint.builder(IndexFieldKind.VECTOR).vectorDims(dims).build(), true);
    }

    /** One TEXT field, one VECTOR field: the shape the no-args selection requires. */
    private static IndexingPlan singleTextPlan(int dims) {
        return new IndexingPlan("library.Book", List.of(
                text("title"),
                new IndexingPlan.IndexedField("genre", "genre", ResolvedFieldHint.of(IndexFieldKind.KEYWORD)),
                vector("embedding", dims)));
    }

    private static IndexingPlan twoTextPlan() {
        return new IndexingPlan("library.Book", List.of(
                text("title"),
                text("body"),
                vector("embedding", 3)));
    }

    @Test
    void autoSelectionEmbedsTheOnlyTextFieldIntoTheOnlyVectorField() {
        Map<String, Object> document = new LinkedHashMap<>(Map.of("title", "hello world", "genre", "memoir"));

        Map<String, Object> embedded = new PlanEmbedder(provider, singleTextPlan(3)).embed(document);

        assertThat(embedded).isSameAs(document);
        assertThat(embedded.get("embedding")).isEqualTo(List.of(0.1f, 0.2f, 0.3f));
    }

    @Test
    void explicitFieldNamesPickTheSourceFromAPlanWithSeveralTextFields() {
        Map<String, Object> document = new LinkedHashMap<>(Map.of("title", "hello world", "body", "a memoir"));

        new PlanEmbedder(provider, twoTextPlan()).embed(document, "body", "embedding");

        assertThat(document.get("embedding")).isEqualTo(List.of(0.4f, 0.5f, 0.6f));
    }

    @Test
    void dimensionMismatchAgainstTheVectorDimsHintFails() {
        PlanEmbedder embedder = new PlanEmbedder(provider, singleTextPlan(4));
        Map<String, Object> document = new LinkedHashMap<>(Map.of("title", "hello world"));

        assertThatThrownBy(() -> embedder.embed(document))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Provider 'fixed-table' produces 3-dimensional vectors,"
                        + " but field 'embedding' is hinted vector_dims=4");
    }

    @Test
    void autoSelectionRejectsAPlanWithMoreThanOneTextField() {
        PlanEmbedder embedder = new PlanEmbedder(provider, twoTextPlan());
        Map<String, Object> document = new LinkedHashMap<>(Map.of("title", "hello world"));

        assertThatThrownBy(() -> embedder.embed(document))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Plan for library.Book has 2 TEXT fields (title, body); name the"
                        + " fields explicitly with embed(document, textFieldName, vectorFieldName)");
    }

    @Test
    void absentTextFieldLeavesTheDocumentUnchanged() {
        Map<String, Object> document = new LinkedHashMap<>(Map.of("genre", "memoir"));

        Map<String, Object> embedded = new PlanEmbedder(provider, singleTextPlan(3)).embed(document);

        assertThat(embedded).isSameAs(document);
        assertThat(embedded).containsOnlyKeys("genre");
    }

    @Test
    void emptyTextFieldLeavesTheDocumentUnchanged() {
        Map<String, Object> document = new LinkedHashMap<>(Map.of("title", ""));

        assertThat(new PlanEmbedder(provider, singleTextPlan(3)).embed(document))
                .containsOnlyKeys("title");
    }
}
