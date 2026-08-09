package ai.pipestream.proto.index.spi;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexingPlanTest {

    private static IndexingPlan.IndexedField field(String path, IndexFieldKind kind) {
        return new IndexingPlan.IndexedField(path, path, ResolvedFieldHint.of(kind));
    }

    @Test
    void constructorRejectsNullComponents() {
        assertThatThrownBy(() -> new IndexingPlan(null, List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("messageFullName");
        assertThatThrownBy(() -> new IndexingPlan("ai.pipestream.test.Doc", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("fields");
    }

    @Test
    void fieldsAreCopiedDefensivelyAndExposedImmutably() {
        List<IndexingPlan.IndexedField> mutable = new ArrayList<>();
        mutable.add(field("title", IndexFieldKind.TEXT));
        IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc", mutable);

        mutable.add(field("body", IndexFieldKind.TEXT));
        assertThat(plan.fields()).hasSize(1);
        assertThatThrownBy(() -> plan.fields().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void findReturnsTheFirstMatchingPathAndEmptyForUnknownPaths() {
        IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc",
                List.of(field("title", IndexFieldKind.TEXT), field("body", IndexFieldKind.TEXT)));

        assertThat(plan.messageFullName()).isEqualTo("ai.pipestream.test.Doc");
        assertThat(plan.find("title")).get()
                .extracting(IndexingPlan.IndexedField::type)
                .isEqualTo(IndexFieldKind.TEXT);
        assertThat(plan.find("zzz")).isEmpty();
    }

    @Test
    void indexableDropsSkipHintsButKeepsThemInThePlan() {
        IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc",
                List.of(field("title", IndexFieldKind.TEXT), field("secret", IndexFieldKind.SKIP)));

        assertThat(plan.find("secret")).isPresent();
        assertThat(plan.indexable())
                .extracting(IndexingPlan.IndexedField::path)
                .containsExactly("title");
    }

    @Test
    void singularIndexedFieldConstructorDefaultsRepeatedToFalse() {
        IndexingPlan.IndexedField singular = field("title", IndexFieldKind.TEXT);
        IndexingPlan.IndexedField repeated =
                new IndexingPlan.IndexedField("tags", "tags", ResolvedFieldHint.of(IndexFieldKind.KEYWORD), true);

        assertThat(singular.repeated()).isFalse();
        assertThat(repeated.repeated()).isTrue();
    }

    @Test
    void indexedFieldRejectsNullComponents() {
        ResolvedFieldHint hint = ResolvedFieldHint.of(IndexFieldKind.TEXT);
        assertThatThrownBy(() -> new IndexingPlan.IndexedField(null, "title", hint))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("path");
        assertThatThrownBy(() -> new IndexingPlan.IndexedField("title", null, hint))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("fieldName");
        assertThatThrownBy(() -> new IndexingPlan.IndexedField("title", "title", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("hint");
    }

    @Test
    void indexedFieldDelegatesTypeStoredAndIndexedToTheHint() {
        ResolvedFieldHint hint = ResolvedFieldHint.builder(IndexFieldKind.KEYWORD)
                .stored(false)
                .indexed(false)
                .build();
        IndexingPlan.IndexedField field = new IndexingPlan.IndexedField("doc_id", "doc_id", hint);

        assertThat(field.type()).isEqualTo(IndexFieldKind.KEYWORD);
        assertThat(field.stored()).isFalse();
        assertThat(field.indexed()).isFalse();
        assertThat(field.hint()).isSameAs(hint);
    }
}
