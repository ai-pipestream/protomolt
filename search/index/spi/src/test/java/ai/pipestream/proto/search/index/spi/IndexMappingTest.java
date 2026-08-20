package ai.pipestream.proto.search.index.spi;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexMappingTest {

    private static IndexMapping.IndexedField field(String path, IndexFieldKind kind) {
        return new IndexMapping.IndexedField(path, path, ResolvedFieldHint.of(kind));
    }

    @Test
    void constructorRejectsNullComponents() {
        assertThatThrownBy(() -> new IndexMapping(null, List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("messageFullName");
        assertThatThrownBy(() -> new IndexMapping("ai.pipestream.test.Doc", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("fields");
    }

    @Test
    void fieldsAreCopiedDefensivelyAndExposedImmutably() {
        List<IndexMapping.IndexedField> mutable = new ArrayList<>();
        mutable.add(field("title", IndexFieldKind.TEXT));
        IndexMapping mapping = new IndexMapping("ai.pipestream.test.Doc", mutable);

        mutable.add(field("body", IndexFieldKind.TEXT));
        assertThat(mapping.fields()).hasSize(1);
        assertThatThrownBy(() -> mapping.fields().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void findReturnsTheFirstMatchingPathAndEmptyForUnknownPaths() {
        IndexMapping mapping = new IndexMapping("ai.pipestream.test.Doc",
                List.of(field("title", IndexFieldKind.TEXT), field("body", IndexFieldKind.TEXT)));

        assertThat(mapping.messageFullName()).isEqualTo("ai.pipestream.test.Doc");
        assertThat(mapping.find("title")).get()
                .extracting(IndexMapping.IndexedField::type)
                .isEqualTo(IndexFieldKind.TEXT);
        assertThat(mapping.find("zzz")).isEmpty();
    }

    @Test
    void indexableDropsSkipHintsButKeepsThemInTheMapping() {
        IndexMapping mapping = new IndexMapping("ai.pipestream.test.Doc",
                List.of(field("title", IndexFieldKind.TEXT), field("secret", IndexFieldKind.SKIP)));

        assertThat(mapping.find("secret")).isPresent();
        assertThat(mapping.indexable())
                .extracting(IndexMapping.IndexedField::path)
                .containsExactly("title");
    }

    @Test
    void singularIndexedFieldConstructorDefaultsRepeatedToFalse() {
        IndexMapping.IndexedField singular = field("title", IndexFieldKind.TEXT);
        IndexMapping.IndexedField repeated =
                new IndexMapping.IndexedField("tags", "tags", ResolvedFieldHint.of(IndexFieldKind.KEYWORD), true);

        assertThat(singular.repeated()).isFalse();
        assertThat(repeated.repeated()).isTrue();
    }

    @Test
    void indexedFieldRejectsNullComponents() {
        ResolvedFieldHint hint = ResolvedFieldHint.of(IndexFieldKind.TEXT);
        assertThatThrownBy(() -> new IndexMapping.IndexedField(null, "title", hint))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("path");
        assertThatThrownBy(() -> new IndexMapping.IndexedField("title", null, hint))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("fieldName");
        assertThatThrownBy(() -> new IndexMapping.IndexedField("title", "title", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("hint");
    }

    @Test
    void indexedFieldDelegatesTypeStoredAndIndexedToTheHint() {
        ResolvedFieldHint hint = ResolvedFieldHint.builder(IndexFieldKind.KEYWORD)
                .stored(false)
                .indexed(false)
                .build();
        IndexMapping.IndexedField field = new IndexMapping.IndexedField("doc_id", "doc_id", hint);

        assertThat(field.type()).isEqualTo(IndexFieldKind.KEYWORD);
        assertThat(field.stored()).isFalse();
        assertThat(field.indexed()).isFalse();
        assertThat(field.hint()).isSameAs(hint);
    }
}
