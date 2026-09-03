package ai.protomolt.proto.search.index.lucene;

import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import ai.protomolt.proto.search.index.spi.VectorElementType;
import ai.protomolt.proto.search.index.spi.VectorSimilarity;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class LuceneFieldSpecsTest {

    @Test
    void carriesAnalyzersDocValuesAndEngineParams() {
        IndexMapping mapping = new IndexMapping("ai.pipestream.test.Doc", List.of(
                new IndexMapping.IndexedField("title", "title",
                        ResolvedFieldHint.builder(IndexFieldKind.TEXT)
                                .analyzer("english")
                                .searchAnalyzer("english_search")
                                .sortable(true)
                                .engineParams(Map.of("lucene.norms", "false", "solr.omitNorms", "true"))
                                .build()),
                new IndexMapping.IndexedField("count", "count",
                        ResolvedFieldHint.builder(IndexFieldKind.INT64).facetable(true).build())));

        LuceneFieldSpecs specs = LuceneFieldSpecs.from(mapping);

        assertThat(specs.messageFullName()).isEqualTo("ai.pipestream.test.Doc");
        LuceneFieldSpecs.FieldSpec title = specs.find("title").orElseThrow();
        assertThat(title.analyzer()).isEqualTo("english");
        assertThat(title.searchAnalyzer()).isEqualTo("english_search");
        assertThat(title.docValuesType()).isEqualTo(DocValuesType.SORTED);
        // only lucene-scoped escape-hatch params, with the prefix stripped
        assertThat(title.engineParams()).containsOnly(Map.entry("norms", "false"));
        assertThat(specs.find("count").orElseThrow().docValuesType())
                .isEqualTo(DocValuesType.SORTED_NUMERIC);
    }

    /**
     * Lucene permits one doc-values type per field, so {@link ProtoLuceneMapper} emits the
     * multi-valued form when a field is both sortable and facetable. The spec once reported the
     * single-valued form for that combination, so consumers configuring an index from the report
     * would build sort fields the written documents cannot serve.
     */
    @Test
    void sortableAndFacetableFieldsReportTheMultiValuedDocValuesType() {
        IndexMapping mapping = new IndexMapping("ai.pipestream.test.Doc", List.of(
                new IndexMapping.IndexedField("title", "title",
                        ResolvedFieldHint.builder(IndexFieldKind.KEYWORD)
                                .sortable(true)
                                .facetable(true)
                                .build()),
                new IndexMapping.IndexedField("count", "count",
                        ResolvedFieldHint.builder(IndexFieldKind.INT64)
                                .sortable(true)
                                .facetable(true)
                                .build())));

        LuceneFieldSpecs specs = LuceneFieldSpecs.from(mapping);

        assertThat(specs.find("title").orElseThrow().docValuesType())
                .isEqualTo(DocValuesType.SORTED_SET);
        assertThat(specs.find("count").orElseThrow().docValuesType())
                .isEqualTo(DocValuesType.SORTED_NUMERIC);
    }

    @Test
    void vectorSpecExposesLuceneSimilarityAndEncoding() {
        IndexMapping mapping = new IndexMapping("ai.pipestream.test.Doc", List.of(
                new IndexMapping.IndexedField("embedding", "embedding",
                        ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                                .vectorDims(768)
                                .vectorSimilarity(VectorSimilarity.MAX_INNER_PRODUCT)
                                .vectorElementType(VectorElementType.BYTE)
                                .build())));

        LuceneFieldSpecs.FieldSpec spec = LuceneFieldSpecs.from(mapping).find("embedding").orElseThrow();

        assertThat(spec.vectorDims()).isEqualTo(768);
        assertThat(spec.similarityFunction()).isEqualTo(VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT);
        assertThat(spec.vectorEncoding()).isEqualTo(VectorEncoding.BYTE);
    }

    @Test
    void subFieldsBecomeIndexedOnlyCompanionSpecs() {
        IndexMapping mapping = new IndexMapping("ai.pipestream.test.Doc", List.of(
                new IndexMapping.IndexedField("title", "title",
                        ResolvedFieldHint.builder(IndexFieldKind.TEXT)
                                .addSubField(new ResolvedFieldHint.SubField(
                                        IndexFieldKind.KEYWORD, "raw", "keyword_analyzer"))
                                .build())));

        LuceneFieldSpecs specs = LuceneFieldSpecs.from(mapping);

        assertThat(specs.fields()).hasSize(2);
        LuceneFieldSpecs.FieldSpec raw = specs.find("title.raw").orElseThrow();
        assertThat(raw.kind()).isEqualTo(IndexFieldKind.KEYWORD);
        assertThat(raw.indexed()).isTrue();
        assertThat(raw.stored()).isFalse();
        assertThat(raw.analyzer()).isEqualTo("keyword_analyzer");
    }

    @Test
    void skippedFieldsAreExcluded() {
        IndexMapping mapping = new IndexMapping("ai.pipestream.test.Doc", List.of(
                new IndexMapping.IndexedField("internal", "internal", ResolvedFieldHint.skipped()),
                new IndexMapping.IndexedField("id", "id", ResolvedFieldHint.of(IndexFieldKind.KEYWORD))));

        LuceneFieldSpecs specs = LuceneFieldSpecs.from(mapping);

        assertThat(specs.fields()).hasSize(1);
        assertThat(specs.find("internal")).isEmpty();
        // a plain keyword needs no docValues and no vector attributes
        LuceneFieldSpecs.FieldSpec id = specs.find("id").orElseThrow();
        assertThat(id.docValuesType()).isEqualTo(DocValuesType.NONE);
        assertThat(id.vectorEncoding()).isEqualTo(VectorEncoding.FLOAT32);
    }
}
