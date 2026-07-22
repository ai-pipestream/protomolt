package ai.pipestream.proto.index.solr;

import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.index.spi.VectorSimilarity;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SolrSchemaGeneratorTest {

    private final SolrSchemaGenerator generator = new SolrSchemaGenerator();

    @Test
    void scalarKindsMapToStockSolrTypes() {
        IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc", List.of(
                field("title", ResolvedFieldHint.of(IndexFieldKind.TEXT)),
                field("id", ResolvedFieldHint.of(IndexFieldKind.KEYWORD)),
                field("small", ResolvedFieldHint.of(IndexFieldKind.INT32)),
                field("big", ResolvedFieldHint.of(IndexFieldKind.INT64)),
                field("ratio", ResolvedFieldHint.of(IndexFieldKind.FLOAT)),
                field("score", ResolvedFieldHint.of(IndexFieldKind.DOUBLE)),
                field("archived", ResolvedFieldHint.of(IndexFieldKind.BOOLEAN)),
                field("created", ResolvedFieldHint.of(IndexFieldKind.DATE)),
                field("payload", ResolvedFieldHint.of(IndexFieldKind.BINARY)),
                field("inner", ResolvedFieldHint.of(IndexFieldKind.OBJECT))));

        SolrSchemaGenerator.SolrSchema schema = generator.generate(plan);

        assertThat(schema.fieldTypes()).isEmpty(); // stock types need no declarations
        assertThat(schema.copyFields()).isEmpty();
        assertThat(schema.fields()).containsExactly(
                Map.of("name", "title", "type", "text_general", "indexed", true, "stored", true),
                Map.of("name", "id", "type", "string", "indexed", true, "stored", true),
                Map.of("name", "small", "type", "pint", "indexed", true, "stored", true),
                Map.of("name", "big", "type", "plong", "indexed", true, "stored", true),
                Map.of("name", "ratio", "type", "pfloat", "indexed", true, "stored", true),
                Map.of("name", "score", "type", "pdouble", "indexed", true, "stored", true),
                Map.of("name", "archived", "type", "boolean", "indexed", true, "stored", true),
                Map.of("name", "created", "type", "pdate", "indexed", true, "stored", true),
                // BINARY defaults to unindexed
                Map.of("name", "payload", "type", "binary", "indexed", false, "stored", true),
                // flat documents: object-shaped values land as JSON strings
                Map.of("name", "inner", "type", "string", "indexed", true, "stored", true));
    }

    @Test
    void analyzerNameBecomesTheTextFieldType() {
        IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc", List.of(
                field("title", ResolvedFieldHint.builder(IndexFieldKind.TEXT)
                        .analyzer("text_en")
                        .build())));

        SolrSchemaGenerator.SolrSchema schema = generator.generate(plan);

        assertThat(schema.fields()).containsExactly(
                Map.of("name", "title", "type", "text_en", "indexed", true, "stored", true));
    }

    @Test
    void sortableOrFacetableSetsDocValues() {
        IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc", List.of(
                field("status", ResolvedFieldHint.builder(IndexFieldKind.KEYWORD).sortable(true).build()),
                field("count", ResolvedFieldHint.builder(IndexFieldKind.INT64).facetable(true).build())));

        SolrSchemaGenerator.SolrSchema schema = generator.generate(plan);

        assertThat(schema.fields()).containsExactly(
                Map.of("name", "status", "type", "string", "indexed", true, "stored", true,
                        "docValues", true),
                Map.of("name", "count", "type", "plong", "indexed", true, "stored", true,
                        "docValues", true));
    }

    @Test
    void everyRangeKindBecomesMinMaxFields() {
        IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc", List.of(
                field("a", ResolvedFieldHint.of(IndexFieldKind.INT_RANGE)),
                field("b", ResolvedFieldHint.of(IndexFieldKind.LONG_RANGE)),
                field("c", ResolvedFieldHint.of(IndexFieldKind.FLOAT_RANGE)),
                field("d", ResolvedFieldHint.of(IndexFieldKind.DOUBLE_RANGE)),
                field("e", ResolvedFieldHint.of(IndexFieldKind.DATE_RANGE))));

        SolrSchemaGenerator.SolrSchema schema = generator.generate(plan);

        assertThat(schema.fields()).extracting(f -> f.get("name") + ":" + f.get("type"))
                .containsExactly(
                        "a_min:pint", "a_max:pint",
                        "b_min:plong", "b_max:plong",
                        "c_min:pfloat", "c_max:pfloat",
                        "d_min:pdouble", "d_max:pdouble",
                        "e_min:pdate", "e_max:pdate");
    }

    @Test
    void vectorFieldGetsADenseVectorFieldType() {
        IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc", List.of(
                field("embedding", ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                        .vectorDims(768)
                        .vectorSimilarity(VectorSimilarity.COSINE)
                        .hnswParams(new ResolvedFieldHint.HnswParams(16, 100))
                        .build())));

        SolrSchemaGenerator.SolrSchema schema = generator.generate(plan);

        assertThat(schema.fieldTypes()).containsExactly(Map.of(
                "name", "knn_vector_768_cosine_m16_ef100",
                "class", "solr.DenseVectorField",
                "vectorDimension", 768,
                "similarityFunction", "cosine",
                "hnswMaxConnections", 16,
                "hnswBeamWidth", 100));
        assertThat(schema.fields()).containsExactly(Map.of(
                "name", "embedding",
                "type", "knn_vector_768_cosine_m16_ef100",
                "indexed", true,
                "stored", true));
    }

    @Test
    void identicalVectorConfigsShareOneFieldType() {
        ResolvedFieldHint vector = ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                .vectorDims(4)
                .vectorSimilarity(VectorSimilarity.L2)
                .build();
        IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc", List.of(
                field("embedding_a", vector), field("embedding_b", vector)));

        SolrSchemaGenerator.SolrSchema schema = generator.generate(plan);

        assertThat(schema.fieldTypes()).hasSize(1);
        assertThat(schema.fieldTypes().get(0))
                .containsEntry("name", "knn_vector_4_l2")
                .containsEntry("similarityFunction", "euclidean");
        assertThat(schema.fields()).extracting(f -> f.get("type"))
                .containsExactly("knn_vector_4_l2", "knn_vector_4_l2");
    }

    @Test
    void everySimilarityMapsToASolrFunction() {
        Map<VectorSimilarity, String> expected = Map.of(
                VectorSimilarity.COSINE, "cosine",
                VectorSimilarity.L2, "euclidean",
                VectorSimilarity.DOT_PRODUCT, "dot_product",
                VectorSimilarity.MAX_INNER_PRODUCT, "dot_product");

        for (var entry : expected.entrySet()) {
            IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc", List.of(
                    field("embedding", ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                            .vectorDims(4)
                            .vectorSimilarity(entry.getKey())
                            .build())));

            assertThat(generator.generate(plan).fieldTypes().get(0))
                    .as("similarity %s", entry.getKey())
                    .containsEntry("similarityFunction", entry.getValue());
        }
    }

    @Test
    void subFieldsProduceFieldsAndCopyFields() {
        IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc", List.of(
                field("title", ResolvedFieldHint.builder(IndexFieldKind.TEXT)
                        .addSubField(new ResolvedFieldHint.SubField(IndexFieldKind.KEYWORD, "raw", ""))
                        .addSubField(new ResolvedFieldHint.SubField(IndexFieldKind.TEXT, "en", "text_en"))
                        .build())));

        SolrSchemaGenerator.SolrSchema schema = generator.generate(plan);

        assertThat(schema.fields()).containsExactly(
                Map.of("name", "title", "type", "text_general", "indexed", true, "stored", true),
                Map.of("name", "title_raw", "type", "string", "indexed", true, "stored", false),
                Map.of("name", "title_en", "type", "text_en", "indexed", true, "stored", false));
        assertThat(schema.copyFields()).containsExactly(
                Map.of("source", "title", "dest", "title_raw"),
                Map.of("source", "title", "dest", "title_en"));
    }

    @Test
    void engineParamsForSolrAreMergedVerbatim() {
        IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc", List.of(
                field("id", ResolvedFieldHint.builder(IndexFieldKind.KEYWORD)
                        .engineParams(Map.of(
                                "solr.omitNorms", "true",
                                "opensearch.index_options", "docs"))
                        .build())));

        SolrSchemaGenerator.SolrSchema schema = generator.generate(plan);

        assertThat(schema.fields()).containsExactly(
                Map.of("name", "id", "type", "string", "indexed", true, "stored", true,
                        "omitNorms", "true"));
    }

    @Test
    void skippedFieldsAreExcluded() {
        IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc", List.of(
                field("internal", ResolvedFieldHint.skipped()),
                field("id", ResolvedFieldHint.of(IndexFieldKind.KEYWORD))));

        assertThat(generator.generate(plan).fields()).extracting(f -> f.get("name"))
                .containsExactly("id");
    }

    /** Solr rejects every value after the first for a field the schema declares singular. */
    @Test
    void repeatedFieldsAreDeclaredMultiValued() {
        IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc", List.of(
                repeatedField("tags", ResolvedFieldHint.of(IndexFieldKind.KEYWORD)),
                field("title", ResolvedFieldHint.of(IndexFieldKind.TEXT))));

        assertThat(generator.generate(plan).fields()).containsExactly(
                Map.of("name", "tags", "type", "string", "indexed", true, "stored", true,
                        "multiValued", true),
                Map.of("name", "title", "type", "text_general", "indexed", true, "stored", true));
    }

    /** DenseVectorField carries the whole vector in one value and rejects multiValued. */
    @Test
    void repeatedVectorFieldStaysSingleValued() {
        IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc", List.of(
                repeatedField("embedding", ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                        .vectorDims(4)
                        .vectorSimilarity(VectorSimilarity.L2)
                        .build())));

        assertThat(generator.generate(plan).fields()).containsExactly(Map.of(
                "name", "embedding", "type", "knn_vector_4_l2", "indexed", true, "stored", true));
    }

    /** A copyField destination has to accept every value its multiValued source produces. */
    @Test
    void subFieldsOfARepeatedFieldAreAlsoMultiValued() {
        IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc", List.of(
                repeatedField("tags", ResolvedFieldHint.builder(IndexFieldKind.TEXT)
                        .addSubField(new ResolvedFieldHint.SubField(IndexFieldKind.KEYWORD, "raw", ""))
                        .build())));

        assertThat(generator.generate(plan).fields()).containsExactly(
                Map.of("name", "tags", "type", "text_general", "indexed", true, "stored", true,
                        "multiValued", true),
                Map.of("name", "tags_raw", "type", "string", "indexed", true, "stored", false,
                        "multiValued", true));
    }

    private static IndexingPlan.IndexedField field(String name, ResolvedFieldHint hint) {
        return new IndexingPlan.IndexedField(name, name, hint);
    }

    private static IndexingPlan.IndexedField repeatedField(String name, ResolvedFieldHint hint) {
        return new IndexingPlan.IndexedField(name, name, hint, true);
    }
}
