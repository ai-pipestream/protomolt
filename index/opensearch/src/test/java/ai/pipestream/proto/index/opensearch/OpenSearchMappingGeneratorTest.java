package ai.pipestream.proto.index.opensearch;

import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexingPlan;
import ai.pipestream.proto.index.spi.MapMode;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.index.spi.VectorElementType;
import ai.pipestream.proto.index.spi.VectorSimilarity;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OpenSearchMappingGeneratorTest {

    private final OpenSearchMappingGenerator generator = new OpenSearchMappingGenerator();

    @Test
    void scalarKindsMapToOpenSearchTypes() {
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
                field("inner", ResolvedFieldHint.of(IndexFieldKind.OBJECT)),
                field("items", ResolvedFieldHint.of(IndexFieldKind.NESTED))));

        Map<String, Object> properties = properties(generator.generate(plan));

        assertThat(properties.get("title")).isEqualTo(Map.of("type", "text"));
        assertThat(properties.get("id")).isEqualTo(Map.of("type", "keyword"));
        assertThat(properties.get("small")).isEqualTo(Map.of("type", "integer"));
        assertThat(properties.get("big")).isEqualTo(Map.of("type", "long"));
        assertThat(properties.get("ratio")).isEqualTo(Map.of("type", "float"));
        assertThat(properties.get("score")).isEqualTo(Map.of("type", "double"));
        assertThat(properties.get("archived")).isEqualTo(Map.of("type", "boolean"));
        assertThat(properties.get("created")).isEqualTo(Map.of("type", "date"));
        // BINARY defaults to unindexed, but the binary mapper has no index parameter to carry it
        assertThat(properties.get("payload")).isEqualTo(Map.of("type", "binary"));
        assertThat(properties.get("inner")).isEqualTo(Map.of("type", "object"));
        assertThat(properties.get("items")).isEqualTo(Map.of("type", "nested"));
    }

    @Test
    void textCarriesAnalyzersAndSubFields() {
        IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc", List.of(
                field("title", ResolvedFieldHint.builder(IndexFieldKind.TEXT)
                        .analyzer("english")
                        .searchAnalyzer("english_search")
                        .addSubField(new ResolvedFieldHint.SubField(IndexFieldKind.KEYWORD, "raw", ""))
                        .addSubField(new ResolvedFieldHint.SubField(IndexFieldKind.TEXT, "ngrams", "trigram"))
                        .build())));

        Map<String, Object> properties = properties(generator.generate(plan));

        assertThat(properties.get("title")).isEqualTo(Map.of(
                "type", "text",
                "analyzer", "english",
                "search_analyzer", "english_search",
                "fields", Map.of(
                        "raw", Map.of("type", "keyword"),
                        "ngrams", Map.of("type", "text", "analyzer", "trigram"))));
    }

    @Test
    void docValuesNullValueAndDateFormatAreEmitted() {
        IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc", List.of(
                field("status", ResolvedFieldHint.builder(IndexFieldKind.KEYWORD)
                        .sortable(true)
                        .nullValue("unknown")
                        .build()),
                field("count", ResolvedFieldHint.builder(IndexFieldKind.INT32)
                        .facetable(true)
                        .nullValue("0")
                        .build()),
                field("created", ResolvedFieldHint.builder(IndexFieldKind.DATE)
                        .dateFormat("epoch_millis")
                        .build())));

        Map<String, Object> properties = properties(generator.generate(plan));

        assertThat(properties.get("status")).isEqualTo(Map.of(
                "type", "keyword", "doc_values", true, "null_value", "unknown"));
        // null_value is typed per the hint: an int, not the string "0"
        assertThat(properties.get("count")).isEqualTo(Map.of(
                "type", "integer", "doc_values", true, "null_value", 0));
        assertThat(properties.get("created")).isEqualTo(Map.of(
                "type", "date", "format", "epoch_millis"));
    }

    @Test
    void floatVectorBecomesKnnVectorWithHnswMethod() {
        IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc", List.of(
                field("embedding", ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                        .vectorDims(768)
                        .vectorSimilarity(VectorSimilarity.COSINE)
                        .hnswParams(new ResolvedFieldHint.HnswParams(16, 100))
                        .engineParams(Map.of("opensearch.mode", "on_disk"))
                        .build())));

        Map<String, Object> properties = properties(generator.generate(plan));

        assertThat(properties.get("embedding")).isEqualTo(Map.of(
                "type", "knn_vector",
                "dimension", 768,
                "method", Map.of(
                        "name", "hnsw",
                        "space_type", "cosinesimil",
                        "parameters", Map.of("m", 16, "ef_construction", 100)),
                "mode", "on_disk"));
    }

    @Test
    void byteVectorCarriesDataTypeAndEverySimilarityMapsToASpaceType() {
        Map<VectorSimilarity, String> expected = Map.of(
                VectorSimilarity.COSINE, "cosinesimil",
                VectorSimilarity.L2, "l2",
                VectorSimilarity.DOT_PRODUCT, "innerproduct",
                VectorSimilarity.MAX_INNER_PRODUCT, "innerproduct");

        for (var entry : expected.entrySet()) {
            IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc", List.of(
                    field("embedding", ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                            .vectorDims(4)
                            .vectorSimilarity(entry.getKey())
                            .vectorElementType(VectorElementType.BYTE)
                            .build())));

            Map<String, Object> properties = properties(generator.generate(plan));

            assertThat(properties.get("embedding"))
                    .as("similarity %s", entry.getKey())
                    .isEqualTo(Map.of(
                            "type", "knn_vector",
                            "dimension", 4,
                            "data_type", "byte",
                            "method", Map.of("name", "hnsw", "space_type", entry.getValue())));
        }
    }

    @Test
    void everyRangeKindMapsToARangeType() {
        IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc", List.of(
                field("a", ResolvedFieldHint.of(IndexFieldKind.INT_RANGE)),
                field("b", ResolvedFieldHint.of(IndexFieldKind.LONG_RANGE)),
                field("c", ResolvedFieldHint.of(IndexFieldKind.FLOAT_RANGE)),
                field("d", ResolvedFieldHint.of(IndexFieldKind.DOUBLE_RANGE)),
                field("e", ResolvedFieldHint.of(IndexFieldKind.DATE_RANGE))));

        Map<String, Object> properties = properties(generator.generate(plan));

        assertThat(properties.get("a")).isEqualTo(Map.of("type", "integer_range"));
        assertThat(properties.get("b")).isEqualTo(Map.of("type", "long_range"));
        assertThat(properties.get("c")).isEqualTo(Map.of("type", "float_range"));
        assertThat(properties.get("d")).isEqualTo(Map.of("type", "double_range"));
        assertThat(properties.get("e")).isEqualTo(Map.of("type", "date_range"));
    }

    @Test
    void engineParamsForOtherEnginesAreIgnored() {
        IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc", List.of(
                field("id", ResolvedFieldHint.builder(IndexFieldKind.KEYWORD)
                        .engineParams(Map.of(
                                "opensearch.index_options", "docs",
                                "solr.omitNorms", "true",
                                "lucene.norms", "false"))
                        .build())));

        Map<String, Object> properties = properties(generator.generate(plan));

        assertThat(properties.get("id")).isEqualTo(Map.of("type", "keyword", "index_options", "docs"));
    }

    @Test
    void skippedFieldsAreExcluded() {
        IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc", List.of(
                field("internal", ResolvedFieldHint.skipped()),
                field("id", ResolvedFieldHint.of(IndexFieldKind.KEYWORD))));

        assertThat(properties(generator.generate(plan))).containsOnlyKeys("id");
    }

    /**
     * The binary mapper accepts only {@code doc_values} and {@code store}. Sending {@code index}
     * fails the whole mapping put with an unsupported-parameter error, even where the hint says
     * the field is unindexed.
     */
    @Test
    void binaryNeverCarriesTheIndexParameter() {
        IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc", List.of(
                field("payload", ResolvedFieldHint.builder(IndexFieldKind.BINARY)
                        .indexed(false)
                        .build()),
                field("id", ResolvedFieldHint.builder(IndexFieldKind.KEYWORD)
                        .indexed(false)
                        .build())));

        Map<String, Object> properties = properties(generator.generate(plan));

        assertThat(properties.get("payload")).isEqualTo(Map.of("type", "binary"));
        // other kinds still carry it
        assertThat(properties.get("id")).isEqualTo(Map.of("type", "keyword", "index", false));
    }

    /**
     * {@link OpenSearchDocumentMapper} renders a JSON-mode map as one JSON string, which an
     * object mapping rejects at index time.
     */
    @Test
    void jsonMapModeMapsToKeyword() {
        IndexingPlan plan = new IndexingPlan("ai.pipestream.test.Doc", List.of(
                field("labels", ResolvedFieldHint.builder(IndexFieldKind.OBJECT)
                        .mapMode(MapMode.JSON)
                        .build()),
                field("attrs", ResolvedFieldHint.builder(IndexFieldKind.OBJECT)
                        .mapMode(MapMode.FLATTEN)
                        .build())));

        Map<String, Object> properties = properties(generator.generate(plan));

        assertThat(properties.get("labels")).isEqualTo(Map.of("type", "keyword"));
        // the other modes emit object-shaped values and keep the object mapping
        assertThat(properties.get("attrs")).isEqualTo(Map.of("type", "object"));
    }

    private static IndexingPlan.IndexedField field(String name, ResolvedFieldHint hint) {
        return new IndexingPlan.IndexedField(name, name, hint);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> properties(Map<String, Object> mappings) {
        assertThat(mappings).containsOnlyKeys("properties");
        return (Map<String, Object>) mappings.get("properties");
    }
}
