package ai.pipestream.proto.index.spi;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ResolvedFieldHintTest {

    @Test
    void compatConstructorAppliesStandardDefaults() {
        ResolvedFieldHint hint = new ResolvedFieldHint(IndexFieldKind.KEYWORD, true, true, "", 0);

        assertThat(hint.vectorSimilarity()).isEqualTo(VectorSimilarity.COSINE);
        assertThat(hint.vectorElementType()).isEqualTo(VectorElementType.FLOAT32);
        assertThat(hint.hnswParams()).isNull();
        assertThat(hint.subFields()).isEmpty();
        assertThat(hint.analyzerOverride()).isEmpty();
        assertThat(hint.searchAnalyzerOverride()).isEmpty();
        assertThat(hint.nullValue()).isNull();
        assertThat(hint.skipIfMissing()).isTrue();
        assertThat(hint.sortable()).isFalse();
        assertThat(hint.facetable()).isFalse();
        assertThat(hint.mapMode()).isNull();
        assertThat(hint.dateFormatOverride()).isEmpty();
        assertThat(hint.dateResolution()).isEqualTo(DateResolution.MILLIS);
        assertThat(hint.engineParams()).isEmpty();
    }

    @Test
    void builderCarriesEveryAttribute() {
        ResolvedFieldHint hint = ResolvedFieldHint.builder(IndexFieldKind.VECTOR)
                .name("embedding")
                .vectorDims(768)
                .vectorSimilarity(VectorSimilarity.MAX_INNER_PRODUCT)
                .vectorElementType(VectorElementType.BYTE)
                .hnswParams(new ResolvedFieldHint.HnswParams(16, 100))
                .addSubField(new ResolvedFieldHint.SubField(IndexFieldKind.KEYWORD, "raw", ""))
                .analyzer("english")
                .searchAnalyzer("english_search")
                .nullValue("0")
                .skipIfMissing(false)
                .sortable(true)
                .facetable(true)
                .mapMode(MapMode.ENTRIES)
                .dateFormat("epoch_millis")
                .dateResolution(DateResolution.SECONDS)
                .engineParams(Map.of("opensearch.engine", "faiss"))
                .build();

        assertThat(hint.type()).isEqualTo(IndexFieldKind.VECTOR);
        assertThat(hint.nameOverride()).contains("embedding");
        assertThat(hint.vectorDims()).isEqualTo(768);
        assertThat(hint.vectorSimilarity()).isEqualTo(VectorSimilarity.MAX_INNER_PRODUCT);
        assertThat(hint.vectorElementType()).isEqualTo(VectorElementType.BYTE);
        assertThat(hint.hnswParams()).isEqualTo(new ResolvedFieldHint.HnswParams(16, 100));
        assertThat(hint.subFields()).containsExactly(
                new ResolvedFieldHint.SubField(IndexFieldKind.KEYWORD, "raw", ""));
        assertThat(hint.analyzerOverride()).contains("english");
        assertThat(hint.searchAnalyzerOverride()).contains("english_search");
        assertThat(hint.nullValue()).isEqualTo("0");
        assertThat(hint.skipIfMissing()).isFalse();
        assertThat(hint.sortable()).isTrue();
        assertThat(hint.facetable()).isTrue();
        assertThat(hint.mapMode()).isEqualTo(MapMode.ENTRIES);
        assertThat(hint.dateFormatOverride()).contains("epoch_millis");
        assertThat(hint.dateResolution()).isEqualTo(DateResolution.SECONDS);
        // toBuilder round-trips losslessly
        assertThat(hint.toBuilder().build()).isEqualTo(hint);
    }

    @Test
    void missingSubstituteCoercesPerType() {
        assertThat(substitute(IndexFieldKind.INT32, "7")).isEqualTo(7);
        assertThat(substitute(IndexFieldKind.INT64, "7")).isEqualTo(7L);
        assertThat(substitute(IndexFieldKind.FLOAT, "1.5")).isEqualTo(1.5f);
        assertThat(substitute(IndexFieldKind.DOUBLE, "1.5")).isEqualTo(1.5d);
        assertThat(substitute(IndexFieldKind.BOOLEAN, "true")).isEqualTo(true);
        // DATE: epoch number when it parses, engine-side date string otherwise
        assertThat(substitute(IndexFieldKind.DATE, "1700000000000")).isEqualTo(1_700_000_000_000L);
        assertThat(substitute(IndexFieldKind.DATE, "2023-11-14T22:13:20Z")).isEqualTo("2023-11-14T22:13:20Z");
        assertThat(substitute(IndexFieldKind.KEYWORD, "n/a")).isEqualTo("n/a");
        // an empty string is a legal substitute, distinct from "no substitute"
        assertThat(substitute(IndexFieldKind.TEXT, "")).isEqualTo("");
    }

    @Test
    void missingSubstituteEmptyWhenUnsetAndLoudWhenUnparsable() {
        assertThat(ResolvedFieldHint.of(IndexFieldKind.INT32).missingSubstitute()).isEmpty();
        ResolvedFieldHint bad = ResolvedFieldHint.builder(IndexFieldKind.INT32).nullValue("x").build();
        assertThatThrownBy(bad::missingSubstitute).isInstanceOf(NumberFormatException.class);
    }

    @Test
    void engineParamsAreScopedByEngineIdWithSuffixKeys() {
        ResolvedFieldHint hint = ResolvedFieldHint.builder(IndexFieldKind.KEYWORD)
                .engineParams(Map.of(
                        "opensearch.index_options", "docs",
                        "opensearch.similarity", "BM25",
                        "solr.omitNorms", "true"))
                .build();

        assertThat(hint.engineParams("opensearch"))
                .containsOnly(Map.entry("index_options", "docs"), Map.entry("similarity", "BM25"));
        assertThat(hint.engineParams("solr")).containsOnly(Map.entry("omitNorms", "true"));
        assertThat(hint.engineParams("lucene")).isEmpty();
    }

    @Test
    void mapModeOrFallsBackToEngineDefault() {
        assertThat(ResolvedFieldHint.of(IndexFieldKind.OBJECT).mapModeOr(MapMode.JSON))
                .isEqualTo(MapMode.JSON);
        assertThat(ResolvedFieldHint.builder(IndexFieldKind.OBJECT).mapMode(MapMode.SKIP).build()
                .mapModeOr(MapMode.JSON)).isEqualTo(MapMode.SKIP);
    }

    @Test
    void subFieldRejectsBlankName() {
        assertThatThrownBy(() -> new ResolvedFieldHint.SubField(IndexFieldKind.KEYWORD, " ", ""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void subFieldsAndEngineParamsAreImmutableCopies() {
        ResolvedFieldHint hint = ResolvedFieldHint.builder(IndexFieldKind.TEXT)
                .subFields(List.of(new ResolvedFieldHint.SubField(IndexFieldKind.KEYWORD, "raw", "")))
                .engineParams(Map.of("lucene.norms", "false"))
                .build();
        assertThatThrownBy(() -> hint.subFields().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> hint.engineParams().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private static Object substitute(IndexFieldKind kind, String nullValue) {
        return ResolvedFieldHint.builder(kind).nullValue(nullValue).build()
                .missingSubstitute().orElseThrow();
    }
}
