package ai.protomolt.proto.asset.catalog;

import ai.protomolt.proto.asset.v1.AssetCatalogRow;
import ai.protomolt.proto.metric.MemberRef;
import ai.protomolt.proto.metric.MetricBackend;
import ai.protomolt.proto.metric.MetricFilter;
import ai.protomolt.proto.metric.MetricRow;
import ai.protomolt.proto.metric.QueryMetricsRequest;
import ai.protomolt.proto.metric.QueryMetricsResponse;
import ai.protomolt.proto.metric.lucene.LuceneMetricExecutor;
import ai.protomolt.proto.metric.spi.MetricExecutor;
import ai.protomolt.proto.metric.spi.MetricMapping;
import ai.protomolt.proto.metric.spi.MetricMappings;
import ai.protomolt.proto.metric.spi.MetricQueries;
import ai.protomolt.proto.metric.spi.ProtoOptionsMetricHintSource;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.IndexMappingFactory;
import ai.protomolt.proto.search.index.spi.ProtoOptionsIndexingHintSource;
import ai.protomolt.proto.search.service.LuceneSearchStore;
import ai.protomolt.proto.search.service.ServedMapping;
import com.google.protobuf.Descriptors.Descriptor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * The catalog view answered by the services that already exist: real catalog
 * rows into a real Lucene index, real metric queries out. No endpoint here
 * knows the questions below — they are ordinary group-by queries over a
 * subject whose declarations the schema itself carries.
 */
class AssetCatalogFacetTest {

    private static final String SUBJECT = "assets";

    @TempDir
    static Path work;

    static Descriptor row = AssetCatalogRow.getDescriptor();
    static LuceneSearchStore store;
    static MetricMapping mapping;
    static Map<MetricBackend, MetricExecutor> executors;

    @BeforeAll
    static void boot() {
        mapping = MetricMappings.build(SUBJECT, row, new ProtoOptionsMetricHintSource());
        IndexMapping indexMapping = new IndexMappingFactory(
                new ProtoOptionsIndexingHintSource()).create(row);
        ServedMapping served = new ServedMapping(indexMapping, "entry_uuid",
                message -> (String) message.getField(row.findFieldByName("entry_uuid")), null);
        store = new LuceneSearchStore(work, Map.of(SUBJECT, served));
        executors = Map.of(MetricBackend.METRIC_BACKEND_LUCENE, new LuceneMetricExecutor(
                new LuceneMetricExecutor.SubjectReader() {
                    @Override
                    public IndexMapping mapping(String subject) {
                        return indexMapping;
                    }

                    @Override
                    public MetricExecutor.Result read(String subject,
                            LuceneMetricExecutor.Aggregation aggregation) {
                        return store.withSearcher(subject, aggregation::run);
                    }
                }));

        // A small, deliberately uneven archive: two verified tars (one
        // bridged), a parquet dataset nobody bridged, two scans with measured
        // OCR scores, one asset nobody classified, and one whose declaration
        // the bytes contradicted.
        index(asset("1", "VERIFIED", "tar", "", 1_000, 3, 2, -1, false));
        index(asset("2", "VERIFIED", "tar", "", 2_000, 1, 0, -1, false));
        index(asset("3", "DECLARED", "parquet", "", 5_000, 1, 0, -1, false));
        index(asset("4", "IDENTIFIED", "pdf", "OCR_TEXT", 800, 2, 1, 0.4, true));
        index(asset("5", "IDENTIFIED", "image", "OCR_TEXT", 400, 2, 1, 0.8, true));
        index(asset("6", "UNCLASSIFIED", "", "", 100, 1, 0, -1, false));
        index(asset("7", "CONFLICTED", "", "", 300, 1, 0, -1, false));
    }

    @AfterAll
    static void tearDown() {
        store.close();
    }

    @Test
    void howManyAssetsOfEachFormatKind() {
        Map<String, Double> byFormat = group("format_kind", "stored_bytes");

        assertThat(byFormat).containsOnlyKeys("tar", "parquet", "pdf", "image", "none");
        assertThat(byFormat.get("tar")).isEqualTo(3_000.0);
        assertThat(byFormat.get("parquet")).isEqualTo(5_000.0);
        // The two assets naming no single format group under their own
        // token rather than dropping out of the facet entirely.
        assertThat(byFormat.get("none")).isEqualTo(400.0);
    }

    @Test
    void howMuchOfTheArchiveNobodyHasClassifiedOrCouldAgreeOn() {
        Map<String, Double> bytesByState = group("classification_state", "stored_bytes");

        assertThat(bytesByState).containsEntry("VERIFIED", 3_000.0)
                .containsEntry("DECLARED", 5_000.0)
                .containsEntry("IDENTIFIED", 1_200.0)
                .containsEntry("UNCLASSIFIED", 100.0)
                .containsEntry("CONFLICTED", 300.0);
    }

    @Test
    void theBacklogMeasuresAConsoleOpensWith() {
        MetricRow totals = one(query(request(
                "unclassified_assets", "conflicted_assets", "bridged_assets").build()));

        assertThat(totals.getMeasuresOrThrow("unclassified_assets")).isEqualTo(1.0);
        assertThat(totals.getMeasuresOrThrow("conflicted_assets")).isEqualTo(1.0);
        assertThat(totals.getMeasuresOrThrow("bridged_assets")).isEqualTo(3.0);
    }

    @Test
    void howManyRenditionsAreOriginalAndHowManyABridgeDerived() {
        MetricRow totals = one(query(request("renditions", "derived_renditions").build()));

        assertThat(totals.getMeasuresOrThrow("renditions")).isEqualTo(11.0);
        assertThat(totals.getMeasuresOrThrow("derived_renditions")).isEqualTo(4.0);
    }

    @Test
    void theOcrQualityDistributionSkipsWhatNobodyScored() {
        Map<String, Map<String, Double>> quality =
                groups("content_class", "mean_content_quality");

        // The mean is over the two scored assets, not over seven rows five of
        // which were never measured. The sentinel -1 never reaches an average.
        assertThat(quality.get("OCR_TEXT").get("mean_content_quality"))
                .isEqualTo(0.6, within(1e-9));
        // The unscored group still appears — the catalog is asked how much is
        // unmeasured — and carries no mean rather than a fabricated zero.
        assertThat(quality).containsKey("none");
        assertThat(quality.get("none")).doesNotContainKey("mean_content_quality");
    }

    @Test
    void whatIsClassifiedButNotYetBridged() {
        Map<String, Double> byBridged = group("bridged", "stored_bytes");

        assertThat(byBridged).containsOnlyKeys("true", "false");
        assertThat(byBridged.get("true")).isEqualTo(2_200.0);
        assertThat(byBridged.get("false")).isEqualTo(7_400.0);
    }

    @Test
    void aFacetScopesToOneFormatTheSameWayEveryOtherSubjectDoes() {
        QueryMetricsResponse response = query(request("stored_bytes")
                .addFilters(MetricFilter.newBuilder()
                        .setMember("format_kind").addEquals("tar"))
                .build());

        assertThat(one(response).getMeasuresOrThrow("stored_bytes")).isEqualTo(3_000.0);
        assertThat(response.getPhysicalPlan()).contains("format_kind:tar");
    }

    @Test
    void theSubjectAnswersUnderTheNameTheHostServesIt() {
        assertThat(mapping.subject()).isEqualTo(SUBJECT);
        assertThat(query(request("stored_bytes").build()).getMappingSubject())
                .isEqualTo(SUBJECT);
    }

    // ------------------------------------------------------------------

    private static Map<String, Map<String, Double>> groups(String dimension, String measure) {
        return query(request(measure)
                .addDimensions(MemberRef.newBuilder().setName(dimension))
                .build())
                .getRowsList().stream()
                .collect(Collectors.toMap(
                        item -> item.getDimensionsOrThrow(dimension),
                        MetricRow::getMeasuresMap));
    }

    private static Map<String, Double> group(String dimension, String measure) {
        return query(request(measure)
                .addDimensions(MemberRef.newBuilder().setName(dimension))
                .build())
                .getRowsList().stream()
                .collect(Collectors.toMap(
                        item -> item.getDimensionsOrThrow(dimension),
                        item -> item.getMeasuresOrThrow(measure)));
    }

    private static QueryMetricsRequest.Builder request(String... measures) {
        QueryMetricsRequest.Builder builder = QueryMetricsRequest.newBuilder()
                .setMappingSubject(SUBJECT).setLimit(100);
        for (String measure : measures) {
            builder.addMeasures(measure);
        }
        return builder;
    }

    private static QueryMetricsResponse query(QueryMetricsRequest request) {
        return MetricQueries.query(mapping, executors, request);
    }

    private static MetricRow one(QueryMetricsResponse response) {
        assertThat(response.getRowsList()).hasSize(1);
        return response.getRows(0);
    }

    private static void index(AssetCatalogRow asset) {
        store.index(SUBJECT, asset);
    }

    private static AssetCatalogRow asset(String id, String state, String formatKind,
                                         String contentClass, long bytes, long renditions,
                                         long derived, double quality, boolean measured) {
        return AssetCatalogRow.newBuilder()
                .setAccountId("acct")
                .setArchive("papers")
                .setEntryUuid("6b3f2a5c-0000-4000-8000-00000000000" + id)
                .setEntryId("asset-" + id)
                .setFilename("asset-" + id)
                .setClassificationState(state)
                .setFormatKind(formatKind)
                .setContentClass(contentClass)
                .setVerified("VERIFIED".equals(state))
                .setSizeBytes(bytes)
                .setRenditionCount(renditions)
                .setDerivedRenditions(derived)
                .setBridged(derived > 0)
                .setContentQuality(quality)
                .setQualityMeasured(measured)
                .setVersion(1)
                .build();
    }
}
