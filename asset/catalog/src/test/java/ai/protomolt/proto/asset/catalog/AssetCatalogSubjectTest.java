package ai.protomolt.proto.asset.catalog;

import ai.protomolt.proto.asset.v1.AssetCatalogRow;
import ai.protomolt.proto.metric.Aggregate;
import ai.protomolt.proto.metric.MemberRole;
import ai.protomolt.proto.metric.TimeGrain;
import ai.protomolt.proto.metric.spi.MetricMapping;
import ai.protomolt.proto.metric.spi.MetricMappings;
import ai.protomolt.proto.metric.spi.ProtoOptionsMetricHintSource;
import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.IndexMappingFactory;
import ai.protomolt.proto.search.index.spi.ProtoOptionsIndexingHintSource;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The catalog subject as the two existing services actually read it. The
 * point of the annotations is that no bespoke endpoint is needed for a
 * catalog view, so the proof is that the real metric and index hint sources
 * resolve the subject rather than that the proto text looks right.
 */
class AssetCatalogSubjectTest {

    private final MetricMapping metrics = MetricMappings.build("",
            AssetCatalogRow.getDescriptor(), new ProtoOptionsMetricHintSource());

    private final IndexMapping index = new IndexMappingFactory(
            new ProtoOptionsIndexingHintSource()).create(AssetCatalogRow.getDescriptor());

    @Test
    void theSubjectNamesItselfAndCountsByAssetIdentity() {
        assertThat(metrics.subject()).isEqualTo("asset.catalog");
    }

    @Test
    void theThreeClassificationDimensionsAreGroupable() {
        for (String dimension : new String[] {
                "classification_state", "format_kind", "content_class"}) {
            assertThat(metrics.member(dimension))
                    .as(dimension + " is a declared dimension")
                    .hasValueSatisfying(member -> {
                        assertThat(member.role()).isEqualTo(MemberRole.MEMBER_ROLE_DIMENSION);
                        assertThat(member.kind())
                                .isEqualTo(MetricMapping.FieldKind.KEYWORD);
                    });
            assertThat(index.fields()).anySatisfy(field -> {
                assertThat(field.path()).isEqualTo(dimension);
                assertThat(field.type()).isEqualTo(IndexFieldKind.KEYWORD);
            });
        }
    }

    @Test
    void theTenantAndArchiveGroupTooSoAFacetIsAlwaysScoped() {
        assertThat(metrics.member("account_id")).hasValueSatisfying(member ->
                assertThat(member.role()).isEqualTo(MemberRole.MEMBER_ROLE_DIMENSION));
        assertThat(metrics.member("archive")).hasValueSatisfying(member ->
                assertThat(member.role()).isEqualTo(MemberRole.MEMBER_ROLE_DIMENSION));
    }

    @Test
    void theMeasuresCoverSizeShapeAndRecoveredTextQuality() {
        assertThat(metrics.member("stored_bytes")).hasValueSatisfying(member -> {
            assertThat(member.role()).isEqualTo(MemberRole.MEMBER_ROLE_MEASURE);
            assertThat(member.aggregate()).isEqualTo(Aggregate.AGGREGATE_SUM);
            assertThat(member.fieldName()).isEqualTo("size_bytes");
        });
        assertThat(metrics.member("renditions")).hasValueSatisfying(member ->
                assertThat(member.aggregate()).isEqualTo(Aggregate.AGGREGATE_SUM));
        assertThat(metrics.member("derived_renditions")).hasValueSatisfying(member ->
                assertThat(member.aggregate()).isEqualTo(Aggregate.AGGREGATE_SUM));
        assertThat(metrics.member("mean_content_quality")).hasValueSatisfying(member -> {
            assertThat(member.aggregate()).isEqualTo(Aggregate.AGGREGATE_AVG);
            // An unmeasured quality is -1, so the mean has to exclude it or
            // it would report a distribution nobody measured.
            assertThat(member.rowFilters()).isNotEmpty();
        });
    }

    @Test
    void theCountsAConsoleOpensWithAreDeclaredMeasures() {
        for (String measure : new String[] {
                "unclassified_assets", "conflicted_assets", "bridged_assets"}) {
            assertThat(metrics.member(measure))
                    .as(measure + " is a declared measure")
                    .hasValueSatisfying(member -> {
                        assertThat(member.role()).isEqualTo(MemberRole.MEMBER_ROLE_MEASURE);
                        assertThat(member.aggregate()).isEqualTo(Aggregate.AGGREGATE_COUNT);
                        assertThat(member.rowFilters()).isNotEmpty();
                    });
        }
    }

    @Test
    void bothTimeDimensionsBucketByDayUnlessAskedOtherwise() {
        for (String dimension : new String[] {"updated_at", "classified_at"}) {
            assertThat(metrics.member(dimension)).hasValueSatisfying(member -> {
                assertThat(member.role()).isEqualTo(MemberRole.MEMBER_ROLE_DIMENSION);
                assertThat(member.kind()).isEqualTo(MetricMapping.FieldKind.DATE);
                assertThat(member.defaultGrain()).isEqualTo(TimeGrain.TIME_GRAIN_DAY);
            });
        }
    }

    @Test
    void identityAndFreeTextAreSearchableWithoutBecomingFacets() {
        // entry_id and filename are how a person finds one asset; grouping
        // by them would produce one bucket per asset, which is not a facet.
        assertThat(metrics.member("entry_id")).isEmpty();
        assertThat(metrics.member("filename")).isEmpty();
        assertThat(index.fields()).anySatisfy(field -> {
            assertThat(field.path()).isEqualTo("filename");
            assertThat(field.type()).isEqualTo(IndexFieldKind.TEXT);
        });
        assertThat(index.fields()).anySatisfy(field -> {
            assertThat(field.path()).isEqualTo("entry_uuid");
            assertThat(field.type()).isEqualTo(IndexFieldKind.KEYWORD);
        });
    }
}
