package ai.pipestream.proto.metric.iceberg;

import ai.pipestream.proto.metric.Aggregate;
import ai.pipestream.proto.metric.FieldMetric;
import ai.pipestream.proto.metric.MemberRole;
import ai.pipestream.proto.metric.spi.CatalogMetricHintSource;
import ai.pipestream.proto.metric.spi.MetricMapping;
import ai.pipestream.proto.metric.spi.MetricMappings;
import ai.pipestream.proto.metric.spi.MetricRefusal;
import ai.pipestream.proto.metric.spi.MetricSubjectResolver;
import com.google.protobuf.Descriptors.Descriptor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;

/**
 * Rollup tables as metric subjects: {@code rollup:<table>} resolves
 * against the lake at request time, reading the declaration the sink
 * stamped onto the table itself (source subject, dimension columns,
 * measure columns with their source aggregates), so any rebuilt rollup is
 * instantly queryable with no side-channel configuration.
 *
 * <p>Re-aggregation is honest or absent: COUNT and SUM columns re-serve
 * as SUM (summing counts is counting), MIN as MIN and MAX as MAX, while
 * AVG and COUNT_DISTINCT columns are not declared as members — an average
 * of averages and a sum of distinct-counts are wrong answers, so those
 * columns stay scan-only outside the service. Date dimensions arrive as
 * their rendered bucket labels and serve as keyword dimensions: a rollup
 * cannot re-bucket time below the grain it was built at.</p>
 */
public final class IcebergRollupSubjects implements MetricSubjectResolver {

    /** The subject-name prefix this resolver answers: {@value}. */
    public static final String PREFIX = "rollup:";

    private final Catalog catalog;
    private final String namespace;

    public IcebergRollupSubjects(Catalog catalog, String namespace) {
        if (catalog == null) {
            throw new IllegalArgumentException("catalog must not be null");
        }
        if (namespace == null || namespace.isBlank()) {
            throw new IllegalArgumentException("namespace must not be blank");
        }
        this.catalog = catalog;
        this.namespace = namespace;
    }

    @Override
    public Resolved resolve(String subject) {
        if (!subject.startsWith(PREFIX)) {
            return null;
        }
        String tableName = subject.substring(PREFIX.length());
        TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
        Table table;
        try {
            table = catalog.loadTable(identifier);
        } catch (NoSuchTableException e) {
            throw new MetricRefusal(MetricRefusal.MISSING_TABLE,
                    "the lake has no rollup table '" + identifier
                            + "': rebuild-rollup writes it", List.of());
        }
        Map<String, String> properties = table.properties();
        String dimensionsCsv = properties.getOrDefault(
                IcebergRollupSink.PROPERTY_DIMENSIONS, "");
        String measuresCsv = properties.getOrDefault(
                IcebergRollupSink.PROPERTY_MEASURES, "");
        if (measuresCsv.isEmpty()) {
            throw new MetricRefusal(MetricRefusal.UNKNOWN_SUBJECT,
                    "table '" + identifier + "' carries no rollup declaration; only"
                            + " tables the rollup sink wrote serve as rollup subjects",
                    List.of());
        }
        List<String> dimensions = dimensionsCsv.isEmpty()
                ? List.of()
                : List.of(dimensionsCsv.split(","));
        List<String> measureNames = new ArrayList<>();
        CatalogMetricHintSource declarations = new CatalogMetricHintSource();
        for (String dimension : dimensions) {
            declarations.put("rollup.RollupRow", dimension, FieldMetric.newBuilder()
                    .setRole(MemberRole.MEMBER_ROLE_DIMENSION)
                    .build());
        }
        for (String entry : measuresCsv.split(",")) {
            int colon = entry.indexOf(':');
            String member = entry.substring(0, colon);
            Aggregate source = Aggregate.valueOf(entry.substring(colon + 1));
            measureNames.add(member);
            Aggregate reaggregate = reaggregate(source);
            if (reaggregate != null) {
                declarations.put("rollup.RollupRow", member, FieldMetric.newBuilder()
                        .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                        .setAggregate(reaggregate)
                        .build());
            }
        }
        Descriptor descriptor =
                IcebergRollupSink.rollupDescriptor(tableName, dimensions, measureNames);
        MetricMapping mapping = MetricMappings.build(subject, descriptor, declarations);
        return new Resolved(mapping, new IcebergMetricExecutor(
                requested -> catalog.loadTable(identifier)));
    }

    /**
     * The honest re-aggregation of a pre-aggregated column, or {@code null}
     * when none exists (AVG of averages and re-counting distincts are
     * wrong answers, never served).
     */
    static Aggregate reaggregate(Aggregate source) {
        return switch (source) {
            case AGGREGATE_COUNT, AGGREGATE_SUM -> Aggregate.AGGREGATE_SUM;
            case AGGREGATE_MIN -> Aggregate.AGGREGATE_MIN;
            case AGGREGATE_MAX -> Aggregate.AGGREGATE_MAX;
            default -> null;
        };
    }
}
