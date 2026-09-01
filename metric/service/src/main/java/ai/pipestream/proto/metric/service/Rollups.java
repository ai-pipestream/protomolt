package ai.pipestream.proto.metric.service;

import ai.pipestream.proto.metric.MemberRef;
import ai.pipestream.proto.metric.MetricRow;
import ai.pipestream.proto.metric.QueryMetricsRequest;
import ai.pipestream.proto.metric.QueryMetricsResponse;
import ai.pipestream.proto.metric.RebuildRollupRequest;
import ai.pipestream.proto.metric.RebuildRollupResponse;
import ai.pipestream.proto.metric.RollupEnrichment;
import ai.pipestream.proto.metric.spi.MetricQueries;
import ai.pipestream.proto.metric.spi.MetricRefusal;
import ai.pipestream.proto.metric.spi.RollupSink;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * One rebuild, shared by the RPC and the catalog verb so the two surfaces
 * cannot drift: the aggregate query runs on the resolved engine and its
 * complete result replaces the named lake table. Exact or refused: a
 * result that fills the group budget cannot be attested complete, and a
 * mount without a sink refuses by name.
 *
 * <p>Joins happen here, never at query time (the metric-joins design):
 * an optional enrichment resolves each result row's join key against a
 * second subject, strictly one to at most one. A fan-out refuses the
 * whole rebuild, a miss leaves the pulled columns empty and is counted,
 * and the pulled members land as ordinary dimension columns, so the
 * enriched rollup serves back with no sink or resolver changes.</p>
 */
final class Rollups {

    /**
     * The most groups one rebuild may hold: the query surface's own limit
     * cap. A result that fills it cannot be attested complete, and a
     * rollup is exact or refused, never truncated. The enrichment lookup
     * runs under the same budget for the same reason.
     */
    static final int GROUP_BUDGET = 1000;

    private Rollups() {
    }

    static RebuildRollupResponse rebuild(ServedMetricSubject subject,
            RollupSink rollups, RebuildRollupRequest request,
            Function<String, ServedMetricSubject> resolve) {
        if (rollups == null) {
            throw new MetricRefusal(MetricRefusal.MISSING_SINK,
                    "this mount has no rollup sink: rollups land in the lake, so"
                            + " mount the metrics role with the Iceberg catalog"
                            + " family", List.of());
        }
        QueryMetricsResponse answer = MetricQueries.query(
                subject.mapping(), subject.executors(),
                QueryMetricsRequest.newBuilder()
                        .setMappingSubject(request.getMappingSubject())
                        .setBackend(request.getBackend())
                        .addAllMeasures(request.getMeasuresList())
                        .addAllDimensions(request.getDimensionsList())
                        .addAllFilters(request.getFiltersList())
                        .setLimit(GROUP_BUDGET)
                        .build());
        if (answer.getRowCount() >= GROUP_BUDGET) {
            throw new MetricRefusal(MetricRefusal.ROLLUP_BUDGET,
                    "the rollup filled the group budget of " + GROUP_BUDGET
                            + " and cannot be attested complete; narrow the"
                            + " dimensions or filter the subject", List.of());
        }

        List<String> dimensionColumns =
                request.getDimensionsList().stream().map(MemberRef::getName).toList();
        List<MetricRow> rows = answer.getRowsList();
        String enrichedFrom = "";
        long unenriched = 0;
        if (request.hasEnrichment()) {
            Enriched enriched = enrich(request, resolve, dimensionColumns, rows);
            dimensionColumns = enriched.dimensionColumns();
            rows = enriched.rows();
            enrichedFrom = request.getEnrichment().getSubject();
            unenriched = enriched.unenriched();
        }

        RollupSink.Written written = rollups.replace(
                request.getMappingSubject(),
                request.getTable(),
                dimensionColumns,
                request.getMeasuresList().stream()
                        .map(name -> new RollupSink.MeasureColumn(name,
                                subject.mapping().members().get(name).aggregate()))
                        .toList(),
                rows);
        return RebuildRollupResponse.newBuilder()
                .setMappingSubject(answer.getMappingSubject())
                .setBackend(answer.getBackend())
                .setTable(written.table())
                .setRowsWritten(written.rowsWritten())
                .setSnapshotId(written.snapshotId())
                .setPhysicalPlan(answer.getPhysicalPlan())
                .setEnrichedFrom(enrichedFrom)
                .setRowsUnenriched(unenriched)
                .build();
    }

    private record Enriched(
            List<String> dimensionColumns, List<MetricRow> rows, long unenriched) {
    }

    /**
     * The enrichment lookup and the join, strictly one to at most one. The
     * lookup is a dimensions-only aggregate on the enrichment subject
     * (distinct combinations of the join key and the pulled members), run
     * under the rebuild's own group budget; rows match on the key
     * dimension's rendered string.
     */
    private static Enriched enrich(RebuildRollupRequest request,
            Function<String, ServedMetricSubject> resolve,
            List<String> dimensionColumns, List<MetricRow> rows) {
        RollupEnrichment enrichment = request.getEnrichment();
        String key = enrichment.getJoinKey();
        if (!dimensionColumns.contains(key)) {
            throw new MetricRefusal(MetricRefusal.INVALID_ENRICHMENT,
                    "join key '" + key + "' is not among the rebuild's dimensions;"
                            + " the key must appear in the aggregate result",
                    List.copyOf(dimensionColumns));
        }
        Set<String> pulled = new LinkedHashSet<>();
        for (String member : enrichment.getMembersList()) {
            if (member.equals(key) || dimensionColumns.contains(member)
                    || request.getMeasuresList().contains(member)
                    || !pulled.add(member)) {
                throw new MetricRefusal(MetricRefusal.INVALID_ENRICHMENT,
                        "enrichment member '" + member + "' collides with another"
                                + " rollup column", List.of());
            }
        }
        if (pulled.isEmpty()) {
            throw new MetricRefusal(MetricRefusal.INVALID_ENRICHMENT,
                    "enrichment names no members to pull", List.of());
        }

        ServedMetricSubject enrichmentSubject = resolve.apply(enrichment.getSubject());
        List<String> lookupMembers = new ArrayList<>();
        lookupMembers.add(key);
        lookupMembers.addAll(pulled);
        QueryMetricsResponse lookup = MetricQueries.lookup(
                enrichmentSubject.mapping(), enrichmentSubject.executors(),
                enrichment.getBackend(), lookupMembers, GROUP_BUDGET);
        if (lookup.getRowCount() >= GROUP_BUDGET) {
            throw new MetricRefusal(MetricRefusal.ROLLUP_BUDGET,
                    "the enrichment lookup on '" + enrichment.getSubject()
                            + "' filled the group budget of " + GROUP_BUDGET
                            + " and cannot attest the join complete", List.of());
        }

        Map<String, MetricRow> byKey = new HashMap<>();
        for (MetricRow row : lookup.getRowsList()) {
            String value = row.getDimensionsOrDefault(key, "");
            if (byKey.put(value, row) != null) {
                // The load-bearing rule: a fan-out would multiply measures,
                // and a rollup is exact or refused.
                throw new MetricRefusal(MetricRefusal.JOIN_FANOUT,
                        "join key '" + key + "' value \"" + value + "\" matches more"
                                + " than one combination on subject '"
                                + enrichment.getSubject() + "'; an enrichment is"
                                + " strictly one to at most one", List.of());
            }
        }

        long unenriched = 0;
        List<MetricRow> enriched = new ArrayList<>(rows.size());
        for (MetricRow row : rows) {
            MetricRow match = byKey.get(row.getDimensionsOrDefault(key, ""));
            if (match == null) {
                unenriched++;
                enriched.add(row);
                continue;
            }
            MetricRow.Builder builder = row.toBuilder();
            for (String member : pulled) {
                builder.putDimensions(member, match.getDimensionsOrDefault(member, ""));
            }
            enriched.add(builder.build());
        }
        List<String> columns = new ArrayList<>(dimensionColumns);
        columns.addAll(pulled);
        return new Enriched(columns, enriched, unenriched);
    }
}
