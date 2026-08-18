package ai.pipestream.proto.metric.door;

import ai.pipestream.proto.metric.MemberRef;
import ai.pipestream.proto.metric.QueryMetricsRequest;
import ai.pipestream.proto.metric.QueryMetricsResponse;
import ai.pipestream.proto.metric.RebuildRollupRequest;
import ai.pipestream.proto.metric.RebuildRollupResponse;
import ai.pipestream.proto.metric.spi.MetricQueries;
import ai.pipestream.proto.metric.spi.MetricRefusal;
import ai.pipestream.proto.metric.spi.RollupSink;
import java.util.List;

/**
 * One rebuild, shared by the RPC and the catalog verb so the two doors
 * cannot drift: the aggregate query runs on the resolved engine and its
 * complete result replaces the named lake table. Exact or refused: a
 * result that fills the group budget cannot be attested complete, and a
 * mount without a sink refuses by name.
 */
final class Rollups {

    /**
     * The most groups one rebuild may hold: the query surface's own limit
     * cap. A result that fills it cannot be attested complete, and a
     * rollup is exact or refused, never truncated.
     */
    static final int GROUP_BUDGET = 1000;

    private Rollups() {
    }

    static RebuildRollupResponse rebuild(ServedMetricSubject subject,
            RollupSink rollups, RebuildRollupRequest request) {
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
        RollupSink.Written written = rollups.replace(
                request.getMappingSubject(),
                request.getTable(),
                request.getDimensionsList().stream().map(MemberRef::getName).toList(),
                request.getMeasuresList().stream()
                        .map(name -> new RollupSink.MeasureColumn(name,
                                subject.mapping().members().get(name).aggregate()))
                        .toList(),
                answer.getRowsList());
        return RebuildRollupResponse.newBuilder()
                .setMappingSubject(answer.getMappingSubject())
                .setBackend(answer.getBackend())
                .setTable(written.table())
                .setRowsWritten(written.rowsWritten())
                .setSnapshotId(written.snapshotId())
                .setPhysicalPlan(answer.getPhysicalPlan())
                .build();
    }
}
