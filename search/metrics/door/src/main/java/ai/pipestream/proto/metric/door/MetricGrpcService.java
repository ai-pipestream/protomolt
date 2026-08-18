package ai.pipestream.proto.metric.door;

import ai.pipestream.proto.metric.DescribeMappingRequest;
import ai.pipestream.proto.metric.DescribeMappingResponse;
import ai.pipestream.proto.metric.MemberRef;
import ai.pipestream.proto.metric.MetricServiceGrpc;
import ai.pipestream.proto.metric.QueryMetricsRequest;
import ai.pipestream.proto.metric.QueryMetricsResponse;
import ai.pipestream.proto.metric.RebuildRollupRequest;
import ai.pipestream.proto.metric.RebuildRollupResponse;
import ai.pipestream.proto.metric.spi.MetricQueries;
import ai.pipestream.proto.metric.spi.MetricRefusal;
import ai.pipestream.proto.metric.spi.RollupSink;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The MetricService over a fixed set of served subjects. Shape rules are
 * the validating interceptor's job (mounted by {@link MetricDoorServices}
 * from day one); this service adds only what a schema cannot express:
 * subject membership, member roles, backend mounts, and executor
 * capabilities. Refusals carry the SPI's stable code in brackets so an
 * agent can branch on it without parsing prose.
 *
 * <p>Status mapping: a refusal the caller can fix by renaming something is
 * {@code INVALID_ARGUMENT}; a refusal about what this mount can run
 * (backends, capabilities) is {@code FAILED_PRECONDITION}.</p>
 */
final class MetricGrpcService extends MetricServiceGrpc.MetricServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(MetricGrpcService.class);

    private static final Set<String> PRECONDITIONS = Set.of(
            MetricRefusal.AMBIGUOUS_BACKEND,
            MetricRefusal.UNKNOWN_BACKEND,
            MetricRefusal.UNSUPPORTED_AGGREGATE,
            MetricRefusal.UNSUPPORTED_FILTER,
            MetricRefusal.MISSING_TABLE,
            MetricRefusal.DISTINCT_BOUND,
            MetricRefusal.MISSING_SINK,
            MetricRefusal.ROLLUP_BUDGET);

    /**
     * The most groups one rebuild may hold: the query surface's own limit
     * cap. A result that fills it cannot be attested complete, and a
     * rollup is exact or refused, never truncated.
     */
    static final int ROLLUP_GROUP_BUDGET = 1000;

    private final Map<String, ServedMetricSubject> subjects;
    private final RollupSink rollups;

    MetricGrpcService(Map<String, ServedMetricSubject> subjects, RollupSink rollups) {
        this.subjects = Map.copyOf(subjects);
        this.rollups = rollups;
    }

    @Override
    public void describeMapping(
            DescribeMappingRequest request, StreamObserver<DescribeMappingResponse> observer) {
        run(observer, () -> {
            ServedMetricSubject subject = subject(request.getMappingSubject());
            return MetricQueries.describe(subject.mapping(),
                    List.copyOf(subject.executors().keySet()));
        });
    }

    @Override
    public void queryMetrics(
            QueryMetricsRequest request, StreamObserver<QueryMetricsResponse> observer) {
        run(observer, () -> {
            ServedMetricSubject subject = subject(request.getMappingSubject());
            return MetricQueries.query(subject.mapping(), subject.executors(), request);
        });
    }

    @Override
    public void rebuildRollup(
            RebuildRollupRequest request, StreamObserver<RebuildRollupResponse> observer) {
        run(observer, () -> {
            if (rollups == null) {
                throw new MetricRefusal(MetricRefusal.MISSING_SINK,
                        "this mount has no rollup sink: rollups land in the lake, so"
                                + " mount the metrics role with the Iceberg catalog"
                                + " family", List.of());
            }
            ServedMetricSubject subject = subject(request.getMappingSubject());
            QueryMetricsResponse answer = MetricQueries.query(
                    subject.mapping(), subject.executors(),
                    QueryMetricsRequest.newBuilder()
                            .setMappingSubject(request.getMappingSubject())
                            .setBackend(request.getBackend())
                            .addAllMeasures(request.getMeasuresList())
                            .addAllDimensions(request.getDimensionsList())
                            .addAllFilters(request.getFiltersList())
                            .setLimit(ROLLUP_GROUP_BUDGET)
                            .build());
            if (answer.getRowCount() >= ROLLUP_GROUP_BUDGET) {
                throw new MetricRefusal(MetricRefusal.ROLLUP_BUDGET,
                        "the rollup filled the group budget of " + ROLLUP_GROUP_BUDGET
                                + " and cannot be attested complete; narrow the"
                                + " dimensions or filter the subject", List.of());
            }
            RollupSink.Written written = rollups.replace(
                    request.getTable(),
                    request.getDimensionsList().stream().map(MemberRef::getName).toList(),
                    request.getMeasuresList(),
                    answer.getRowsList());
            return RebuildRollupResponse.newBuilder()
                    .setMappingSubject(answer.getMappingSubject())
                    .setBackend(answer.getBackend())
                    .setTable(written.table())
                    .setRowsWritten(written.rowsWritten())
                    .setSnapshotId(written.snapshotId())
                    .setPhysicalPlan(answer.getPhysicalPlan())
                    .build();
        });
    }

    private ServedMetricSubject subject(String name) {
        ServedMetricSubject subject = subjects.get(name);
        if (subject == null) {
            throw new MetricRefusal(MetricRefusal.UNKNOWN_SUBJECT,
                    "unknown mapping subject '" + name + "'; served subjects: "
                            + String.join(", ", subjects.keySet()),
                    List.copyOf(subjects.keySet()));
        }
        return subject;
    }

    private <T> void run(StreamObserver<T> observer, java.util.function.Supplier<T> work) {
        try {
            observer.onNext(work.get());
            observer.onCompleted();
        } catch (MetricRefusal refusal) {
            Status status = PRECONDITIONS.contains(refusal.code())
                    ? Status.FAILED_PRECONDITION
                    : Status.INVALID_ARGUMENT;
            observer.onError(status.withDescription(
                    "[" + refusal.code() + "] " + refusal.getMessage()).asRuntimeException());
        } catch (RuntimeException e) {
            LOG.error("metric request failed", e);
            observer.onError(Status.INTERNAL
                    .withDescription("metric request failed").asRuntimeException());
        }
    }
}
