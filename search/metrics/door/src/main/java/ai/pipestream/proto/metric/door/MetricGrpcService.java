package ai.pipestream.proto.metric.door;

import ai.pipestream.proto.metric.DescribeMappingRequest;
import ai.pipestream.proto.metric.DescribeMappingResponse;
import ai.pipestream.proto.metric.MetricServiceGrpc;
import ai.pipestream.proto.metric.QueryMetricsRequest;
import ai.pipestream.proto.metric.QueryMetricsResponse;
import ai.pipestream.proto.metric.spi.MetricQueries;
import ai.pipestream.proto.metric.spi.MetricRefusal;
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
            MetricRefusal.MISSING_TABLE);

    private final Map<String, ServedMetricSubject> subjects;

    MetricGrpcService(Map<String, ServedMetricSubject> subjects) {
        this.subjects = Map.copyOf(subjects);
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
