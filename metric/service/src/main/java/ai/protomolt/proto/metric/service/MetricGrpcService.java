package ai.protomolt.proto.metric.service;

import ai.protomolt.proto.metric.DescribeMappingRequest;
import ai.protomolt.proto.metric.DescribeMappingResponse;
import ai.protomolt.proto.metric.MetricServiceGrpc;
import ai.protomolt.proto.metric.QueryMetricsRequest;
import ai.protomolt.proto.metric.QueryMetricsResponse;
import ai.protomolt.proto.metric.RebuildRollupRequest;
import ai.protomolt.proto.metric.RebuildRollupResponse;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.MetricAccess;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.metric.spi.MetricQueries;
import ai.protomolt.proto.metric.spi.MetricRefusal;
import ai.protomolt.proto.metric.spi.RollupSink;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The MetricService over a fixed set of served subjects. Shape rules are
 * the validating interceptor's job (mounted by {@link MetricServices}
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

    private final Map<String, ServedMetricSubject> subjects;
    private final RollupSink rollups;
    private final ai.protomolt.proto.metric.spi.MetricSubjectResolver resolver;
    private final MetricAccessRewrites rewrites;

    MetricGrpcService(Map<String, ServedMetricSubject> subjects, RollupSink rollups,
            ai.protomolt.proto.metric.spi.MetricSubjectResolver resolver,
            java.util.function.Supplier<ai.protomolt.proto.authz.AccessPolicy> accessPolicy) {
        this.subjects = Map.copyOf(subjects);
        this.rollups = rollups;
        this.resolver = resolver;
        this.rewrites = new MetricAccessRewrites(accessPolicy);
    }

    @Override
    public void describeMapping(
            DescribeMappingRequest request, StreamObserver<DescribeMappingResponse> observer) {
        Caller caller = CallerContexts.current();
        run(observer, () -> {
            ServedMetricSubject subject = restricted(caller, request.getMappingSubject());
            DescribeMappingResponse described = MetricQueries.describe(subject.mapping(),
                    List.copyOf(subject.executors().keySet()));
            MetricAccess access = rewrites.accessFor(caller);
            return access == null ? described : MetricAccessRewrites.filter(
                    described, access, request.getMappingSubject());
        });
    }

    @Override
    public void queryMetrics(
            QueryMetricsRequest request, StreamObserver<QueryMetricsResponse> observer) {
        Caller caller = CallerContexts.current();
        run(observer, () -> {
            ServedMetricSubject subject = restricted(caller, request.getMappingSubject());
            MetricAccess access = rewrites.accessFor(caller);
            QueryMetricsRequest effective = access == null ? request
                    : MetricAccessRewrites.rewrite(request, access, caller,
                            subject.mapping());
            return MetricQueries.query(subject.mapping(), subject.executors(), effective);
        });
    }

    @Override
    public void rebuildRollup(
            RebuildRollupRequest request, StreamObserver<RebuildRollupResponse> observer) {
        Caller caller = CallerContexts.current();
        run(observer, () -> {
            if (rewrites.accessFor(caller) != null) {
                throw new StatusRuntimeException(Status.PERMISSION_DENIED.withDescription(
                        "caller '" + caller.name() + "' carries metric access rules; a"
                                + " rebuilt rollup is an unrestricted reduction, so the"
                                + " rebuild verb is refused"));
            }
            return Rollups.rebuild(
                    subject(request.getMappingSubject()), rollups, request, this::subject);
        });
    }

    /**
     * The served subject, additionally fail-closed for restricted principals: a subject
     * this mount does not serve statically (a rollup table through the resolver) carries
     * no mapping-level rules to enforce, so a caller with metric access rules is refused
     * it rather than served an unrestricted view.
     */
    private ServedMetricSubject restricted(Caller caller, String name) {
        if (rewrites.accessFor(caller) != null && !subjects.containsKey(name)) {
            throw new StatusRuntimeException(Status.PERMISSION_DENIED.withDescription(
                    "caller '" + caller.name() + "' carries metric access rules, which"
                            + " subject '" + name + "' (not statically served here)"
                            + " cannot enforce"));
        }
        return subject(name);
    }

    private ServedMetricSubject subject(String name) {
        return Subjects.find(subjects, resolver, name);
    }

    private <T> void run(StreamObserver<T> observer, java.util.function.Supplier<T> work) {
        try {
            observer.onNext(work.get());
            observer.onCompleted();
        } catch (StatusRuntimeException refusal) {
            observer.onError(refusal);
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
