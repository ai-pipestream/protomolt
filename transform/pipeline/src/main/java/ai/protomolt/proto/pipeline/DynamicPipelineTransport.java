package ai.protomolt.proto.pipeline;

import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.Metadata;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Descriptor-driven {@link PipelineTransport} over host-resolved gRPC channels. */
public final class DynamicPipelineTransport implements PipelineTransport {

    /** Durable job ID metadata header stamped on coordinated pipeline RPCs. */
    public static final Metadata.Key<String> JOB_ID_HEADER =
            Metadata.Key.of("x-protomolt-job-id", Metadata.ASCII_STRING_MARSHALLER);

    /** Pipeline step name metadata header stamped on coordinated pipeline RPCs. */
    public static final Metadata.Key<String> STEP_NAME_HEADER =
            Metadata.Key.of("x-protomolt-step-name", Metadata.ASCII_STRING_MARSHALLER);

    /** Fan-out branch ID metadata header stamped on coordinated fan-out branch RPCs. */
    public static final Metadata.Key<String> BRANCH_ID_HEADER =
            Metadata.Key.of("x-protomolt-branch-id", Metadata.ASCII_STRING_MARSHALLER);

    /** Stable idempotency key ({@code jobId/stepName} or {@code jobId/step#index}) header. */
    public static final Metadata.Key<String> OPERATION_ID_HEADER =
            Metadata.Key.of("x-protomolt-operation-id", Metadata.ASCII_STRING_MARSHALLER);

    /** Resolves an opaque service-profile dependency to a host-owned channel. */
    @FunctionalInterface
    public interface ChannelResolver {
        Channel resolve(ServiceDependency dependency);
    }

    /** Supplies host-owned call metadata, such as credentials, without persisting it. */
    @FunctionalInterface
    public interface MetadataResolver {
        Metadata resolve(ServiceDependency dependency);
    }

    private final ChannelResolver channels;
    private final MetadataResolver metadata;

    public DynamicPipelineTransport(ChannelResolver channels) {
        this(channels, dependency -> new Metadata());
    }

    public DynamicPipelineTransport(ChannelResolver channels, MetadataResolver metadata) {
        this.channels = Objects.requireNonNull(channels, "channels");
        this.metadata = Objects.requireNonNull(metadata, "metadata");
    }

    @Override
    public List<DynamicMessage> invoke(ServiceDependency dependency,
                                       MethodDescriptor method,
                                       List<DynamicMessage> requests,
                                       long deadlineMillis,
                                       int maxResponses) {
        return invoke(null, dependency, method, requests, deadlineMillis, maxResponses);
    }

    @Override
    public List<DynamicMessage> invoke(CallContext context,
                                       ServiceDependency dependency,
                                       MethodDescriptor method,
                                       List<DynamicMessage> requests,
                                       long deadlineMillis,
                                       int maxResponses) {
        Objects.requireNonNull(dependency, "dependency");
        Objects.requireNonNull(method, "method");
        Objects.requireNonNull(requests, "requests");
        if (deadlineMillis <= 0) {
            throw new IllegalArgumentException("deadlineMillis must be positive");
        }
        if (maxResponses <= 0) {
            throw new IllegalArgumentException("maxResponses must be positive");
        }
        if (!method.isClientStreaming() && requests.size() != 1) {
            throw new IllegalArgumentException(method.getFullName()
                    + " takes exactly one request; got " + requests.size());
        }
        Channel channel = Objects.requireNonNull(channels.resolve(dependency),
                "channel resolver returned null");
        Metadata resolved = Objects.requireNonNull(metadata.resolve(dependency),
                "metadata resolver returned null");
        Metadata headers = new Metadata();
        headers.merge(resolved);
        if (context != null && !context.jobId().isEmpty()) {
            headers.put(JOB_ID_HEADER, context.jobId());
            headers.put(STEP_NAME_HEADER, context.stepName());
            if (!context.branchId().isEmpty()) {
                headers.put(BRANCH_ID_HEADER, context.branchId());
            }
            headers.put(OPERATION_ID_HEADER, context.operationId());
        }
        CallOptions options = CallOptions.DEFAULT.withDeadlineAfter(
                deadlineMillis, TimeUnit.MILLISECONDS);
        if (!method.isClientStreaming()) {
            return DynamicGrpcCalls.call(channel, method, requests.getFirst(), options,
                    headers, maxResponses);
        }
        if (!method.isServerStreaming()) {
            return List.of(DynamicGrpcCalls.callClientStreaming(channel, method,
                    requests.iterator(), options, headers));
        }
        return DynamicGrpcCalls.callBidiStreaming(channel, method, requests.iterator(),
                options, headers, maxResponses);
    }
}
