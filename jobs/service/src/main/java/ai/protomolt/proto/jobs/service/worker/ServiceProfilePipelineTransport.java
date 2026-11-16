package ai.protomolt.proto.jobs.service.worker;

import ai.protomolt.proto.grpc.invoke.ChannelFactory;
import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.grpc.invoke.UpstreamStatus;
import ai.protomolt.proto.grpc.policy.OutboundChannelPolicyException;
import ai.protomolt.proto.grpc.profile.ServiceProfileRepository;
import ai.protomolt.proto.grpc.profile.ServiceProfileValidation;
import ai.protomolt.proto.grpc.profile.v1.MethodPolicy;
import ai.protomolt.proto.grpc.profile.v1.Operation;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.grpc.profile.v1.Transport;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.pipeline.DynamicPipelineTransport;
import ai.protomolt.proto.pipeline.PipelineTransport;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Policy-enforcing {@link PipelineTransport} for durable pipeline runs.
 *
 * <p>Resolves every {@link ServiceDependency} through the host's {@link ServiceProfileRepository},
 * enforces {@code OutboundChannelPolicy} target, transport, and deadline constraints via
 * {@link ChannelFactory} before opening or reusing a channel, stamps durable operation-identity
 * metadata headers, and closes owned channels when the worker shuts down.
 */
final class ServiceProfilePipelineTransport implements PipelineTransport, AutoCloseable {

    private final PipelineChannelCache channelCache;
    private final ServiceProfileRepository serviceProfiles;
    private final WorkflowRunWorker.EndpointCredentials credentials;

    ServiceProfilePipelineTransport(ChannelFactory channels,
                                    ServiceProfileRepository serviceProfiles,
                                    WorkflowRunWorker.EndpointCredentials credentials) {
        this.channelCache = new PipelineChannelCache(channels);
        this.serviceProfiles = serviceProfiles;
        this.credentials = Objects.requireNonNull(credentials, "credentials");
    }

    String resolveTargetKey(ServiceDependency dependency) {
        if (serviceProfiles != null) {
            try {
                Optional<ServiceProfile> found = serviceProfiles.find(
                        dependency.getServiceProfile());
                if (found.isPresent()) {
                    ServiceEndpoint endpoint = findEndpoint(found.get(), dependency.getEndpoint());
                    if (endpoint != null) {
                        if (!endpoint.getHost().isBlank() && endpoint.getPort() > 0) {
                            return formatTarget(endpoint);
                        }
                        return found.get().getName() + "/" + endpoint.getName();
                    }
                }
            } catch (IOException ignored) {
                // invoke() surfaces repository I/O faults when the step runs.
            }
        }
        return staticDependencyKey(dependency);
    }

    static String staticDependencyKey(ServiceDependency dependency) {
        String profile = dependency.getServiceProfile();
        String endpoint = dependency.getEndpoint();
        if (!profile.isBlank()) {
            return endpoint.isBlank() ? profile : profile + "/" + endpoint;
        }
        return endpoint.isBlank() ? dependency.getAlias() : endpoint;
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
        validateInvokeArguments(dependency, method, requests, deadlineMillis, maxResponses);

        ServiceProfile profile = resolveProfile(dependency);
        ServiceEndpoint endpoint = resolveEndpoint(profile, dependency.getEndpoint());
        long effectiveDeadlineMs =
                applyMethodPolicy(profile, profileMethodName(method), deadlineMillis);
        String target = formatTarget(endpoint);
        boolean tls = endpoint.getTransport() == Transport.TRANSPORT_TLS;
        if ((!endpoint.getTrustRef().isBlank() || !endpoint.getClientCertificateRef().isBlank())
                && !tls) {
            throw new IllegalArgumentException(
                    "trust and client-certificate references require TLS transport");
        }

        Metadata hostHeaders = Objects.requireNonNull(
                credentials.headers(profile, endpoint), "endpoint headers");
        ChannelCredentials customTls = credentials.channelCredentials(profile, endpoint);
        boolean credentialed = !endpoint.getCredentialRef().isBlank() || customTls != null;

        channelCache.validateTargetAndDeadline(target, tls, effectiveDeadlineMs);

        Metadata headers = buildHeaders(hostHeaders, context);
        if (customTls != null) {
            ManagedChannel dedicated = channelCache.openDedicated(target, customTls);
            try {
                return executeCall(dedicated, method, requests, effectiveDeadlineMs, maxResponses,
                        headers, credentialed);
            } finally {
                dedicated.shutdownNow();
            }
        }
        PipelineChannelCache.Lease lease = channelCache.acquireShared(target, tls);
        try {
            return executeCall(lease.channel(), method, requests, effectiveDeadlineMs,
                    maxResponses, headers, credentialed);
        } finally {
            lease.release();
        }
    }

    private void validateInvokeArguments(ServiceDependency dependency,
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
        if (channelCache.isClosed()) {
            throw new IllegalStateException("pipeline transport is closed");
        }
    }

    private ServiceProfile resolveProfile(ServiceDependency dependency) {
        if (serviceProfiles == null) {
            throw new OutboundChannelPolicyException(
                    "service profile repository is not configured; cannot resolve pipeline "
                            + "dependency '" + dependency.getAlias() + "' (profile '"
                            + dependency.getServiceProfile() + "')");
        }
        ServiceProfile profile;
        try {
            profile = serviceProfiles.find(dependency.getServiceProfile())
                    .orElseThrow(() -> new IllegalArgumentException(
                            "service profile '" + dependency.getServiceProfile()
                                    + "' was not found"));
        } catch (IOException e) {
            throw new IllegalStateException(
                    "failed to read service profile '" + dependency.getServiceProfile() + "'", e);
        }
        ServiceProfileValidation.validateConnectionProfile(profile);
        if (profile.hasSchemaSource()) {
            String profileFp = profile.getSchemaSource().getDescriptorFingerprint();
            if (!profileFp.isEmpty() && !profileFp.equals(dependency.getDescriptorFingerprint())) {
                throw new IllegalArgumentException("service profile '" + profile.getName()
                        + "' descriptor fingerprint '" + profileFp
                        + "' does not match pipeline dependency fingerprint '"
                        + dependency.getDescriptorFingerprint() + "'");
            }
        }
        return profile;
    }

    private static ServiceEndpoint findEndpoint(ServiceProfile profile, String requested) {
        if (profile.getEndpointsCount() == 0) {
            return null;
        }
        if (requested == null || requested.isBlank()) {
            return profile.getEndpoints(0);
        }
        for (ServiceEndpoint candidate : profile.getEndpointsList()) {
            if (candidate.getName().equals(requested)) {
                return candidate;
            }
        }
        return null;
    }

    private static ServiceEndpoint resolveEndpoint(ServiceProfile profile, String requested) {
        if (profile.getEndpointsCount() == 0) {
            throw new IllegalArgumentException(
                    "service profile '" + profile.getName() + "' defines no endpoints");
        }
        ServiceEndpoint found = findEndpoint(profile, requested);
        if (found == null) {
            throw new IllegalArgumentException("endpoint '" + requested
                    + "' is not in service profile '" + profile.getName() + "'");
        }
        return found;
    }

    /** A method as service profiles name it: {@code package.Service/Method}. */
    private static String profileMethodName(MethodDescriptor method) {
        return method.getService().getFullName() + "/" + method.getName();
    }

    private static long applyMethodPolicy(ServiceProfile profile, String methodFullName,
                                          long deadlineMillis) {
        MethodPolicy matched = null;
        for (MethodPolicy candidate : profile.getMethodPoliciesList()) {
            if (candidate.getMethod().equals(methodFullName)) {
                matched = candidate;
                break;
            }
        }
        if (matched == null) {
            return deadlineMillis;
        }
        if (matched.getApprovalRequired()
                || matched.getOperationList().contains(Operation.OPERATION_APPROVAL_REQUIRED)) {
            throw new IllegalArgumentException("method '" + methodFullName
                    + "' requires approval in service profile '" + profile.getName() + "'");
        }
        Duration configured = matched.getDeadline();
        if (configured.equals(Duration.getDefaultInstance())) {
            return deadlineMillis;
        }
        long configuredMs = Math.max(1L,
                java.time.Duration.ofSeconds(configured.getSeconds(), configured.getNanos())
                        .toMillis());
        return Math.min(deadlineMillis, configuredMs);
    }

    private static String formatTarget(ServiceEndpoint endpoint) {
        String host = endpoint.getHost();
        if (host.indexOf(':') >= 0 && !host.startsWith("[")) {
            host = "[" + host + "]";
        }
        return host + ":" + endpoint.getPort();
    }

    private static Metadata buildHeaders(Metadata hostHeaders, CallContext context) {
        Metadata headers = new Metadata();
        headers.merge(hostHeaders);
        if (context != null && !context.jobId().isEmpty()) {
            headers.put(DynamicPipelineTransport.JOB_ID_HEADER, context.jobId());
            headers.put(DynamicPipelineTransport.STEP_NAME_HEADER, context.stepName());
            if (!context.branchId().isEmpty()) {
                headers.put(DynamicPipelineTransport.BRANCH_ID_HEADER, context.branchId());
            }
            headers.put(DynamicPipelineTransport.OPERATION_ID_HEADER, context.operationId());
        }
        return headers;
    }

    private static List<DynamicMessage> executeCall(Channel channel,
                                                    MethodDescriptor method,
                                                    List<DynamicMessage> requests,
                                                    long deadlineMillis,
                                                    int maxResponses,
                                                    Metadata headers,
                                                    boolean credentialed) {
        CallOptions options = CallOptions.DEFAULT.withDeadlineAfter(
                deadlineMillis, TimeUnit.MILLISECONDS);
        try {
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
        } catch (StatusRuntimeException e) {
            if (credentialed) {
                throw UpstreamStatus.reply(e.getStatus(), true).asRuntimeException();
            }
            throw e;
        }
    }

    @Override
    public void close() {
        channelCache.close();
    }
}
