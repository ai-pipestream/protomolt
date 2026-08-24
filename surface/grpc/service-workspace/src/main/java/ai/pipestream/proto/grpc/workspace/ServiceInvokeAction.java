package ai.pipestream.proto.grpc.workspace;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.actions.StreamEmitter;
import ai.pipestream.proto.actions.StreamingAction;
import ai.pipestream.proto.grpc.invoke.ChannelFactory;
import ai.pipestream.proto.grpc.invoke.GrpcInvokeAction;
import ai.pipestream.proto.grpc.profile.ServiceProfileRepository;
import ai.pipestream.proto.grpc.profile.v1.DescriptorArtifact;
import ai.pipestream.proto.grpc.profile.v1.MethodPolicy;
import ai.pipestream.proto.grpc.profile.v1.Operation;
import ai.pipestream.proto.grpc.profile.v1.ServiceEndpoint;
import ai.pipestream.proto.grpc.profile.v1.ServiceProfile;
import ai.pipestream.proto.grpc.profile.v1.Transport;
import ai.pipestream.proto.registry.SchemaRegistryStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.time.Duration;
import java.util.Base64;

/** Invokes a registered service while keeping its descriptor inside ProtoMolt. */
public final class ServiceInvokeAction implements StreamingAction {

    private static final int MAX_RESPONSES = 4_096;

    private final ServiceProfileRepository repository;
    private final SchemaRegistryStore registry;
    private final GrpcInvokeAction delegate;

    public ServiceInvokeAction(ServiceProfileRepository repository, SchemaRegistryStore registry,
                               ChannelFactory channels) {
        this.repository = repository;
        this.registry = registry;
        this.delegate = new GrpcInvokeAction(channels);
    }

    @Override
    public String name() {
        return "service-invoke";
    }

    @Override
    public String requiredScope() {
        return Scopes.SERVICE_INVOKE;
    }

    @Override
    public String description() {
        return "Invokes a registered unary or server-streaming gRPC method by service profile. "
                + "ProtoMolt resolves the pinned descriptor from its schema registry, so callers "
                + "send only the profile name, method, endpoint choice, and request JSON. One call "
                + "is attempted; methods requiring approval are refused.";
    }

    @Override
    public ObjectNode inputSchema() {
        return ServiceActionJson.schemaFor("ServiceInvokeRequest");
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        Invocation invocation = prepare(input, context);
        ObjectNode result = delegate.execute(invocation.delegateInput(), context);
        result.put("serviceProfile", invocation.profile().getName());
        result.put("endpoint", invocation.endpoint().getName());
        result.put("descriptorFingerprint",
                invocation.profile().getSchemaSource().getDescriptorFingerprint());
        return result;
    }

    @Override
    public void executeStreaming(ObjectNode input, ActionContext context, StreamEmitter emitter)
            throws ActionException {
        delegate.executeStreaming(prepare(input, context).delegateInput(), context, emitter);
    }

    private Invocation prepare(ObjectNode input, ActionContext context) throws ActionException {
        // Both the unary and the streaming path come through here, so the contract is
        // enforced once for either.
        ServiceActionJson.parse(input, "ServiceInvokeRequest", name());
        ServiceProfileRepository profiles = ServiceActionSupport.requireRepository(repository);
        String name = ServiceActionSupport.requireString(input, "name");
        String method = ServiceActionSupport.requireString(input, "method");
        JsonNode request = input.get("request");
        if (request == null || !request.isObject()) {
            throw ServiceActionSupport.invalid("'request' must be an object", "/request");
        }
        ServiceProfile profile;
        try {
            profile = profiles.find(name).orElseThrow(() -> ServiceActionSupport.notFound(name));
        } catch (IOException e) {
            throw ServiceActionSupport.storage("read service profile '" + name + "'", e);
        }
        String endpointName = optionalString(input, "endpoint");
        ServiceEndpoint endpoint = ServiceActionSupport.endpoint(profile, endpointName);
        rejectUnresolvedTransport(endpoint);
        MethodPolicy policy = profile.getMethodPoliciesList().stream()
                .filter(candidate -> candidate.getMethod().equals(method))
                .findFirst()
                .orElse(null);
        rejectApprovalRequired(policy, method);

        DescriptorArtifact artifact;
        try {
            artifact = ServiceActionSupport.descriptorArtifact(profile, profiles, registry);
        } catch (IOException | IllegalArgumentException e) {
            throw new ActionException("invalid-descriptor", e.getMessage());
        }

        ObjectNode delegated = context.objectMapper().createObjectNode();
        delegated.put("target", ServiceActionSupport.target(endpoint));
        delegated.put("method", method);
        delegated.set("request", request.deepCopy());
        delegated.putObject("schema").put("descriptorSetBase64",
                Base64.getEncoder().encodeToString(artifact.getDescriptorSet().toByteArray()));
        delegated.put("tls", endpoint.getTransport() == Transport.TRANSPORT_TLS);
        copyMaxResponses(input, delegated);
        delegated.put("deadlineMs", effectiveDeadline(input, policy));
        return new Invocation(profile, endpoint, delegated);
    }

    private static void rejectUnresolvedTransport(ServiceEndpoint endpoint) throws ActionException {
        if (!endpoint.getCredentialRef().isBlank() || !endpoint.getTrustRef().isBlank()
                || !endpoint.getClientCertificateRef().isBlank()) {
            throw new ActionException("unsupported-transport",
                    "invocation with credential, custom-trust, or client-certificate references "
                            + "requires a configured credential resolver");
        }
    }

    private static void rejectApprovalRequired(MethodPolicy policy, String method)
            throws ActionException {
        if (policy != null && (policy.getApprovalRequired()
                || policy.getOperationList().contains(Operation.OPERATION_APPROVAL_REQUIRED))) {
            throw new ActionException("approval-required",
                    "method '" + method + "' requires approval outside this action");
        }
    }

    private static int effectiveDeadline(ObjectNode input, MethodPolicy policy)
            throws ActionException {
        boolean supplied = input.hasNonNull("deadlineMs");
        int requested = supplied ? ServiceActionSupport.deadline(input)
                : ServiceActionSupport.DEFAULT_DEADLINE_MS;
        if (policy == null || policy.getDeadline().equals(
                com.google.protobuf.Duration.getDefaultInstance())) {
            return requested;
        }
        Duration configured = Duration.ofSeconds(policy.getDeadline().getSeconds(),
                policy.getDeadline().getNanos());
        long configuredMs = Math.max(1, configured.toMillis());
        if (configuredMs > Integer.MAX_VALUE) {
            throw new ActionException("invalid-profile",
                    "method deadline exceeds the supported millisecond range");
        }
        if (supplied && requested > configuredMs) {
            throw ServiceActionSupport.invalid("'deadlineMs' exceeds the registered method policy",
                    "/deadlineMs");
        }
        return supplied ? requested : Math.toIntExact(configuredMs);
    }

    private static void copyMaxResponses(ObjectNode input, ObjectNode delegated)
            throws ActionException {
        JsonNode value = input.get("maxResponses");
        if (value == null || value.isNull()) {
            return;
        }
        if (!value.isIntegralNumber() || !value.canConvertToInt() || value.asInt() <= 0
                || value.asInt() > MAX_RESPONSES) {
            throw ServiceActionSupport.invalid("'maxResponses' must be an integer from 1 to "
                            + MAX_RESPONSES,
                    "/maxResponses");
        }
        delegated.put("maxResponses", value.asInt());
    }

    private static String optionalString(ObjectNode input, String field) throws ActionException {
        JsonNode value = input.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        if (!value.isTextual() || value.asText().isBlank()) {
            throw ServiceActionSupport.invalid("'" + field + "' must be a non-empty string",
                    "/" + field);
        }
        return value.asText();
    }

    private record Invocation(ServiceProfile profile, ServiceEndpoint endpoint,
                              ObjectNode delegateInput) {
    }
}
