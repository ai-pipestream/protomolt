package ai.protomolt.proto.grpc.workspace;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.Fields;
import ai.protomolt.proto.actions.Reply;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.actions.StreamEmitter;
import ai.protomolt.proto.actions.StreamingAction;
import ai.protomolt.proto.grpc.invoke.ChannelFactory;
import ai.protomolt.proto.grpc.invoke.GrpcInvokeAction;
import ai.protomolt.proto.grpc.profile.ServiceProfileRepository;
import ai.protomolt.proto.grpc.profile.v1.DescriptorArtifact;
import ai.protomolt.proto.grpc.profile.v1.MethodPolicy;
import ai.protomolt.proto.grpc.profile.v1.Operation;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.grpc.profile.v1.Transport;
import ai.protomolt.proto.registry.SchemaRegistryStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Message;
import io.grpc.Metadata;

import com.google.protobuf.Descriptors.Descriptor;
import java.io.IOException;
import java.time.Duration;
import java.util.Base64;

/** Invokes a registered service while keeping its descriptor inside ProtoMolt. */
public final class ServiceInvokeAction implements StreamingAction {

    private static final int MAX_RESPONSES = 4_096;

    private final ServiceProfileRepository repository;
    private final SchemaRegistryStore registry;
    private final GrpcInvokeAction delegate;
    private final ProfileCredentialResolver credentials;

    public ServiceInvokeAction(ServiceProfileRepository repository, SchemaRegistryStore registry,
                               ChannelFactory channels) {
        this(repository, registry, channels, null);
    }

    public ServiceInvokeAction(ServiceProfileRepository repository, SchemaRegistryStore registry,
                               ChannelFactory channels, ProfileCredentialResolver credentials) {
        this.repository = repository;
        this.registry = registry;
        this.delegate = new GrpcInvokeAction(channels);
        this.credentials = credentials;
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
    public Descriptor requestType() {
        return ServiceActionJson.request("ServiceInvokeRequest");
    }

    @Override
    public Descriptor responseType() {
        return ServiceActionJson.request("ServiceInvokeResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        Invocation invocation = prepare(input, context);
        // The reply is the delegate's plus what only this verb knows: which stored profile
        // and endpoint the call went to, and the descriptor it was typed against.
        return Reply.of(responseType())
                .copyFrom(delegate.execute(invocation.delegateRequest(), context, invocation.headers()))
                .set("serviceProfile", invocation.profile().getName())
                .set("endpoint", invocation.endpoint().getName())
                .set("descriptorFingerprint",
                        invocation.profile().getSchemaSource().getDescriptorFingerprint())
                .build();
    }

    @Override
    public void executeStreaming(Message input, ActionContext context, StreamEmitter emitter)
            throws ActionException {
        Invocation invocation = prepare(input, context);
        delegate.executeStreaming(invocation.delegateRequest(), context, emitter,
                invocation.headers());
    }

    private Invocation prepare(Message input, ActionContext context) throws ActionException {
        // Both the unary and the streaming path come through here.
        ServiceProfileRepository profiles = ServiceActionSupport.requireRepository(repository);
        String name = Fields.string(input, "name");
        String method = Fields.string(input, "method");
        // The request is a structure: its shape is the callee's input type.
        ObjectNode request = Fields.json(input, "request");
        ServiceProfile profile;
        try {
            profile = profiles.find(name).orElseThrow(() -> ServiceActionSupport.notFound(name));
        } catch (IOException e) {
            throw ServiceActionSupport.storage("read service profile '" + name + "'", e);
        }
        String endpointName = Fields.string(input, "endpoint");
        ServiceEndpoint endpoint = ServiceActionSupport.endpoint(profile, endpointName);
        MethodPolicy policy = ProfileMethodPolicy.find(profile, method);
        ProfileMethodPolicy.requireAllowed(policy, method);
        Metadata headers = ProfileCredentials.headers(profile, endpoint, credentials);

        DescriptorArtifact artifact;
        try {
            artifact = ServiceActionSupport.descriptorArtifact(profile, profiles, registry);
        } catch (IOException | IllegalArgumentException e) {
            throw new ActionException("invalid-descriptor", e.getMessage());
        }

        Reply delegated = Reply.of(delegate.requestType())
                .set("target", ServiceActionSupport.target(endpoint))
                .set("method", method)
                .set("request", request)
                .set("tls", endpoint.getTransport() == Transport.TRANSPORT_TLS)
                .set("maxResponses", maxResponses(input))
                .set("deadlineMs", ProfileMethodPolicy.deadline(policy,
                        Fields.integer(input, "deadlineMs")));
        delegated.nest("schema")
                .set("descriptorSetBase64", Base64.getEncoder()
                        .encodeToString(artifact.getDescriptorSet().toByteArray()))
                .build();
        return new Invocation(profile, endpoint, delegated.build(), headers);
    }


    /** How many replies to collect; zero leaves the delegate's own default in place. */
    private static int maxResponses(Message input) throws ActionException {
        int asked = Fields.integer(input, "maxResponses");
        if (asked < 0 || asked > MAX_RESPONSES) {
            throw ServiceActionSupport.invalid(
                    "'maxResponses' must be an integer from 1 to " + MAX_RESPONSES,
                    "/maxResponses");
        }
        return asked;
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
                              Message delegateRequest, Metadata headers) {
    }
}
