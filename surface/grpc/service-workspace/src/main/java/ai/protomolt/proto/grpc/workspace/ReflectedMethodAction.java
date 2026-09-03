package ai.protomolt.proto.grpc.workspace;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.grpc.invoke.ChannelFactory;
import ai.protomolt.proto.grpc.invoke.DynamicGrpcCalls;
import ai.protomolt.proto.grpc.profile.ServiceProfileRepository;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.grpc.profile.v1.Transport;
import ai.protomolt.proto.registry.SchemaRegistryStore;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * One method of a registered service, as a verb.
 *
 * <p>The method's own request and response messages are the verb's contract, so the schema a
 * caller reads is the service's own and the reply is the service's own. Nothing is
 * transcoded on the way through: the request arrives as a message and is sent as one.
 *
 * <p>The endpoint is resolved per call rather than captured, so a profile that is refreshed
 * or re-pointed takes effect on the next call without re-registering the verb.
 */
final class ReflectedMethodAction implements ProtoAction {

    private final String name;
    private final String profileName;
    private final MethodDescriptor method;
    private final ServiceProfileRepository repository;
    private final SchemaRegistryStore registry;
    private final ChannelFactory channels;

    ReflectedMethodAction(String name, String profileName, MethodDescriptor method,
                          ServiceProfileRepository repository, SchemaRegistryStore registry,
                          ChannelFactory channels) {
        this.name = name;
        this.profileName = profileName;
        this.method = method;
        this.repository = repository;
        this.registry = registry;
        this.channels = channels;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String requiredScope() {
        return Scopes.SERVICE_INVOKE;
    }

    @Override
    public String description() {
        return "Calls " + method.getFullName() + " on the registered service '" + profileName
                + "'. The request and reply are the service's own messages.";
    }

    @Override
    public Descriptor requestType() {
        return method.getInputType();
    }

    @Override
    public Descriptor responseType() {
        return method.getOutputType();
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        ServiceProfile profile = profile();
        ServiceEndpoint endpoint = ServiceActionSupport.endpoint(profile, "");
        ManagedChannel channel = channels.open(ServiceActionSupport.target(endpoint),
                endpoint.getTransport() == Transport.TRANSPORT_TLS);
        try {
            List<DynamicMessage> responses = DynamicGrpcCalls.call(channel, method,
                    CatalogContract.as(input, DynamicMessage.getDefaultInstance(
                            method.getInputType()), name),
                    CallOptions.DEFAULT.withDeadlineAfter(
                            ServiceActionSupport.DEFAULT_DEADLINE_MS, TimeUnit.MILLISECONDS),
                    new Metadata(), 1);
            if (responses.isEmpty()) {
                throw new ActionException("empty-response",
                        method.getFullName() + " answered with no message");
            }
            // A server-streaming method is driven by one request and answers with the first
            // reply; a caller who wants the rest of the stream uses service-invoke.
            return responses.get(0);
        } catch (StatusRuntimeException e) {
            throw new ActionException("upstream-" + e.getStatus().getCode().name()
                    .toLowerCase(java.util.Locale.ROOT).replace('_', '-'),
                    method.getFullName() + " failed: " + e.getStatus().getDescription());
        } finally {
            channel.shutdownNow();
        }
    }

    /** The profile as it stands now, so a refresh reaches the next call. */
    private ServiceProfile profile() throws ActionException {
        try {
            return repository.find(profileName)
                    .orElseThrow(() -> ServiceActionSupport.notFound(profileName));
        } catch (IOException e) {
            throw ServiceActionSupport.storage("read service profile '" + profileName + "'", e);
        }
    }
}
