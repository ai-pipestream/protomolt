package ai.pipestream.proto.grpc.workspace;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.CatalogContract;
import ai.pipestream.proto.actions.Fields;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Reply;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.grpc.invoke.ChannelFactory;
import ai.pipestream.proto.grpc.invoke.ReflectionException;
import ai.pipestream.proto.grpc.profile.ServiceProfileRepository;
import ai.pipestream.proto.grpc.profile.v1.ServiceProfile;
import ai.pipestream.proto.registry.SchemaRegistryStore;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

/** Registers a reflected gRPC service without returning its descriptor bytes to the caller. */
public final class ServiceRegisterAction implements ProtoAction {

    private final ServiceProfileRepository repository;
    private final ChannelFactory channels;
    private final SchemaRegistryStore registry;

    public ServiceRegisterAction(ServiceProfileRepository repository, SchemaRegistryStore registry,
                                 ChannelFactory channels) {
        this.repository = repository;
        this.registry = registry;
        this.channels = channels;
    }

    @Override
    public String name() {
        return "service-register";
    }

    @Override
    public String requiredScope() {
        return Scopes.SERVICE_INVOKE;
    }

    @Override
    public String description() {
        return "Registers a stable gRPC service profile, reflects one named endpoint, stores the "
                + "descriptor set in the schema registry, and returns only profile and "
                + "schema summaries. Credential and key fields are opaque host references.";
    }

    @Override
    public Descriptor requestType() {
        return ServiceActionJson.request("ServiceRegisterRequest");
    }

    @Override
    public Descriptor responseType() {
        return ServiceActionJson.request("ServiceRegisterResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        ServiceProfileRepository store = ServiceActionSupport.requireRepository(repository);
        // The catalog has already parsed and checked the request.
        ServiceProfile profile = CatalogContract.as(
                Fields.message(input, "profile"), ServiceProfile.getDefaultInstance(), name());
        String endpoint = Fields.string(input, "endpoint");
        try {
            ServiceProfile saved = ServiceActionSupport.reflectAndStore(profile,
                    endpoint.isEmpty() ? null : endpoint,
                    deadline(input),
                    store, registry, channels);
            Reply result = Reply.of(responseType()).set("ok", true).set("profile", saved);
            ServiceActionSupport.writeServices(result, "services", saved, store, registry);
            return result.build();
        } catch (ReflectionException e) {
            return Reply.of(responseType())
                    .set("ok", false).set("error", e.getMessage()).build();
        }
    }

    /** The reflection deadline; zero selects the action default, as the message says. */
    private static int deadline(Message input) {
        int asked = Fields.integer(input, "deadlineMs");
        return asked == 0 ? ServiceActionSupport.DEFAULT_DEADLINE_MS : asked;
    }
}
