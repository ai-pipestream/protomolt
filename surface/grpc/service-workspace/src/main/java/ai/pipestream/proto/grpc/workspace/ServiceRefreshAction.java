package ai.pipestream.proto.grpc.workspace;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.Fields;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Reply;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.grpc.invoke.ChannelFactory;
import ai.pipestream.proto.grpc.invoke.ReflectionException;
import ai.pipestream.proto.grpc.profile.ServiceProfileRepository;
import ai.pipestream.proto.grpc.profile.v1.ServiceProfile;
import ai.pipestream.proto.registry.SchemaRegistryStore;
import com.google.protobuf.Message;

import com.google.protobuf.Descriptors.Descriptor;
import java.io.IOException;

/** Re-reflects a stored profile and advances its content-addressed schema identity. */
public final class ServiceRefreshAction implements ProtoAction {

    private final ServiceProfileRepository repository;
    private final ProfileStored stored;
    private final ChannelFactory channels;
    private final SchemaRegistryStore registry;

    public ServiceRefreshAction(ServiceProfileRepository repository, SchemaRegistryStore registry,
                                ChannelFactory channels) {
        this(repository, registry, channels, null);
    }

    ServiceRefreshAction(ServiceProfileRepository repository, SchemaRegistryStore registry,
                         ChannelFactory channels, ProfileStored stored) {
        this.repository = repository;
        this.registry = registry;
        this.channels = channels;
        this.stored = stored;
    }

    @Override
    public String name() {
        return "service-refresh";
    }

    @Override
    public String requiredScope() {
        return Scopes.SERVICE_INVOKE;
    }

    @Override
    public String description() {
        return "Re-reflects a registered service endpoint, stores the descriptor as a "
                + "content-addressed registry artifact, and updates the profile fingerprint.";
    }

    @Override
    public Descriptor requestType() {
        return ServiceActionJson.request("ServiceRefreshRequest");
    }

    @Override
    public Descriptor responseType() {
        return ServiceActionJson.request("ServiceRefreshResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        ServiceProfileRepository store = ServiceActionSupport.requireRepository(repository);
        // The catalog has already parsed and checked the request.
        String name = Fields.string(input, "name");
        ServiceProfile profile;
        try {
            profile = store.find(name).orElseThrow(() -> ServiceActionSupport.notFound(name));
        } catch (IOException e) {
            throw ServiceActionSupport.storage("read service profile '" + name + "'", e);
        }
        String endpoint = Fields.string(input, "endpoint");
        try {
            ServiceProfile refreshed = ServiceActionSupport.reflectAndStore(profile,
                    endpoint.isEmpty() ? null : endpoint,
                    deadline(input),
                    store, registry, channels);
            if (stored != null) {
                stored.stored(refreshed);
            }
            Reply result = Reply.of(responseType())
                    .set("ok", true)
                    .set("changed", !refreshed.getSchemaSource().getDescriptorFingerprint()
                            .equals(profile.getSchemaSource().getDescriptorFingerprint()))
                    .set("profile", refreshed);
            ServiceActionSupport.writeServices(result, "services", refreshed, store, registry);
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
