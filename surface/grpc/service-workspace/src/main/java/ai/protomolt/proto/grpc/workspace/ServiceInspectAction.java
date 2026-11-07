package ai.protomolt.proto.grpc.workspace;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.Fields;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Reply;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.grpc.profile.ServiceProfileRepository;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.registry.SchemaRegistryStore;
import com.google.protobuf.Message;

import com.google.protobuf.Descriptors.Descriptor;
import java.io.IOException;

/** Reads one profile and renders its service/method contracts without returning descriptor bytes. */
public final class ServiceInspectAction implements ProtoAction {

    private final ServiceProfileRepository repository;
    private final SchemaRegistryStore registry;
    private final ProfileCredentialResolver credentials;

    public ServiceInspectAction(ServiceProfileRepository repository, SchemaRegistryStore registry) {
        this(repository, registry, null);
    }

    /** Host credentials resolve endpoint references locally; inspection still never dials out. */
    ServiceInspectAction(ServiceProfileRepository repository, SchemaRegistryStore registry,
                         ProfileCredentialResolver credentials) {
        this.repository = repository;
        this.registry = registry;
        this.credentials = credentials;
    }

    @Override
    public String name() {
        return "service-inspect";
    }

    @Override
    public String requiredScope() {
        return Scopes.SERVICE_INVOKE;
    }

    @Override
    public String description() {
        return "Inspects one registered service: connection profile, method names, streaming shapes, "
                + "request/response types, and their top-level fields, without copying descriptor "
                + "bytes. Endpoint credential references resolve locally; an unresolvable "
                + "reference fails the inspection explicitly.";
    }

    @Override
    public Descriptor requestType() {
        return ServiceActionJson.request("ServiceInspectRequest");
    }

    @Override
    public Descriptor responseType() {
        return ServiceActionJson.request("ServiceInspectResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        ServiceProfileRepository store = ServiceActionSupport.requireRepository(repository);
        String name = Fields.string(input, "name");
        ServiceProfile profile;
        try {
            profile = store.find(name).orElseThrow(() -> ServiceActionSupport.notFound(name));
        } catch (IOException e) {
            throw ServiceActionSupport.storage("read service profile '" + name + "'", e);
        }
        // A profile whose credential reference does not resolve is reported here rather
        // than at the next call; the reference stays opaque and the value is never rendered.
        for (ServiceEndpoint endpoint : profile.getEndpointsList()) {
            ProfileCredentials.requireResolvable(profile, endpoint, credentials);
        }
        Reply result = Reply.of(responseType()).set("profile", profile);
        ServiceActionSupport.writeServices(result, "services", profile, store, registry);
        return result.build();
    }
}
