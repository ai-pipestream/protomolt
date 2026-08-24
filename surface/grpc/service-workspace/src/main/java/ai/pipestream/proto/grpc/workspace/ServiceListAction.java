package ai.pipestream.proto.grpc.workspace;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.JsonAction;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.grpc.profile.ServiceProfileRepository;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.google.protobuf.Descriptors.Descriptor;
import java.io.IOException;

/** Lists the durable service identities without loading descriptor artifacts. */
public final class ServiceListAction implements JsonAction {

    private final ServiceProfileRepository repository;

    public ServiceListAction(ServiceProfileRepository repository) {
        this.repository = repository;
    }

    @Override
    public String name() {
        return "service-list";
    }

    @Override
    public String requiredScope() {
        return Scopes.SERVICE_INVOKE;
    }

    @Override
    public String description() {
        return "Lists registered gRPC service profiles with endpoint names and descriptor fingerprints; "
                + "descriptor bytes and credential material are never returned.";
    }

    @Override
    public Descriptor requestType() {
        return ServiceActionJson.request("ServiceListRequest");
    }

    @Override
    public Descriptor responseType() {
        return ServiceActionJson.request("ServiceListResponse");
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        ServiceProfileRepository store = ServiceActionSupport.requireRepository(repository);
        // The request carries no fields, so parsing exists to refuse one that does: a caller
        // sending a filter this verb does not have has asked for something it will not get.
        ServiceActionJson.parse(input, "ServiceListRequest", name());
        ObjectNode result = context.objectMapper().createObjectNode();
        var services = result.putArray("services");
        try {
            store.list().forEach(profile -> services.add(
                    ServiceActionSupport.summary(profile, context.objectMapper())));
        } catch (IOException e) {
            throw ServiceActionSupport.storage("list service profiles", e);
        }
        return result;
    }
}
