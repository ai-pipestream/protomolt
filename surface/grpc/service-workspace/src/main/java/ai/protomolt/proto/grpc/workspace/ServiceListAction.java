package ai.protomolt.proto.grpc.workspace;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Reply;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.grpc.profile.ServiceProfileRepository;
import com.google.protobuf.Message;

import com.google.protobuf.Descriptors.Descriptor;
import java.io.IOException;

/** Lists the durable service identities without loading descriptor artifacts. */
public final class ServiceListAction implements ProtoAction {

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
    public Message execute(Message input, ActionContext context) throws ActionException {
        ServiceProfileRepository store = ServiceActionSupport.requireRepository(repository);
        // The request carries no fields; a caller sending a filter this verb does not have
        // is refused by the contract before dispatch.
        Reply result = Reply.of(responseType());
        try {
            store.list().forEach(profile ->
                    ServiceActionSupport.writeSummary(result.append("services"), profile));
        } catch (IOException e) {
            throw ServiceActionSupport.storage("list service profiles", e);
        }
        return result.build();
    }
}
