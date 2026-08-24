package ai.pipestream.proto.inference.service.actions;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.CatalogContract;
import ai.pipestream.proto.actions.Fields;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Reply;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.inference.spi.InferenceEngines;
import ai.pipestream.proto.inference.v1.ListModelsRequest;
import ai.pipestream.proto.inference.v1.ListModelsResponse;
import ai.pipestream.proto.inference.v1.ModelEntry;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

/**
 * The {@code inference-list-models} verb: reads the server's inference model
 * catalog — which models this server can run, on which providers, with which
 * capabilities. The discovery lane for callers of {@code inference-generate}.
 * <p>
 * A null facade means inference is not configured on this server; every call
 * then answers {@code unavailable}.
 */
public final class ListModelsAction implements ProtoAction {

    private final InferenceEngines engines;

    /**
     * @param engines the inference facade, or null when inference is not configured
     */
    public ListModelsAction(InferenceEngines engines) {
        this.engines = engines;
    }

    @Override
    public String name() {
        return "inference-list-models";
    }

    @Override
    public String requiredScope() {
        return Scopes.SERVICE_INVOKE;
    }

    @Override
    public String description() {
        return "Lists every model registered in the server's inference catalog: id, provider, "
                + "endpoint, capabilities, and provenance labels, plus the catalog generation "
                + "(a mutation counter — a stale view is detectable).";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("InferenceListModelsRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("InferenceListModelsResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        InferenceActionSupport.requireEngines(engines);
        // An omitted provider arrives as the empty string, which lists every provider.
        ListModelsResponse response = engines.listModels(ListModelsRequest.newBuilder()
                .setProvider(Fields.string(input, "provider"))
                .build());
        Reply result = Reply.of(responseType()).set("ok", true);
        for (ModelEntry entry : response.getModelsList()) {
            InferenceActionSupport.writeEntry(result.append("models"), entry);
        }
        return result.set("catalogGeneration", response.getCatalogGeneration()).build();
    }
}
