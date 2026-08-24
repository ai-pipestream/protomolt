package ai.pipestream.proto.inference.service.actions;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.CatalogContract;
import ai.pipestream.proto.actions.JsonAction;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.inference.spi.InferenceEngines;
import ai.pipestream.proto.inference.spi.UnknownModelException;
import ai.pipestream.proto.inference.v1.DescribeModelRequest;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;

/**
 * The {@code inference-describe-model} verb: one catalog entry, capabilities
 * included — the "tell me about this model before I route work to it" lane.
 * <p>
 * A null facade means inference is not configured on this server; every call
 * then answers {@code unavailable}.
 */
public final class DescribeModelAction implements JsonAction {

    private final InferenceEngines engines;

    /**
     * @param engines the inference facade, or null when inference is not configured
     */
    public DescribeModelAction(InferenceEngines engines) {
        this.engines = engines;
    }

    @Override
    public String name() {
        return "inference-describe-model";
    }

    @Override
    public String requiredScope() {
        return Scopes.SERVICE_INVOKE;
    }

    @Override
    public String description() {
        return "Describes one model in the server's inference catalog by id: provider, endpoint, "
                + "backend model name, capabilities (context window, streaming, thinking, "
                + "modalities), and provenance labels.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("InferenceDescribeModelRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("InferenceDescribeModelResponse");
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        InferenceActionSupport.requireEngines(engines);
        String model = InferenceActionSupport.requireString(input, "model");
        ObjectNode result = JsonNodeFactory.instance.objectNode();
        try {
            result.put("ok", true);
            result.set("entry", InferenceActionSupport.entryJson(
                    engines.describe(DescribeModelRequest.newBuilder().setModel(model).build())
                            .getEntry()));
        } catch (UnknownModelException e) {
            result.put("ok", false);
            result.put("error", e.getMessage());
        }
        return result;
    }
}
