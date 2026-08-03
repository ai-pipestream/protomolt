package ai.pipestream.proto.inference.service.actions;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.inference.spi.InferenceEngines;
import ai.pipestream.proto.inference.v1.ListModelsRequest;
import ai.pipestream.proto.inference.v1.ListModelsResponse;
import ai.pipestream.proto.inference.v1.ModelEntry;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
    public String description() {
        return "Lists every model registered in the server's inference catalog: id, provider, "
                + "endpoint, capabilities, and provenance labels, plus the catalog generation "
                + "(a mutation counter — a stale view is detectable).";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = JsonNodeFactory.instance.objectNode();
        schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        schema.put("type", "object");
        schema.putObject("properties").putObject("provider")
                .put("type", "string")
                .put("description", "When set, only entries executed by this provider.");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        InferenceActionSupport.requireEngines(engines);
        String provider = InferenceActionSupport.optionalString(input, "provider");
        ListModelsResponse response = engines.listModels(ListModelsRequest.newBuilder()
                .setProvider(provider == null ? "" : provider)
                .build());
        ObjectNode result = JsonNodeFactory.instance.objectNode();
        result.put("ok", true);
        ArrayNode models = result.putArray("models");
        for (ModelEntry entry : response.getModelsList()) {
            models.add(InferenceActionSupport.entryJson(entry));
        }
        result.put("catalogGeneration", response.getCatalogGeneration());
        return result;
    }
}
