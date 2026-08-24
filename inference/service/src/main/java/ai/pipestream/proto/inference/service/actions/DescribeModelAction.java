package ai.pipestream.proto.inference.service.actions;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.CatalogContract;
import ai.pipestream.proto.actions.Fields;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Reply;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.inference.spi.InferenceEngines;
import ai.pipestream.proto.inference.spi.UnknownModelException;
import ai.pipestream.proto.inference.v1.DescribeModelRequest;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

/**
 * The {@code inference-describe-model} verb: one catalog entry, capabilities
 * included — the "tell me about this model before I route work to it" lane.
 * <p>
 * A null facade means inference is not configured on this server; every call
 * then answers {@code unavailable}.
 */
public final class DescribeModelAction implements ProtoAction {

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
    public Message execute(Message input, ActionContext context) throws ActionException {
        InferenceActionSupport.requireEngines(engines);
        String model = Fields.string(input, "model");
        try {
            Reply result = Reply.of(responseType()).set("ok", true);
            InferenceActionSupport.writeEntry(result.nest("entry"),
                    engines.describe(DescribeModelRequest.newBuilder().setModel(model).build())
                            .getEntry());
            return result.build();
        } catch (UnknownModelException e) {
            return Reply.of(responseType())
                    .set("ok", false).set("error", e.getMessage()).build();
        }
    }
}
