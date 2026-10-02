package ai.protomolt.proto.actions.toolkit;

import ai.protomolt.proto.actions.ActionProvider;
import ai.protomolt.proto.actions.ProtoAction;
import java.util.List;

/** Stateless toolkit operations installed as an optional catalog capability. */
public final class ToolkitActions implements ActionProvider {
    @Override
    public List<? extends ProtoAction> actions() {
        return List.of(new CompileAction(),
                new ValidateMessageAction(),
                new DiffSchemasAction(),
                new CheckCompatAction(),
                new RenderJsonSchemaAction(),
                new RenderPromptAction(),
                new EvalCelAction(),
                new MapMessageAction(),
                new SynthesizeShapeAction(),
                new JoinMessagesAction(),
                new MergeSchemasAction(),
                new CheckRulesAction(),
                new InferSchemaAction(),
                new MaskMessageAction(),
                new ExtractMetadataAction(),
                new ListTypesAction());
    }
}
