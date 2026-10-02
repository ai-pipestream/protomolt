package ai.protomolt.proto.actions.index;

import ai.protomolt.proto.actions.ActionProvider;
import ai.protomolt.proto.actions.ProtoAction;
import java.util.List;

/** Installs index artifact rendering when the indexing action module is on the classpath. */
public final class IndexActions implements ActionProvider {
    @Override
    public List<? extends ProtoAction> actions() {
        return List.of(new RenderIndexMappingsAction());
    }
}
