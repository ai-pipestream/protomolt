package ai.pipestream.proto.chain;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.grpc.recipe.ArtifactRepository;
import ai.pipestream.proto.grpc.recipe.RecipeRepository;
import ai.pipestream.proto.grpc.recipe.RunEvidenceRepository;

/** Registers the agent-operable recipe workbench actions with one shared host wiring. */
public final class RecipeWorkbenchActions {

    private RecipeWorkbenchActions() {
    }

    public static ActionCatalog register(ActionCatalog catalog, ChainRunner runner,
                                         ArtifactRepository artifacts,
                                         RunEvidenceRepository runs,
                                         RecipeRepository recipes) {
        return catalog.register(new CompileRecipeAction())
                .register(new SuggestMappingsAction())
                .register(new RecordRecipeRunAction(runner, artifacts, runs))
                .register(new ReplayRecipeAction(artifacts, runs))
                .register(new PromoteRecipeAction(recipes));
    }
}
