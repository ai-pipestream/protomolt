package ai.pipestream.proto.chain;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.grpc.recipe.RecipeRepository;
import ai.pipestream.proto.grpc.recipe.RecipeValidation;
import ai.pipestream.proto.grpc.recipe.v1.GrpcRecipe;
import ai.pipestream.proto.grpc.recipe.v1.VersionedRecipe;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Timestamp;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;

/** Promotes validated recipe content as one immutable registry version. */
final class PromoteRecipeAction implements ProtoAction {

    private final RecipeRepository recipes;
    private final Clock clock;

    PromoteRecipeAction(RecipeRepository recipes) {
        this(recipes, Clock.systemUTC());
    }

    PromoteRecipeAction(RecipeRepository recipes, Clock clock) {
        this.recipes = recipes;
        this.clock = clock;
    }

    @Override
    public String name() {
        return "promote-recipe";
    }

    @Override
    public String description() {
        return "Promotes validated recipe content under an immutable version in the mounted "
                + "git registry. Re-promoting identical content is idempotent; changing an "
                + "existing version fails.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = RecipeActionJson.schema();
        ObjectNode properties = schema.putObject("properties");
        properties.putObject("recipe").put("type", "object");
        properties.putObject("version").put("type", "string");
        schema.putArray("required").add("recipe").add("version");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        if (recipes == null) {
            throw RecipeActionJson.unavailable("recipe promotion",
                    "start protomolt-serve with --registry-git");
        }
        GrpcRecipe recipe = (GrpcRecipe) RecipeActionJson.parse(
                RecipeActionJson.object(input, "recipe"), GrpcRecipe.newBuilder(), "/recipe");
        String version = RecipeActionJson.text(input, "version");
        Instant now = clock.instant();
        VersionedRecipe promoted = VersionedRecipe.newBuilder()
                .setRecipe(recipe)
                .setVersion(version)
                .setRecipeFingerprint(RecipeValidation.fingerprint(recipe))
                .setCreatedAt(Timestamp.newBuilder().setSeconds(now.getEpochSecond())
                        .setNanos(now.getNano()))
                .build();
        try {
            RecipeValidation.validate(promoted);
            recipes.save(promoted);
        } catch (IllegalArgumentException e) {
            throw RecipeActionJson.invalid(e.getMessage(), "/recipe");
        } catch (IOException e) {
            throw new ActionException("repository-failed",
                    "Failed to promote recipe: " + e.getMessage());
        }
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("promoted", true);
        output.set("versionedRecipe", RecipeActionJson.render(promoted, context));
        return output;
    }
}
