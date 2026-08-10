package ai.pipestream.proto.chain;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.SchemaResolver;
import ai.pipestream.proto.grpc.recipe.ArtifactRepository;
import ai.pipestream.proto.grpc.recipe.RunEvidenceRepository;
import ai.pipestream.proto.grpc.recipe.v1.GrpcRecipe;
import ai.pipestream.proto.grpc.recipe.v1.RunEvidence;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

/** Offline fixture replay action over repository-backed run evidence. */
final class ReplayRecipeAction implements ProtoAction {

    private final ArtifactRepository artifacts;
    private final RunEvidenceRepository runs;

    ReplayRecipeAction(ArtifactRepository artifacts, RunEvidenceRepository runs) {
        this.artifacts = artifacts;
        this.runs = runs;
    }

    @Override
    public String name() {
        return "replay-recipe";
    }

    @Override
    public String description() {
        return "Replays a recorded recipe run entirely offline from redacted, content-addressed "
                + "fixtures. It detects recipe, descriptor, request, response, mapping, and "
                + "step-order drift without contacting a service.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = RecipeActionJson.schema();
        ObjectNode properties = schema.putObject("properties");
        properties.putObject("recipe").put("type", "object");
        properties.putObject("runId").put("type", "string");
        properties.putObject("schema").put("type", "object")
                .put("description", "The exact descriptors used by the recorded run.");
        schema.putArray("required").add("recipe").add("runId").add("schema");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        if (artifacts == null || runs == null) {
            throw RecipeActionJson.unavailable("recipe replay",
                    "start protomolt-serve with --recipe-workspace");
        }
        GrpcRecipe recipe = (GrpcRecipe) RecipeActionJson.parse(
                RecipeActionJson.object(input, "recipe"), GrpcRecipe.newBuilder(), "/recipe");
        String runId = RecipeActionJson.text(input, "runId");
        RunEvidence evidence;
        RecipeReplay.ReplayResult result;
        try {
            evidence = runs.find(runId).orElseThrow(() ->
                    RecipeActionJson.invalid("No run evidence named '" + runId + "'", "/runId"));
            result = RecipeReplay.replay(recipe, evidence,
                    SchemaResolver.resolve(input, "schema", context).files(), artifacts);
        } catch (ActionException e) {
            throw e;
        } catch (IllegalArgumentException e) {
            throw RecipeActionJson.invalid(e.getMessage(), "/recipe");
        } catch (IOException e) {
            throw new ActionException("repository-failed", "Replay failed: " + e.getMessage());
        }
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("ok", result.ok());
        if (!result.failure().isBlank()) {
            output.put("failure", result.failure());
        }
        ArrayNode steps = output.putArray("steps");
        for (RecipeReplay.StepReplay step : result.steps()) {
            ObjectNode node = steps.addObject();
            node.put("stepName", step.stepName());
            node.put("recordedStatus", step.recordedStatus().name());
            node.put("ok", step.ok());
            node.put("detail", step.detail());
        }
        return output;
    }
}
