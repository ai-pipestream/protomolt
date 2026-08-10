package ai.pipestream.proto.chain;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.grpc.recipe.RecipeValidation;
import ai.pipestream.proto.grpc.recipe.v1.GrpcRecipe;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

/** Compiles an existing checked chain definition into the durable recipe contract. */
final class CompileRecipeAction implements ProtoAction {

    @Override
    public String name() {
        return "compile-recipe";
    }

    @Override
    public String description() {
        return "Compiles a descriptor-grounded chain into a deterministic gRPC recipe. "
                + "Every method, mapping, gate, deadline, and descriptor fingerprint is "
                + "checked before the recipe is returned.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = RecipeActionJson.schema();
        schema.putObject("properties").set("chain", RunChainAction.chainSchema());
        schema.putArray("required").add("chain");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        ChainDefinition chain = parseChecked(RecipeActionJson.object(input, "chain"), context);
        GrpcRecipe recipe;
        try {
            recipe = ChainRecipeCompiler.compile(chain);
        } catch (IllegalArgumentException e) {
            throw RecipeActionJson.invalid(e.getMessage(), "/chain");
        }
        ObjectNode output = context.objectMapper().createObjectNode();
        output.set("recipe", RecipeActionJson.render(recipe, context));
        output.put("recipeFingerprint", RecipeValidation.fingerprint(recipe));
        return output;
    }

    static ChainDefinition parseChecked(ObjectNode chain, ActionContext context)
            throws ActionException {
        ChainDefinition definition;
        try {
            definition = ChainJson.parse(chain, context);
        } catch (ChainJson.ChainParseException e) {
            throw RecipeActionJson.invalid(e.getMessage(), e.step.isBlank()
                    ? "/chain" : "/chain/steps/" + e.step);
        }
        List<ChainVerifier.Finding> findings = new ChainVerifier().verify(definition);
        if (!findings.isEmpty()) {
            ObjectNode details = context.objectMapper().createObjectNode();
            ArrayNode nodes = details.putArray("findings");
            for (ChainVerifier.Finding finding : findings) {
                ObjectNode node = nodes.addObject();
                node.put("step", finding.step());
                node.put("kind", finding.kind());
                node.put("error", finding.error());
            }
            throw new ActionException("chain-invalid",
                    "Chain has " + findings.size() + " static finding(s)", details);
        }
        return definition;
    }
}
