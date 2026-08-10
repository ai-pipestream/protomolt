package ai.pipestream.proto.chain;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.grpc.recipe.ArtifactRepository;
import ai.pipestream.proto.grpc.recipe.RunEvidenceRepository;
import ai.pipestream.proto.grpc.recipe.v1.RunEvidence;
import ai.pipestream.proto.json.MalformedProtobufJsonException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DynamicMessage;

import java.io.IOException;

/** Runs an inline chain and records redacted content-addressed evidence for replay. */
final class RecordRecipeRunAction implements ProtoAction {

    private final ChainRunner runner;
    private final ArtifactRepository artifacts;
    private final RunEvidenceRepository runs;

    RecordRecipeRunAction(ChainRunner runner, ArtifactRepository artifacts,
                          RunEvidenceRepository runs) {
        this.runner = runner;
        this.artifacts = artifacts;
        this.runs = runs;
    }

    @Override
    public String name() {
        return "record-recipe-run";
    }

    @Override
    public String description() {
        return "Executes a checked chain as a draft recipe probe, removes fields marked pii "
                + "or secret before persistence, and stores bounded content-addressed input, "
                + "request, response, and output fixtures plus immutable run evidence.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = RecipeActionJson.schema();
        ObjectNode properties = schema.putObject("properties");
        properties.set("chain", RunChainAction.chainSchema());
        properties.putObject("input").put("type", "object");
        RecipeActionJson.identitySchema(properties, "runId");
        RecipeActionJson.identitySchema(properties, "recipeVersion");
        schema.putArray("required").add("chain").add("input").add("runId");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        if (artifacts == null || runs == null) {
            throw RecipeActionJson.unavailable("recipe run recording",
                    "start protomolt-serve with --recipe-workspace");
        }
        ObjectNode chainNode = RecipeActionJson.object(input, "chain");
        ChainDefinition chain = CompileRecipeAction.parseChecked(chainNode, context);
        ObjectNode inputNode = RecipeActionJson.object(input, "input");
        DynamicMessage message;
        try {
            message = context.transcoder().fromJsonDynamic(inputNode.toString(), chain.inputType());
        } catch (MalformedProtobufJsonException e) {
            throw RecipeActionJson.invalid("Input is not valid proto3 JSON for "
                    + chain.inputType().getFullName() + ": " + e.getMessage(), "/input");
        }
        String runId = RecipeActionJson.identity(input, "runId");
        String version = RecipeActionJson.optionalIdentity(input, "recipeVersion");
        RecipeRunRecorder recorder = new RecipeRunRecorder(runner, artifacts, runs);
        RunEvidence evidence;
        try {
            evidence = recorder.record(runId, version, chain, message);
        } catch (ChainRunner.ChainExecutionException failure) {
            try {
                evidence = runs.find(runId).orElseThrow();
            } catch (Exception missing) {
                throw new ActionException("execution-failed", failure.getMessage());
            }
            ObjectNode output = context.objectMapper().createObjectNode();
            output.put("ok", false);
            output.put("failedStep", failure.step());
            output.put("failureKind", failure.kind().name());
            output.set("evidence", RecipeActionJson.render(evidence, context));
            return output;
        } catch (IllegalArgumentException e) {
            throw RecipeActionJson.invalid(e.getMessage(), "/runId");
        } catch (IOException e) {
            throw new ActionException("repository-failed",
                    "Failed to record recipe run: " + e.getMessage());
        }
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("ok", true);
        output.set("evidence", RecipeActionJson.render(evidence, context));
        return output;
    }
}
