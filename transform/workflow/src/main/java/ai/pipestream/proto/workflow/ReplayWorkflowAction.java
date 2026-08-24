package ai.pipestream.proto.workflow;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.CatalogContract;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.SchemaResolver;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.grpc.workflow.ArtifactRepository;
import ai.pipestream.proto.grpc.workflow.RunEvidenceRepository;
import ai.pipestream.proto.grpc.workflow.v1.Workflow;
import ai.pipestream.proto.grpc.workflow.v1.RunEvidence;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import com.google.protobuf.Descriptors.Descriptor;

/** Offline fixture replay action over repository-backed run evidence. */
final class ReplayWorkflowAction implements ProtoAction {

    private final ArtifactRepository artifacts;
    private final RunEvidenceRepository runs;

    ReplayWorkflowAction(ArtifactRepository artifacts, RunEvidenceRepository runs) {
        this.artifacts = artifacts;
        this.runs = runs;
    }

    @Override
    public String name() {
        return "replay-workflow";
    }

    @Override
    public String requiredScope() {
        return Scopes.WORKFLOW_RUN;
    }

    @Override
    public String description() {
        return "Replays a recorded workflow run entirely offline from redacted, content-addressed "
                + "fixtures. It detects workflow, descriptor, request, response, mapping, and "
                + "step-order drift without contacting a service.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("ReplayWorkflowRequest");
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        if (artifacts == null || runs == null) {
            throw WorkflowActionJson.unavailable("workflow replay",
                    "start protomolt-serve with --workflow-workspace");
        }
        Workflow workflow = (Workflow) WorkflowActionJson.parse(
                WorkflowActionJson.object(input, "workflow"), Workflow.newBuilder(), "/workflow");
        String runId = WorkflowActionJson.identity(input, "runId");
        RunEvidence evidence;
        WorkflowReplay.ReplayResult result;
        try {
            evidence = runs.find(runId).orElseThrow(() ->
                    WorkflowActionJson.invalid("No run evidence named '" + runId + "'", "/runId"));
            result = WorkflowReplay.replay(workflow, evidence,
                    SchemaResolver.resolve(input, "schema", context).files(), artifacts);
        } catch (ActionException e) {
            throw e;
        } catch (IllegalArgumentException e) {
            throw WorkflowActionJson.invalid(e.getMessage(), "/workflow");
        } catch (IOException e) {
            throw new ActionException("repository-failed", "Replay failed: " + e.getMessage());
        }
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("ok", result.ok());
        if (!result.failure().isBlank()) {
            output.put("failure", result.failure());
        }
        ArrayNode steps = output.putArray("steps");
        for (WorkflowReplay.StepReplay step : result.steps()) {
            ObjectNode node = steps.addObject();
            node.put("stepName", step.stepName());
            node.put("recordedStatus", step.recordedStatus().name());
            node.put("ok", step.ok());
            node.put("detail", step.detail());
        }
        return output;
    }
}
