package ai.protomolt.proto.workflow;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.Fields;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Reply;
import ai.protomolt.proto.actions.SchemaResolver;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.RunEvidenceRepository;
import ai.protomolt.proto.grpc.workflow.v1.RunEvidence;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.io.IOException;

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
    public Descriptor responseType() {
        return CatalogContract.response("ReplayWorkflowResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        if (artifacts == null || runs == null) {
            throw WorkflowRequests.unavailable("workflow replay",
                    "start protomolt-serve with --workflow-workspace");
        }
        Workflow workflow = CatalogContract.as(
                Fields.message(input, "workflow"), Workflow.getDefaultInstance(), name());
        String runId = WorkflowRequests.identity(input, "runId");
        RunEvidence evidence;
        WorkflowReplay.ReplayResult result;
        try {
            evidence = runs.find(runId).orElseThrow(() ->
                    WorkflowRequests.invalid("No run evidence named '" + runId + "'", "/runId"));
            result = WorkflowReplay.replay(workflow, evidence,
                    SchemaResolver.resolve(input, "schema", context).files(), artifacts);
        } catch (ActionException e) {
            throw e;
        } catch (IllegalArgumentException e) {
            throw WorkflowRequests.invalid(e.getMessage(), "/workflow");
        } catch (IOException e) {
            throw new ActionException("repository-failed", "Replay failed: " + e.getMessage());
        }
        Reply output = Reply.of(responseType())
                .set("ok", result.ok())
                .set("failure", result.failure());
        for (WorkflowReplay.StepReplay step : result.steps()) {
            output.append("steps")
                    .set("stepName", step.stepName())
                    .set("recordedStatus", step.recordedStatus().name())
                    .set("ok", step.ok())
                    .set("detail", step.detail())
                    .build();
        }
        return output.build();
    }
}
