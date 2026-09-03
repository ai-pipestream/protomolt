package ai.protomolt.proto.workflow;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.Fields;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Reply;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.RunEvidenceRepository;
import ai.protomolt.proto.grpc.workflow.v1.RunEvidence;
import ai.protomolt.proto.http.json.MalformedProtobufJsonException;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.io.IOException;

/** Runs an inline workflow and records redacted content-addressed evidence for replay. */
final class RecordWorkflowRunAction implements ProtoAction {

    private final WorkflowRunner runner;
    private final ArtifactRepository artifacts;
    private final RunEvidenceRepository runs;

    RecordWorkflowRunAction(WorkflowRunner runner, ArtifactRepository artifacts,
                          RunEvidenceRepository runs) {
        this.runner = runner;
        this.artifacts = artifacts;
        this.runs = runs;
    }

    @Override
    public String name() {
        return "record-workflow-run";
    }

    @Override
    public String requiredScope() {
        return Scopes.WORKFLOW_RUN;
    }

    @Override
    public String description() {
        return "Executes a checked workflow as a draft workflow probe, removes fields marked pii "
                + "or secret before persistence, and stores bounded content-addressed input, "
                + "request, response, and output fixtures plus immutable run evidence.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("RecordWorkflowRunRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("RecordWorkflowRunResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        if (artifacts == null || runs == null) {
            throw WorkflowRequests.unavailable("workflow run recording",
                    "start protomolt-serve with --workflow-workspace");
        }
        CompiledWorkflow workflow = CompileWorkflowAction.parseChecked(
                Fields.message(input, "workflow"), context);
        DynamicMessage message;
        try {
            message = context.transcoder().fromJsonDynamic(
                    Fields.json(input, "input").toString(), workflow.inputType());
        } catch (MalformedProtobufJsonException e) {
            throw WorkflowRequests.invalid("Input is not valid proto3 JSON for "
                    + workflow.inputType().getFullName() + ": " + e.getMessage(), "/input");
        }
        String runId = WorkflowRequests.identity(input, "runId");
        String version = WorkflowRequests.optionalIdentity(input, "workflowVersion");
        WorkflowRunRecorder recorder = new WorkflowRunRecorder(runner, artifacts, runs);
        RunEvidence evidence;
        try {
            evidence = recorder.record(runId, version, workflow, message);
        } catch (WorkflowRunner.WorkflowExecutionException failure) {
            try {
                evidence = runs.find(runId).orElseThrow();
            } catch (Exception missing) {
                throw new ActionException("execution-failed", failure.getMessage());
            }
            return Reply.of(responseType())
                    .set("ok", false)
                    .set("failedStep", failure.step())
                    .set("failureKind", failure.kind().name())
                    .set("evidence", evidence)
                    .build();
        } catch (IllegalArgumentException e) {
            throw WorkflowRequests.invalid(e.getMessage(), "/runId");
        } catch (IOException e) {
            throw new ActionException("repository-failed",
                    "Failed to record workflow run: " + e.getMessage());
        }
        return Reply.of(responseType())
                .set("ok", true)
                .set("evidence", evidence)
                .build();
    }
}
