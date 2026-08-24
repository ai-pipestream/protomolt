package ai.pipestream.proto.workflow;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.CatalogContract;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.grpc.workflow.ArtifactRepository;
import ai.pipestream.proto.grpc.workflow.RunEvidenceRepository;
import ai.pipestream.proto.grpc.workflow.v1.RunEvidence;
import ai.pipestream.proto.http.json.MalformedProtobufJsonException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DynamicMessage;

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
    public ObjectNode inputSchema() {
        return CatalogContract.schemaFor("RecordWorkflowRunRequest");
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        CatalogContract.check(input, "RecordWorkflowRunRequest", name());
        if (artifacts == null || runs == null) {
            throw WorkflowActionJson.unavailable("workflow run recording",
                    "start protomolt-serve with --workflow-workspace");
        }
        ObjectNode workflowNode = WorkflowActionJson.object(input, "workflow");
        CompiledWorkflow workflow = CompileWorkflowAction.parseChecked(workflowNode, context);
        ObjectNode inputNode = WorkflowActionJson.object(input, "input");
        DynamicMessage message;
        try {
            message = context.transcoder().fromJsonDynamic(inputNode.toString(), workflow.inputType());
        } catch (MalformedProtobufJsonException e) {
            throw WorkflowActionJson.invalid("Input is not valid proto3 JSON for "
                    + workflow.inputType().getFullName() + ": " + e.getMessage(), "/input");
        }
        String runId = WorkflowActionJson.identity(input, "runId");
        String version = WorkflowActionJson.optionalIdentity(input, "workflowVersion");
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
            ObjectNode output = context.objectMapper().createObjectNode();
            output.put("ok", false);
            output.put("failedStep", failure.step());
            output.put("failureKind", failure.kind().name());
            output.set("evidence", WorkflowActionJson.render(evidence, context));
            return output;
        } catch (IllegalArgumentException e) {
            throw WorkflowActionJson.invalid(e.getMessage(), "/runId");
        } catch (IOException e) {
            throw new ActionException("repository-failed",
                    "Failed to record workflow run: " + e.getMessage());
        }
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("ok", true);
        output.set("evidence", WorkflowActionJson.render(evidence, context));
        return output;
    }
}
