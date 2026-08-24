package ai.pipestream.proto.workflow;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.CatalogContract;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.grpc.workflow.WorkflowValidation;
import ai.pipestream.proto.grpc.workflow.v1.Workflow;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;
import com.google.protobuf.Descriptors.Descriptor;

/** Compiles an existing checked workflow definition into the durable workflow contract. */
final class CompileWorkflowAction implements ProtoAction {

    @Override
    public String name() {
        return "compile-workflow";
    }

    @Override
    public String requiredScope() {
        return Scopes.SCHEMA_READ;
    }

    @Override
    public String description() {
        return "Compiles a descriptor-grounded workflow into a deterministic gRPC workflow. "
                + "Every method, mapping, gate, deadline, and descriptor fingerprint is "
                + "checked before the workflow is returned.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("CompileWorkflowRequest");
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        CompiledWorkflow definition = parseChecked(WorkflowActionJson.object(input, "workflow"), context);
        Workflow workflow;
        try {
            workflow = WorkflowCompiler.compile(definition);
        } catch (IllegalArgumentException e) {
            throw WorkflowActionJson.invalid(e.getMessage(), "/workflow");
        }
        ObjectNode output = context.objectMapper().createObjectNode();
        output.set("workflow", WorkflowActionJson.render(workflow, context));
        output.put("workflowFingerprint", WorkflowValidation.fingerprint(workflow));
        return output;
    }

    static CompiledWorkflow parseChecked(ObjectNode workflow, ActionContext context)
            throws ActionException {
        CompiledWorkflow definition;
        try {
            definition = WorkflowJson.parse(workflow, context);
        } catch (WorkflowJson.WorkflowParseException e) {
            throw WorkflowActionJson.invalid(e.getMessage(), e.step.isBlank()
                    ? "/workflow" : "/workflow/steps/" + e.step);
        }
        List<WorkflowVerifier.Finding> findings = new WorkflowVerifier().verify(definition);
        if (!findings.isEmpty()) {
            ObjectNode details = context.objectMapper().createObjectNode();
            ArrayNode nodes = details.putArray("findings");
            for (WorkflowVerifier.Finding finding : findings) {
                ObjectNode node = nodes.addObject();
                node.put("step", finding.step());
                node.put("kind", finding.kind());
                node.put("error", finding.error());
            }
            throw new ActionException("workflow-invalid",
                    "Workflow has " + findings.size() + " static finding(s)", details);
        }
        return definition;
    }
}
