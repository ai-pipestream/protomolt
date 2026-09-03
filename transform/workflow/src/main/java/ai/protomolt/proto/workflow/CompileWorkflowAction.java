package ai.protomolt.proto.workflow;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.Fields;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Reply;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.grpc.workflow.WorkflowValidation;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.util.List;

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
    public Descriptor responseType() {
        return CatalogContract.response("CompileWorkflowResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        CompiledWorkflow definition = parseChecked(Fields.message(input, "workflow"), context);
        Workflow workflow;
        try {
            workflow = WorkflowCompiler.compile(definition);
        } catch (IllegalArgumentException e) {
            throw WorkflowRequests.invalid(e.getMessage(), "/workflow");
        }
        return Reply.of(responseType())
                .set("workflow", workflow)
                .set("workflowFingerprint", WorkflowValidation.fingerprint(workflow))
                .build();
    }

    static CompiledWorkflow parseChecked(Message workflow, ActionContext context)
            throws ActionException {
        CompiledWorkflow definition;
        try {
            definition = WorkflowJson.parse(workflow, context);
        } catch (WorkflowJson.WorkflowParseException e) {
            throw WorkflowRequests.invalid(e.getMessage(), e.step.isBlank()
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
