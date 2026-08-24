package ai.pipestream.proto.workflow;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.CatalogContract;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

/**
 * The {@code check-workflow} verb: verify a workflow without executing anything — methods
 * resolve and are unary, step names are sound scope variables, and every gate, mapping
 * rule, and CEL expression type-checks against exactly the scope its step will see. The
 * lint gate for consoles, CI, and registration.
 */
public final class CheckWorkflowAction implements ProtoAction {

    @Override
    public String name() {
        return "check-workflow";
    }

    @Override
    public String requiredScope() {
        return Scopes.WORKFLOW_RUN;
    }

    @Override
    public String description() {
        return "Statically verifies a workflow definition: every step's method resolves and "
                + "is unary, step names are valid scope variables, 'when' gates are boolean "
                + "CEL, and every mapping rule and CEL expression type-checks against the "
                + "scope that step will see ('input' plus prior steps' responses). A workflow "
                + "that checks clean cannot fail on a type error at run time.";
    }

    @Override
    public ObjectNode inputSchema() {
        return CatalogContract.schemaFor("CheckWorkflowRequest");
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context)
            throws ActionException {
        CatalogContract.check(input, "CheckWorkflowRequest", name());
        ObjectNode result = JsonNodeFactory.instance.objectNode();
        ArrayNode findingsNode = result.putArray("findings");
        JsonNode workflowNode = input.get("workflow");
        if (!(workflowNode instanceof ObjectNode workflow)) {
            result.put("ok", false);
            finding(findingsNode, "", "workflow", "'workflow' object is required");
            return result;
        }
        CompiledWorkflow definition;
        try {
            definition = WorkflowJson.parse(workflow, context);
        } catch (WorkflowJson.WorkflowParseException e) {
            result.put("ok", false);
            finding(findingsNode, e.step, "workflow", e.getMessage());
            return result;
        }
        List<WorkflowVerifier.Finding> findings = new WorkflowVerifier().verify(definition);
        for (WorkflowVerifier.Finding entry : findings) {
            finding(findingsNode, entry.step(), entry.kind(), entry.error());
        }
        result.put("ok", findings.isEmpty());
        return result;
    }

    private static void finding(ArrayNode findings, String step, String kind, String error) {
        ObjectNode node = findings.addObject();
        node.put("step", step);
        node.put("kind", kind);
        node.put("error", error);
    }
}
