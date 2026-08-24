package ai.pipestream.proto.workflow;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.CatalogContract;
import ai.pipestream.proto.actions.Fields;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Reply;
import ai.pipestream.proto.actions.Scopes;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
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
    public Descriptor requestType() {
        return CatalogContract.request("CheckWorkflowRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("CheckWorkflowResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context)
            throws ActionException {
        // This verb reports on a workflow rather than refusing one, so a definition it
        // cannot read comes back as a finding.
        Reply result = Reply.of(responseType());
        CompiledWorkflow definition;
        try {
            definition = WorkflowJson.parse(Fields.message(input, "workflow"), context);
        } catch (WorkflowJson.WorkflowParseException e) {
            finding(result, e.step, "workflow", e.getMessage());
            return result.set("ok", false).build();
        }
        List<WorkflowVerifier.Finding> findings = new WorkflowVerifier().verify(definition);
        for (WorkflowVerifier.Finding entry : findings) {
            finding(result, entry.step(), entry.kind(), entry.error());
        }
        return result.set("ok", findings.isEmpty()).build();
    }

    private static void finding(Reply result, String step, String kind, String error) {
        result.append("findings")
                .set("step", step)
                .set("kind", kind)
                .set("error", error)
                .build();
    }
}
