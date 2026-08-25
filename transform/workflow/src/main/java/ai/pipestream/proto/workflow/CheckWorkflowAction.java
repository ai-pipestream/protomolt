package ai.pipestream.proto.workflow;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.CatalogContract;
import ai.pipestream.proto.actions.Fields;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Reply;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The {@code check-workflow} verb: verify a workflow without executing anything — methods
 * resolve and are unary, step names are sound scope variables, and every gate, mapping
 * rule, and CEL expression type-checks against exactly the scope its step will see. The
 * lint gate for consoles, CI, and registration.
 */
public final class CheckWorkflowAction implements ProtoAction {

    /** The leading {@code steps[n]} of a violation path, when it has one. */
    private static final Pattern STEP_INDEX = Pattern.compile("steps\\[(\\d+)\\]");

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
        // This verb reports on a workflow rather than refusing one, so nothing about the
        // definition is an error here: what it cannot read, and what the contract will not
        // allow, both come back as findings.
        Message workflow = Fields.message(input, "workflow");
        Reply result = Reply.of(responseType());

        // The request declares this field inspect-only, so the door left the workflow's own
        // rules unchecked and they are this verb's to report.
        ValidationResult declared = CatalogContract.inspect(workflow);
        for (ValidationResult.Violation violation : declared.violations()) {
            finding(result, stepOf(violation.path()), "contract",
                    CatalogContract.finding(violation).path("field").asText()
                            + " " + violation.message());
        }

        CompiledWorkflow definition;
        try {
            definition = WorkflowJson.parse(workflow, context);
        } catch (WorkflowJson.WorkflowParseException e) {
            finding(result, e.step, "workflow", e.getMessage());
            return result.set("ok", false).build();
        }
        List<WorkflowVerifier.Finding> findings = new WorkflowVerifier().verify(definition);
        for (WorkflowVerifier.Finding entry : findings) {
            finding(result, entry.step(), entry.kind(), entry.error());
        }
        return result.set("ok", declared.valid() && findings.isEmpty()).build();
    }

    /**
     * The step a violation sits under, read off its path: {@code steps[2].cel_rules[0].target}
     * reports against step 2. A violation outside any step reports against the workflow.
     */
    private static String stepOf(String path) {
        Matcher index = STEP_INDEX.matcher(path);
        return index.lookingAt() ? "steps[" + index.group(1) + "]" : "";
    }

    private static void finding(Reply result, String step, String kind, String error) {
        result.append("findings")
                .set("step", step)
                .set("kind", kind)
                .set("error", error)
                .build();
    }
}
