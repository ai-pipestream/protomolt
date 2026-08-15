package ai.pipestream.proto.workflow;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.grpc.workflow.WorkflowVersionRepository;
import ai.pipestream.proto.grpc.workflow.WorkflowValidation;
import ai.pipestream.proto.grpc.workflow.v1.Workflow;
import ai.pipestream.proto.grpc.workflow.v1.VersionedWorkflow;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Timestamp;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;

/** Promotes validated workflow content as one immutable registry version. */
final class PromoteWorkflowAction implements ProtoAction {

    private final WorkflowVersionRepository workflows;
    private final Clock clock;

    PromoteWorkflowAction(WorkflowVersionRepository workflows) {
        this(workflows, Clock.systemUTC());
    }

    PromoteWorkflowAction(WorkflowVersionRepository workflows, Clock clock) {
        this.workflows = workflows;
        this.clock = clock;
    }

    @Override
    public String name() {
        return "promote-workflow";
    }

    @Override
    public String description() {
        return "Promotes validated workflow content under an immutable version in the mounted "
                + "git registry. Re-promoting identical content is idempotent; changing an "
                + "existing version fails.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = WorkflowActionJson.schema();
        ObjectNode properties = schema.putObject("properties");
        properties.putObject("workflow").put("type", "object");
        WorkflowActionJson.identitySchema(properties, "version");
        schema.putArray("required").add("workflow").add("version");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        if (workflows == null) {
            throw WorkflowActionJson.unavailable("workflow promotion",
                    "start protomolt-serve with --registry-git");
        }
        Workflow workflow = (Workflow) WorkflowActionJson.parse(
                WorkflowActionJson.object(input, "workflow"), Workflow.newBuilder(), "/workflow");
        String version = WorkflowActionJson.identity(input, "version");
        Instant now = clock.instant();
        VersionedWorkflow promoted = VersionedWorkflow.newBuilder()
                .setWorkflow(workflow)
                .setVersion(version)
                .setWorkflowFingerprint(WorkflowValidation.fingerprint(workflow))
                .setCreatedAt(Timestamp.newBuilder().setSeconds(now.getEpochSecond())
                        .setNanos(now.getNano()))
                .build();
        try {
            WorkflowValidation.validate(promoted);
            workflows.save(promoted);
        } catch (IllegalArgumentException e) {
            throw WorkflowActionJson.invalid(e.getMessage(), "/workflow");
        } catch (IOException e) {
            throw new ActionException("repository-failed",
                    "Failed to promote workflow: " + e.getMessage());
        }
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("promoted", true);
        output.set("versionedWorkflow", WorkflowActionJson.render(promoted, context));
        return output;
    }
}
