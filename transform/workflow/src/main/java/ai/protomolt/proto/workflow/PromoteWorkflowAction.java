package ai.protomolt.proto.workflow;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.Fields;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Reply;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.grpc.workflow.WorkflowValidation;
import ai.protomolt.proto.grpc.workflow.WorkflowVersionRepository;
import ai.protomolt.proto.grpc.workflow.v1.VersionedWorkflow;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;

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
    public String requiredScope() {
        return Scopes.WORKFLOW_RUN;
    }

    @Override
    public String description() {
        return "Promotes validated workflow content under an immutable version in the mounted "
                + "git registry. Re-promoting identical content is idempotent; changing an "
                + "existing version fails.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("PromoteWorkflowRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("PromoteWorkflowResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        if (workflows == null) {
            throw WorkflowRequests.unavailable("workflow promotion",
                    "start protomolt-serve with --registry-git");
        }
        Workflow workflow = CatalogContract.as(
                Fields.message(input, "workflow"), Workflow.getDefaultInstance(), name());
        String version = WorkflowRequests.identity(input, "version");
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
            promoted = saveOrReuse(promoted);
        } catch (IllegalArgumentException e) {
            throw WorkflowRequests.invalid(e.getMessage(), "/workflow");
        } catch (IOException e) {
            throw new ActionException("repository-failed",
                    "Failed to promote workflow: " + e.getMessage());
        }
        return Reply.of(responseType())
                .set("promoted", true)
                .set("versionedWorkflow", context.transcoder().toJson(promoted))
                .build();
    }

    private VersionedWorkflow saveOrReuse(VersionedWorkflow proposed) throws IOException {
        String name = proposed.getWorkflow().getName();
        String version = proposed.getVersion();
        var existing = workflows.find(name, version);
        if (existing.isPresent()) return matching(proposed, existing.get());
        try {
            workflows.save(proposed);
            return proposed;
        } catch (IOException | IllegalArgumentException failure) {
            // A concurrent promotion or an uncertain write may already have stored
            // this content. Only a validated matching envelope proves success.
            var winner = workflows.find(name, version);
            if (winner.isPresent()) return matching(proposed, winner.get());
            throw failure;
        }
    }

    private static VersionedWorkflow matching(VersionedWorkflow proposed, VersionedWorkflow stored) {
        WorkflowValidation.validate(stored);
        if (!proposed.getVersion().equals(stored.getVersion())
                || !proposed.getWorkflowFingerprint().equals(stored.getWorkflowFingerprint())
                || !Arrays.equals(proposed.getWorkflow().toByteArray(), stored.getWorkflow().toByteArray())) {
            throw new IllegalArgumentException("workflow " + proposed.getWorkflow().getName()
                    + " version " + proposed.getVersion() + " is immutable: the stored content differs");
        }
        return stored;
    }
}
