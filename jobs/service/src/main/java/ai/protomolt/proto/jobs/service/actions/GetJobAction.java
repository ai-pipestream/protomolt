package ai.protomolt.proto.jobs.service.actions;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.CatalogContract;
import ai.protomolt.proto.actions.Fields;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Reply;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStore;
import com.google.protobuf.Message;

import com.google.protobuf.Descriptors.Descriptor;
import java.util.Optional;
import java.util.UUID;

/**
 * The {@code get-job} verb: a single-record read of one workflow run — the CLI,
 * MCP, and 2am-debugging lane (the topic is the watch lane; there is no
 * WatchJob streaming). Answers the full row: input, checkpoints, result.
 * <p>
 * A null store means workflow runs are not configured on this server; every
 * call then answers {@code unavailable}.
 */
public final class GetJobAction implements ProtoAction {

    private final WorkflowRunStore store;

    /**
     * @param store the jobs store, or null when jobs are not configured
     */
    public GetJobAction(WorkflowRunStore store) {
        this.store = store;
    }

    @Override
    public String name() {
        return "get-job";
    }

    @Override
    public String requiredScope() {
        return Scopes.SERVICE_INVOKE;
    }

    @Override
    public String description() {
        return "Reads one workflow run by id: status, attempt, the outstanding step when "
                + "parked, the input, every step checkpoint, the result and verdict when "
                + "completed, and the verbatim error when failed.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("GetJobRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("GetJobResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        // Availability first: a node with no job store cannot serve any request, so
        // saying that is more use than listing fields on a verb that cannot run.
        ActionSupport.requireStore(store);
        UUID jobId = ActionSupport.jobId(Fields.string(input, "jobId"));
        Optional<WorkflowRunRecord> job = store.get(jobId);
        if (job.isEmpty()) {
            return Reply.of(responseType())
                    .set("ok", false)
                    .set("error", "no workflow run " + jobId)
                    .build();
        }
        Reply result = Reply.of(responseType()).set("ok", true);
        ActionSupport.writeJob(result.nest("job"), job.get(), true);
        return result.build();
    }
}
