package ai.pipestream.proto.jobs.service.actions;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.CatalogContract;
import ai.pipestream.proto.actions.JsonAction;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.jobs.service.store.WorkflowRunRecord;
import ai.pipestream.proto.jobs.service.store.WorkflowRunStore;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
public final class GetJobAction implements JsonAction {

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
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        // Availability first: a node with no job store cannot serve any request, so
        // saying that is more use than listing fields on a verb that cannot run.
        ActionSupport.requireStore(store);
        String jobIdText = ActionSupport.requireString(input, "jobId");
        UUID jobId;
        try {
            jobId = UUID.fromString(jobIdText.trim());
        } catch (IllegalArgumentException e) {
            throw ActionSupport.invalidInput("'jobId' must be a uuid; got '" + jobIdText + "'");
        }
        Optional<WorkflowRunRecord> job = store.get(jobId);
        ObjectNode result = JsonNodeFactory.instance.objectNode();
        if (job.isEmpty()) {
            result.put("ok", false);
            result.put("error", "no workflow run " + jobId);
            return result;
        }
        result.put("ok", true);
        result.set("job", ActionSupport.jobJson(job.get(), true));
        return result;
    }
}
