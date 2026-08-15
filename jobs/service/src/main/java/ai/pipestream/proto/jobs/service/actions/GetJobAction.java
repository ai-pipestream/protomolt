package ai.pipestream.proto.jobs.service.actions;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.jobs.service.store.WorkflowRunRecord;
import ai.pipestream.proto.jobs.service.store.WorkflowRunStore;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
    public String description() {
        return "Reads one workflow run by id: status, attempt, the outstanding step when "
                + "parked, the input, every step checkpoint, the result and verdict when "
                + "completed, and the verbatim error when failed.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = JsonNodeFactory.instance.objectNode();
        schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        schema.put("type", "object");
        schema.putObject("properties").putObject("jobId")
                .put("type", "string")
                .put("description", "The job's uuid.");
        schema.putArray("required").add("jobId");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
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
