package ai.pipestream.proto.jobs.service.actions;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.jobs.service.store.WorkflowRunRecord;
import ai.pipestream.proto.jobs.service.store.WorkflowRunStore;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

/**
 * The {@code list-jobs} verb: page workflow runs, newest first, with optional
 * status and workflow-name filters. Rows are summaries — no input, checkpoints,
 * or result (get-job answers those) — so the page stays cheap.
 * <p>
 * A null store means workflow runs are not configured on this server; every
 * call then answers {@code unavailable}.
 */
public final class ListJobsAction implements ProtoAction {

    /** The default page size. */
    public static final int DEFAULT_LIMIT = 50;

    /** The page-size ceiling. */
    public static final int MAX_LIMIT = 500;

    private final WorkflowRunStore store;

    /**
     * @param store the jobs store, or null when jobs are not configured
     */
    public ListJobsAction(WorkflowRunStore store) {
        this.store = store;
    }

    @Override
    public String name() {
        return "list-jobs";
    }

    @Override
    public String description() {
        return "Lists workflow runs, newest first, optionally filtered by status (QUEUED, "
                + "RUNNING, WAITING, COMPLETED, FAILED, DEAD) and workflow name. Rows are "
                + "summaries without input/checkpoints/result; use get-job for one job's "
                + "full record.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = JsonNodeFactory.instance.objectNode();
        schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        schema.put("type", "object");
        ObjectNode properties = schema.putObject("properties");
        properties.putObject("status")
                .put("type", "string")
                .put("description", "Restrict to one status: QUEUED, RUNNING, WAITING, "
                        + "COMPLETED, FAILED, or DEAD.");
        properties.putObject("workflowName")
                .put("type", "string")
                .put("description", "Restrict to one workflow.");
        properties.putObject("limit")
                .put("type", "integer")
                .put("description", "Page size; default " + DEFAULT_LIMIT + ", capped at "
                        + MAX_LIMIT + ".");
        properties.putObject("offset")
                .put("type", "integer")
                .put("description", "Rows to skip.");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        ActionSupport.requireStore(store);
        String status = ActionSupport.optionalString(input, "status");
        if (status != null && !WorkflowRunRecord.STATUSES.contains(status)) {
            throw ActionSupport.invalidInput("'status' must be one of "
                    + String.join(", ", WorkflowRunRecord.STATUSES.stream().sorted().toList())
                    + "; got '" + status + "'");
        }
        String workflowName = ActionSupport.optionalString(input, "workflowName");
        int limit = ActionSupport.optionalInt(input, "limit", DEFAULT_LIMIT, 1, MAX_LIMIT);
        long offset = ActionSupport.optionalOffset(input, "offset");
        List<WorkflowRunRecord> jobs = store.list(status, workflowName, limit, offset);
        ObjectNode result = JsonNodeFactory.instance.objectNode();
        result.put("ok", true);
        ArrayNode array = result.putArray("jobs");
        for (WorkflowRunRecord job : jobs) {
            array.add(ActionSupport.jobJson(job, false));
        }
        return result;
    }
}
