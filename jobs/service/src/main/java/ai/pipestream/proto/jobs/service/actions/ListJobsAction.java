package ai.pipestream.proto.jobs.service.actions;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.CatalogContract;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.jobs.service.store.WorkflowRunRecord;
import ai.pipestream.proto.jobs.service.store.WorkflowRunStore;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;
import com.google.protobuf.Descriptors.Descriptor;

/**
 * The {@code list-jobs} verb: page workflow runs, newest first, with optional
 * status and workflow-name filters. Rows are summaries — no input, checkpoints,
 * or result (get-job answers those) — so the page stays cheap.
 * <p>
 * A null store means workflow runs are not configured on this server; every
 * call then answers {@code unavailable}.
 */
public final class ListJobsAction implements ProtoAction {

    /**
     * Proto enum values carry their type name, so the wire form of COMPLETED is
     * JOB_STATUS_COMPLETED. The store's status column does not, and
     * JOB_STATUS_UNSPECIFIED reduces to no filter at all.
     */
    private static final String STATUS_PREFIX = "JOB_STATUS_";

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
    public String requiredScope() {
        return Scopes.SERVICE_INVOKE;
    }

    @Override
    public String description() {
        return "Lists workflow runs, newest first, optionally filtered by status (QUEUED, "
                + "RUNNING, WAITING, COMPLETED, FAILED, DEAD) and workflow name. Rows are "
                + "summaries without input/checkpoints/result; use get-job for one job's "
                + "full record.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("ListJobsRequest");
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        // Availability first: a node with no job store cannot serve any request, so
        // saying that is more use than listing fields on a verb that cannot run.
        ActionSupport.requireStore(store);
        // The contract names the status with an enum, so an unknown one is refused
        // before the verb runs; the store's own vocabulary drops the type-name prefix
        // that proto enum values carry.
        String status = ActionSupport.optionalString(input, "status");
        if (status != null && status.startsWith(STATUS_PREFIX)) {
            status = status.substring(STATUS_PREFIX.length());
        }
        if (status != null && status.isEmpty()) {
            status = null;
        }
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
