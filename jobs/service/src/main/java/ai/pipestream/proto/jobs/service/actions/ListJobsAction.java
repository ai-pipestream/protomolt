package ai.pipestream.proto.jobs.service.actions;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.CatalogContract;
import ai.pipestream.proto.actions.Fields;
import ai.pipestream.proto.actions.ProtoAction;
import ai.pipestream.proto.actions.Reply;
import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.jobs.service.store.WorkflowRunRecord;
import ai.pipestream.proto.jobs.service.store.WorkflowRunStore;
import com.google.protobuf.Message;

import com.google.protobuf.Descriptors.Descriptor;
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
    public Descriptor responseType() {
        return CatalogContract.response("ListJobsResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        // Availability first: a node with no job store cannot serve any request, so
        // saying that is more use than listing fields on a verb that cannot run.
        ActionSupport.requireStore(store);
        // The contract names the status with an enum, so an unknown one is refused
        // before the verb runs; the store's own vocabulary drops the type-name prefix
        // that proto enum values carry.
        String declared = Fields.enumName(input, "status");
        String status = declared.startsWith(STATUS_PREFIX)
                ? declared.substring(STATUS_PREFIX.length())
                : declared;
        // The unspecified value is the enum's zero, which filters nothing.
        if (!WorkflowRunRecord.STATUSES.contains(status)) {
            status = null;
        }
        String workflowName = Fields.string(input, "workflowName");
        // Zero selects the default, as the message says; anything above the ceiling is
        // clamped rather than refused, because the ceiling bounds the answer not the ask.
        int asked = Fields.integer(input, "limit");
        int limit = asked == 0 ? DEFAULT_LIMIT : Math.min(asked, MAX_LIMIT);
        List<WorkflowRunRecord> jobs = store.list(
                status, workflowName.isEmpty() ? null : workflowName,
                limit, Fields.integer(input, "offset"));
        Reply result = Reply.of(responseType()).set("ok", true);
        for (WorkflowRunRecord job : jobs) {
            ActionSupport.writeJob(result.append("jobs"), job, false);
        }
        return result.build();
    }
}
