package ai.protomolt.proto.jobs.service.store;

import ai.protomolt.proto.pipeline.v1.PipelineRunCheckpoint;
import ai.protomolt.proto.pipeline.v1.PipelineRunCheckpoints;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

/**
 * The writes a pipeline run makes to {@code workflow_run}: its protobuf checkpoints and
 * result. Each mirrors the workflow write of the same name in {@link JdbcWorkflowRunStore},
 * with the same claim guard and the same event committed in the same transaction.
 */
final class JdbcPipelineRunWrites {

    private final WorkflowRunDatabase database;

    JdbcPipelineRunWrites(WorkflowRunDatabase database) {
        this.database = database;
    }

    void saveCheckpoints(WorkerClaim claim, byte[] checkpoints,
            WorkflowRunEventRecord stepEvent) {
        database.inTransaction(c -> {
            JdbcWorkflowRunStore.updateClaimed(c, """
                    UPDATE workflow_run SET pipeline_checkpoints_pb = ?,
                           updated_at = clock_timestamp()
                     WHERE job_id = ? """ + JdbcWorkflowRunStore.CLAIM_GUARD, ps -> {
                ps.setBytes(1, checkpoints);
                JdbcWorkflowRunStore.bindClaim(ps, 2, claim);
            }, claim);
            JdbcWorkflowRunStore.enqueue(c, stepEvent);
            return null;
        });
    }

    void markWaiting(WorkerClaim claim, String stepName, byte[] checkpoints,
            WorkflowRunEventRecord event) {
        database.inTransaction(c -> {
            JdbcWorkflowRunStore.updateClaimed(c, """
                    UPDATE workflow_run SET status = 'WAITING', outstanding_step = ?,
                           pipeline_checkpoints_pb = ?, lease_owner = NULL, lease_until = NULL,
                           updated_at = clock_timestamp()
                     WHERE job_id = ? """ + JdbcWorkflowRunStore.CLAIM_GUARD, ps -> {
                ps.setString(1, stepName);
                ps.setBytes(2, checkpoints);
                JdbcWorkflowRunStore.bindClaim(ps, 3, claim);
            }, claim);
            JdbcWorkflowRunStore.enqueue(c, event);
            return null;
        });
    }

    void markCompleted(WorkerClaim claim, byte[] result, String verdict,
            WorkflowRunEventRecord event) {
        database.inTransaction(c -> {
            JdbcWorkflowRunStore.updateClaimed(c, """
                    UPDATE workflow_run SET status = 'COMPLETED', pipeline_result_pb = ?,
                           verdict = ?, lease_owner = NULL, lease_until = NULL,
                           outstanding_step = NULL, completed_at = clock_timestamp(),
                           updated_at = clock_timestamp()
                     WHERE job_id = ? """ + JdbcWorkflowRunStore.CLAIM_GUARD, ps -> {
                ps.setBytes(1, result);
                ps.setString(2, verdict);
                JdbcWorkflowRunStore.bindClaim(ps, 3, claim);
            }, claim);
            JdbcWorkflowRunStore.enqueue(c, event);
            return null;
        });
    }

    ParkedCompletion completeParkedStep(UUID jobId, String stepName,
            PipelineRunCheckpoint checkpoint, WorkflowRunEventRecord stepEvent,
            String validationError) {
        return database.inTransaction(c -> {
            String select = """
                    SELECT status, outstanding_step, pipeline_checkpoints_pb FROM workflow_run
                     WHERE job_id = ? FOR UPDATE""";
            String status;
            String outstanding;
            PipelineRunCheckpoints checkpoints;
            try (PreparedStatement ps = c.prepareStatement(select)) {
                ps.setObject(1, jobId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw WorkflowRunStoreException.notFound(jobId);
                    }
                    status = rs.getString("status");
                    outstanding = rs.getString("outstanding_step");
                    checkpoints = PipelineCheckpointBytes.parse(
                            rs.getBytes("pipeline_checkpoints_pb"));
                }
            } catch (SQLException e) {
                throw WorkflowRunStoreException.wrap("complete-parked-step read failed", e);
            }
            ParkedCompletion settled = PipelineCheckpointBytes.alreadySettled(
                    checkpoints, stepName, checkpoint, status);
            if (settled != null) {
                return settled;
            }
            if (!WorkflowRunRecord.STATUS_WAITING.equals(status)
                    || !stepName.equals(outstanding)) {
                return new ParkedCompletion.WrongState(status, outstanding);
            }
            if (validationError != null) {
                JdbcWorkflowRunStore.update(c, """
                        UPDATE workflow_run SET status = 'FAILED', error = ?,
                               lease_owner = NULL, lease_until = NULL, outstanding_step = NULL,
                               completed_at = now(), updated_at = now()
                         WHERE job_id = ?""", ps -> {
                    ps.setString(1, validationError);
                    ps.setObject(2, jobId);
                }, jobId);
                JdbcWorkflowRunStore.enqueue(c, stepEvent);
                return new ParkedCompletion.Rejected(validationError);
            }
            byte[] appended = checkpoints.toBuilder().addCheckpoints(checkpoint).build()
                    .toByteArray();
            JdbcWorkflowRunStore.update(c, """
                    UPDATE workflow_run SET status = 'QUEUED', outstanding_step = NULL,
                           pipeline_checkpoints_pb = ?, run_after = now(), updated_at = now()
                     WHERE job_id = ?""", ps -> {
                ps.setBytes(1, appended);
                ps.setObject(2, jobId);
            }, jobId);
            JdbcWorkflowRunStore.enqueue(c, stepEvent);
            return new ParkedCompletion.Completed();
        });
    }
}
