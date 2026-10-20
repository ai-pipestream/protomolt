package ai.protomolt.proto.jobs.service.store;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * JDBC value binding and row mapping for workflow runs and their outbox events.
 * This boundary does not execute SQL, own connections, or decide state transitions.
 * Stored JSON text and protobuf event bytes pass through unchanged.
 */
final class WorkflowRunJdbcMapping {

    private WorkflowRunJdbcMapping() {
    }

    static void setText(PreparedStatement ps, int index, String value)
            throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.VARCHAR);
        } else {
            ps.setString(index, value);
        }
    }

    static void setInstant(PreparedStatement ps, int index, Instant value)
            throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.TIMESTAMP_WITH_TIMEZONE);
        } else {
            ps.setObject(index, value.atOffset(ZoneOffset.UTC));
        }
    }

    static WorkflowRunRecord mapJob(ResultSet rs) throws SQLException {
        WorkflowRunRecord record = new WorkflowRunRecord();
        record.jobId = rs.getObject("job_id", UUID.class);
        record.workflowName = rs.getString("workflow_name");
        record.workflowDefinition = rs.getString("workflow_definition");
        record.input = rs.getString("input");
        record.inputRef = rs.getString("input_ref");
        record.status = rs.getString("status");
        record.attempt = rs.getInt("attempt");
        record.maxAttempts = rs.getInt("max_attempts");
        record.runAfter = toInstant(rs.getObject("run_after", OffsetDateTime.class));
        record.outstandingStep = rs.getString("outstanding_step");
        record.checkpoints = rs.getString("checkpoints");
        record.result = rs.getString("result");
        record.resultRef = rs.getString("result_ref");
        record.verdict = rs.getString("verdict");
        record.error = rs.getString("error");
        record.leaseOwner = rs.getString("lease_owner");
        record.leaseUntil = toInstant(rs.getObject("lease_until", OffsetDateTime.class));
        record.createdAt = toInstant(rs.getObject("created_at", OffsetDateTime.class));
        record.updatedAt = toInstant(rs.getObject("updated_at", OffsetDateTime.class));
        record.completedAt = toInstant(rs.getObject("completed_at", OffsetDateTime.class));
        return record;
    }

    static WorkflowRunEventRecord mapEvent(ResultSet rs) throws SQLException {
        WorkflowRunEventRecord record = new WorkflowRunEventRecord();
        record.eventId = rs.getObject("event_id", UUID.class);
        record.eventType = rs.getString("event_type");
        record.payload = rs.getBytes("payload");
        record.kafkaKey = rs.getString("kafka_key");
        record.attempts = rs.getInt("attempts");
        record.status = rs.getString("status");
        record.createdAt = toInstant(rs.getObject("created_at", OffsetDateTime.class));
        record.publishedAt = toInstant(rs.getObject("published_at", OffsetDateTime.class));
        record.lastError = rs.getString("last_error");
        return record;
    }

    private static Instant toInstant(OffsetDateTime value) {
        return value == null ? null : value.toInstant();
    }

}
