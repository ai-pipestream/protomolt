package ai.pipestream.proto.jobs.service.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * The Postgres default {@link WorkflowRunStore}: stateless SQL over the
 * {@link WorkflowRunDatabase} wrapper. All SQL is prepared-statement
 * parameterized; the JSONB columns ride as JSON text (the worker and the
 * actions own the Jackson work). Every commit point writes its outbox event
 * in the same transaction as the job mutation — that is the outbox
 * pattern's whole point.
 */
public final class JdbcWorkflowRunStore implements WorkflowRunStore {

    /** Cap on the outbox's {@code last_error} column content. */
    private static final int MAX_ERROR_LENGTH = 4000;

    private static final ObjectMapper JSON = new ObjectMapper();

    // Both column lists end with a line break (the closing delimiter sits on its
    // own line) so every splice below joins onto the next clause cleanly.
    private static final String JOB_COLUMNS = """
            job_id, workflow_name, workflow_definition, input, input_ref, status, attempt,
            max_attempts, run_after, outstanding_step, checkpoints, result,
            result_ref, verdict, error, lease_owner, lease_until, created_at,
            updated_at, completed_at
            """;

    private static final String EVENT_COLUMNS = """
            event_id, event_type, payload, kafka_key, attempts, status, created_at,
            published_at, last_error
            """;

    private final WorkflowRunDatabase database;

    /**
     * @param database the jobs store's database wrapper
     */
    public JdbcWorkflowRunStore(WorkflowRunDatabase database) {
        this.database = database;
    }

    @Override
    public InsertOutcome insert(WorkflowRunRecord job, WorkflowRunEventRecord event) {
        return database.inTransaction(c -> {
            String sql = """
                    INSERT INTO workflow_run (job_id, workflow_name, workflow_definition, input,
                                              input_ref, status, attempt, max_attempts, run_after,
                                              outstanding_step, checkpoints, result, result_ref,
                                              verdict, error, lease_owner, lease_until, completed_at)
                    VALUES (?, ?, ?::jsonb, ?::jsonb, ?, ?, ?, ?, ?, ?, ?::jsonb,
                            ?::jsonb, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (job_id) DO NOTHING""";
            int inserted;
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setObject(1, job.jobId);
                ps.setString(2, job.workflowName);
                ps.setObject(3, job.workflowDefinition, Types.OTHER);
                ps.setObject(4, job.input, Types.OTHER);
                setText(ps, 5, job.inputRef);
                ps.setString(6, job.status);
                ps.setInt(7, job.attempt);
                ps.setInt(8, job.maxAttempts);
                setInstant(ps, 9, job.runAfter == null ? Instant.now() : job.runAfter);
                setText(ps, 10, job.outstandingStep);
                ps.setObject(11, job.checkpoints == null ? "[]" : job.checkpoints, Types.OTHER);
                ps.setObject(12, job.result, Types.OTHER);
                setText(ps, 13, job.resultRef);
                setText(ps, 14, job.verdict);
                setText(ps, 15, job.error);
                setText(ps, 16, job.leaseOwner);
                setInstant(ps, 17, job.leaseUntil);
                setInstant(ps, 18, job.completedAt);
                inserted = ps.executeUpdate();
            } catch (SQLException e) {
                throw WorkflowRunStoreException.wrap("insert workflow run failed", e);
            }
            if (inserted == 1) {
                // The event commits or rolls back with the job row it
                // describes — the outbox pattern's whole point.
                enqueue(c, event);
            }
            return new InsertOutcome(get(c, job.jobId)
                    .orElseThrow(() -> WorkflowRunStoreException.notFound(job.jobId)),
                    inserted == 1);
        });
    }

    @Override
    public Optional<WorkflowRunRecord> get(UUID jobId) {
        return database.readOnly(c -> get(c, jobId));
    }

    private Optional<WorkflowRunRecord> get(Connection c, UUID jobId) {
        String sql = "SELECT " + JOB_COLUMNS + " FROM workflow_run WHERE job_id = ?";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setObject(1, jobId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(mapJob(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw WorkflowRunStoreException.wrap("get workflow run failed", e);
        }
    }

    @Override
    public List<WorkflowRunRecord> list(String statusOrNull, String workflowNameOrNull, int limit,
            long offset) {
        StringBuilder where = new StringBuilder();
        List<String> filters = new ArrayList<>(2);
        if (statusOrNull != null) {
            where.append(" WHERE status = ?");
            filters.add(statusOrNull);
        }
        if (workflowNameOrNull != null) {
            where.append(where.isEmpty() ? " WHERE" : " AND").append(" workflow_name = ?");
            filters.add(workflowNameOrNull);
        }
        return database.readOnly(c -> {
            String sql = "SELECT " + JOB_COLUMNS + " FROM workflow_run" + where
                    + " ORDER BY created_at DESC, job_id DESC LIMIT ? OFFSET ?";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                int p = 1;
                for (String filter : filters) {
                    ps.setString(p++, filter);
                }
                ps.setInt(p++, limit);
                ps.setLong(p, offset);
                List<WorkflowRunRecord> rows = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        rows.add(mapJob(rs));
                    }
                }
                return rows;
            } catch (SQLException e) {
                throw WorkflowRunStoreException.wrap("list workflow runs failed", e);
            }
        });
    }

    @Override
    public Optional<WorkflowRunRecord> claim(String workerId, Duration leaseDuration) {
        return database.inTransaction(c -> {
            // One statement: the sub-select finds the oldest eligible QUEUED
            // job and locks it (SKIP LOCKED — concurrent workers never claim
            // the same row), the update flips it RUNNING under that lock.
            String sql = """
                    UPDATE workflow_run SET status = 'RUNNING', lease_owner = ?,
                           lease_until = ?, attempt = attempt + 1, updated_at = now()
                     WHERE job_id = (SELECT job_id FROM workflow_run
                                      WHERE status = 'QUEUED' AND run_after <= now()
                                      ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1)
                    RETURNING
                    """ + JOB_COLUMNS;
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, workerId);
                ps.setObject(2, Instant.now().plus(leaseDuration).atOffset(ZoneOffset.UTC));
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? Optional.of(mapJob(rs)) : Optional.empty();
                }
            } catch (SQLException e) {
                throw WorkflowRunStoreException.wrap("claim workflow run failed", e);
            }
        });
    }

    @Override
    public int requeueExpiredLeases() {
        return database.inTransaction(c -> {
            String sql = """
                    UPDATE workflow_run SET status = 'QUEUED', lease_owner = NULL,
                           lease_until = NULL, updated_at = now()
                     WHERE status = 'RUNNING' AND lease_until IS NOT NULL
                       AND lease_until < now()""";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                return ps.executeUpdate();
            } catch (SQLException e) {
                throw WorkflowRunStoreException.wrap("lease sweep failed", e);
            }
        });
    }

    @Override
    public void saveCheckpoint(UUID jobId, String checkpointsJson,
            WorkflowRunEventRecord stepEvent) {
        database.inTransaction(c -> {
            update(c, """
                    UPDATE workflow_run SET checkpoints = ?::jsonb, updated_at = now()
                     WHERE job_id = ?""", ps -> {
                ps.setObject(1, checkpointsJson, Types.OTHER);
                ps.setObject(2, jobId);
            }, jobId);
            enqueue(c, stepEvent);
            return null;
        });
    }

    @Override
    public void markWaiting(UUID jobId, String stepName, String checkpointsJson,
            WorkflowRunEventRecord event) {
        database.inTransaction(c -> {
            update(c, """
                    UPDATE workflow_run SET status = 'WAITING', outstanding_step = ?,
                           checkpoints = ?::jsonb, lease_owner = NULL, lease_until = NULL,
                           updated_at = now()
                     WHERE job_id = ?""", ps -> {
                ps.setString(1, stepName);
                ps.setObject(2, checkpointsJson, Types.OTHER);
                ps.setObject(3, jobId);
            }, jobId);
            enqueue(c, event);
            return null;
        });
    }

    @Override
    public void markCompleted(UUID jobId, String resultJson, String verdict,
            WorkflowRunEventRecord event) {
        database.inTransaction(c -> {
            update(c, """
                    UPDATE workflow_run SET status = 'COMPLETED', result = ?::jsonb,
                           verdict = ?, lease_owner = NULL, lease_until = NULL,
                           outstanding_step = NULL, completed_at = now(), updated_at = now()
                     WHERE job_id = ?""", ps -> {
                ps.setObject(1, resultJson, Types.OTHER);
                ps.setString(2, verdict);
                ps.setObject(3, jobId);
            }, jobId);
            enqueue(c, event);
            return null;
        });
    }

    @Override
    public void markFailed(UUID jobId, String error, WorkflowRunEventRecord event) {
        markTerminal("FAILED", jobId, error, event);
    }

    @Override
    public void markDead(UUID jobId, String error, WorkflowRunEventRecord event) {
        markTerminal("DEAD", jobId, error, event);
    }

    /** FAILED and DEAD share their shape: status, verbatim error, lease cleared. */
    private void markTerminal(String status, UUID jobId, String error,
            WorkflowRunEventRecord event) {
        database.inTransaction(c -> {
            update(c, """
                    UPDATE workflow_run SET status = ?, error = ?,
                           lease_owner = NULL, lease_until = NULL, outstanding_step = NULL,
                           completed_at = now(), updated_at = now()
                     WHERE job_id = ?""", ps -> {
                ps.setString(1, status);
                ps.setString(2, error);
                ps.setObject(3, jobId);
            }, jobId);
            enqueue(c, event);
            return null;
        });
    }

    @Override
    public void requeue(UUID jobId, Duration delay) {
        database.inTransaction(c -> {
            update(c, """
                    UPDATE workflow_run SET status = 'QUEUED', run_after = ?,
                           lease_owner = NULL, lease_until = NULL, updated_at = now()
                     WHERE job_id = ?""", ps -> {
                ps.setObject(1, Instant.now().plus(delay).atOffset(ZoneOffset.UTC));
                ps.setObject(2, jobId);
            }, jobId);
            return null;
        });
    }

    @Override
    public ParkedCompletion completeParkedStep(UUID jobId, String stepName,
            String checkpointEntryJson, WorkflowRunEventRecord stepEvent) {
        return database.inTransaction(c -> {
            String select = """
                    SELECT status, outstanding_step, checkpoints FROM workflow_run
                     WHERE job_id = ? FOR UPDATE""";
            String status;
            String outstanding;
            ArrayNode checkpoints;
            try (PreparedStatement ps = c.prepareStatement(select)) {
                ps.setObject(1, jobId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw WorkflowRunStoreException.notFound(jobId);
                    }
                    status = rs.getString("status");
                    outstanding = rs.getString("outstanding_step");
                    checkpoints = readArray(rs.getString("checkpoints"), jobId);
                }
            } catch (SQLException e) {
                throw WorkflowRunStoreException.wrap("complete-parked-step read failed", e);
            }
            // Idempotent redelivery: the step's checkpoint already persisted.
            for (JsonNode entry : checkpoints) {
                if (stepName.equals(entry.path("name").asText())) {
                    return new ParkedCompletion.AlreadyDone(status);
                }
            }
            if (!WorkflowRunRecord.STATUS_WAITING.equals(status)
                    || !stepName.equals(outstanding)) {
                return new ParkedCompletion.WrongState(status, outstanding);
            }
            JsonNode entry;
            try {
                entry = JSON.readTree(checkpointEntryJson);
            } catch (Exception e) {
                throw WorkflowRunStoreException.wrap("checkpoint entry is not valid JSON", e);
            }
            checkpoints.add(entry);
            update(c, """
                    UPDATE workflow_run SET status = 'QUEUED', outstanding_step = NULL,
                           checkpoints = ?::jsonb, run_after = now(), updated_at = now()
                     WHERE job_id = ?""", ps -> {
                ps.setObject(1, checkpoints.toString(), Types.OTHER);
                ps.setObject(2, jobId);
            }, jobId);
            enqueue(c, stepEvent);
            return new ParkedCompletion.Completed();
        });
    }

    @Override
    public List<WorkflowRunEventRecord> pollPendingEvents(int limit) {
        return database.inTransaction(c -> {
            // Oldest event first so relay lag is bounded by the outbox, not
            // by chance. The locks release when this method's transaction
            // commits — claimed rows stay PENDING and re-claimable until
            // settled (at-least-once claiming, not exclusive ownership).
            String sql = "SELECT " + EVENT_COLUMNS + """
                    FROM workflow_run_events_outbox
                    WHERE status = 'PENDING' ORDER BY created_at ASC, event_id ASC
                    LIMIT ? FOR UPDATE SKIP LOCKED""";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setInt(1, limit);
                List<WorkflowRunEventRecord> claimed = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        claimed.add(mapEvent(rs));
                    }
                }
                return claimed;
            } catch (SQLException e) {
                throw WorkflowRunStoreException.wrap("outbox poll failed", e);
            }
        });
    }

    @Override
    public boolean markEventPublished(UUID eventId) {
        return database.inTransaction(c -> {
            String sql = """
                    UPDATE workflow_run_events_outbox SET status = 'PUBLISHED',
                           published_at = ?
                     WHERE event_id = ? AND status = 'PENDING'""";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setObject(1, Instant.now().atOffset(ZoneOffset.UTC));
                ps.setObject(2, eventId);
                return ps.executeUpdate() == 1;
            } catch (SQLException e) {
                throw WorkflowRunStoreException.wrap("outbox mark-published failed", e);
            }
        });
    }

    @Override
    public Optional<WorkflowRunEventRecord> markEventFailed(WorkflowRunEventRecord event,
            String error) {
        return database.inTransaction(c -> {
            String select = "SELECT " + EVENT_COLUMNS + """
                    FROM workflow_run_events_outbox
                    WHERE event_id = ? FOR UPDATE""";
            try (PreparedStatement ps = c.prepareStatement(select)) {
                ps.setObject(1, event.eventId);
                WorkflowRunEventRecord managed;
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()
                            || !WorkflowRunEventRecord.STATUS_PENDING.equals(rs.getString("status"))) {
                        return Optional.empty();
                    }
                    managed = mapEvent(rs);
                }
                managed.attempts = managed.attempts + 1;
                managed.lastError = truncate(error);
                if (managed.attempts >= WorkflowRunEventRecord.MAX_ATTEMPTS) {
                    managed.status = WorkflowRunEventRecord.STATUS_FAILED;
                }
                try (PreparedStatement update = c.prepareStatement("""
                        UPDATE workflow_run_events_outbox SET attempts = ?, status = ?,
                               last_error = ?
                         WHERE event_id = ?""")) {
                    update.setInt(1, managed.attempts);
                    update.setString(2, managed.status);
                    if (managed.lastError == null) {
                        update.setNull(3, Types.VARCHAR);
                    } else {
                        update.setString(3, managed.lastError);
                    }
                    update.setObject(4, managed.eventId);
                    update.executeUpdate();
                }
                return Optional.of(managed);
            } catch (SQLException e) {
                throw WorkflowRunStoreException.wrap("outbox mark-failed failed", e);
            }
        });
    }

    /**
     * Insert the event row into the caller's transaction: the outbox write is
     * atomic with the job mutation the caller is committing.
     *
     * @param c the caller's connection (its transaction is the commit unit)
     * @param event the event to outbox
     */
    private static void enqueue(Connection c, WorkflowRunEventRecord event) {
        String sql = """
                INSERT INTO workflow_run_events_outbox (event_id, event_type, payload,
                                                        kafka_key, attempts, status, created_at)
                VALUES (?, ?, ?, ?, 0, 'PENDING', ?)""";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setObject(1, event.eventId);
            ps.setString(2, event.eventType);
            ps.setBytes(3, event.payload);
            ps.setString(4, event.kafkaKey);
            ps.setObject(5, event.createdAt.atOffset(ZoneOffset.UTC));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw WorkflowRunStoreException.wrap("outbox enqueue failed", e);
        }
    }

    /** One conditional update with parameter binding; the row must exist. */
    private static void update(Connection c, String sql, SqlBinder binder, UUID jobId) {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            binder.bind(ps);
            if (ps.executeUpdate() != 1) {
                throw WorkflowRunStoreException.notFound(jobId);
            }
        } catch (SQLException e) {
            throw WorkflowRunStoreException.wrap("update workflow run failed", e);
        }
    }

    @FunctionalInterface
    private interface SqlBinder {
        void bind(PreparedStatement ps) throws SQLException;
    }

    private static ArrayNode readArray(String json, UUID jobId) {
        try {
            JsonNode node = JSON.readTree(json == null ? "[]" : json);
            if (node instanceof ArrayNode array) {
                return array;
            }
        } catch (Exception e) {
            throw WorkflowRunStoreException.wrap("stored checkpoints are not readable", e);
        }
        // Corrupt state the CHECK constraints cannot catch: fail loud.
        throw new IllegalStateException(
                "stored checkpoints are not an array for job " + jobId);
    }

    private static void setText(PreparedStatement ps, int index, String value)
            throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.VARCHAR);
        } else {
            ps.setString(index, value);
        }
    }

    private static void setInstant(PreparedStatement ps, int index, Instant value)
            throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.TIMESTAMP_WITH_TIMEZONE);
        } else {
            ps.setObject(index, value.atOffset(ZoneOffset.UTC));
        }
    }

    private static WorkflowRunRecord mapJob(ResultSet rs) throws SQLException {
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

    private static WorkflowRunEventRecord mapEvent(ResultSet rs) throws SQLException {
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

    private static String truncate(String error) {
        if (error == null) {
            return null;
        }
        return error.length() <= MAX_ERROR_LENGTH ? error : error.substring(0, MAX_ERROR_LENGTH);
    }
}
