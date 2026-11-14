package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.archive.RepositorySinkJobStore;
import ai.protomolt.proto.repo.sink.v1.PublishRepositorySinkRequest;
import ai.protomolt.proto.repo.sink.v1.RenditionProvenance;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJob;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJobState;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import javax.sql.DataSource;

/**
 * Durable PostgreSQL implementation of {@link RepositorySinkJobStore} backed by isolated table
 * {@code repo_sink.sink_jobs} with monotonic lease fencing and cooperative cross-instance
 * cancellation.
 *
 * <p>All reads, heartbeats, and conditional state transitions execute as short autocommit
 * statements with bounded query timeouts so no database connection or transaction is ever held open
 * across source reads, renderer execution, or streamed repository uploads.
 */
final class JdbcRepositorySinkJobStore implements RepositorySinkJobStore {

    private static final int STATEMENT_TIMEOUT_SECONDS = 15;

    private static final String CREATE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS repo_sink";
    private static final String CREATE_TABLE_SQL =
            """
            CREATE TABLE IF NOT EXISTS repo_sink.sink_jobs (
                account_id TEXT NOT NULL,
                operation_id TEXT NOT NULL,
                state TEXT NOT NULL,
                job_pb BYTEA NOT NULL,
                request_pb BYTEA NOT NULL,
                pending_provenance_pb BYTEA,
                owner_id TEXT NOT NULL DEFAULT 'unowned',
                lease_epoch BIGINT NOT NULL DEFAULT 1,
                lease_expires_at TIMESTAMPTZ NOT NULL DEFAULT TIMESTAMP WITH TIME ZONE '1970-01-01 00:00:00+00',
                cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
                cancel_reason TEXT NOT NULL DEFAULT '',
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (account_id, operation_id)
            )
            """;

    private static final String ALTER_OWNER_SQL =
            "ALTER TABLE repo_sink.sink_jobs ADD COLUMN IF NOT EXISTS owner_id TEXT NOT NULL DEFAULT 'unowned'";
    private static final String ALTER_EPOCH_SQL =
            "ALTER TABLE repo_sink.sink_jobs ADD COLUMN IF NOT EXISTS lease_epoch BIGINT NOT NULL DEFAULT 1";
    private static final String ALTER_EXPIRES_SQL =
            "ALTER TABLE repo_sink.sink_jobs ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ NOT NULL DEFAULT TIMESTAMP WITH TIME ZONE '1970-01-01 00:00:00+00'";
    private static final String ALTER_CANCEL_REQ_SQL =
            "ALTER TABLE repo_sink.sink_jobs ADD COLUMN IF NOT EXISTS cancel_requested BOOLEAN NOT NULL DEFAULT FALSE";
    private static final String ALTER_CANCEL_REASON_SQL =
            "ALTER TABLE repo_sink.sink_jobs ADD COLUMN IF NOT EXISTS cancel_reason TEXT NOT NULL DEFAULT ''";

    private static final String SELECT_SQL =
            """
            SELECT job_pb, request_pb, pending_provenance_pb,
                   owner_id, lease_epoch, lease_expires_at,
                   cancel_requested, cancel_reason
              FROM repo_sink.sink_jobs
             WHERE account_id = ? AND operation_id = ?
            """;

    private static final String INSERT_IF_ABSENT_SQL =
            """
            INSERT INTO repo_sink.sink_jobs (
                account_id, operation_id, state, job_pb, request_pb, pending_provenance_pb,
                owner_id, lease_epoch, lease_expires_at, cancel_requested, cancel_reason,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (account_id, operation_id) DO NOTHING
            """;

    private static final String RENEW_LEASE_SQL =
            """
            UPDATE repo_sink.sink_jobs
               SET lease_expires_at = ?,
                   updated_at = now()
             WHERE account_id = ?
               AND operation_id = ?
               AND owner_id = ?
               AND lease_epoch = ?
               AND state IN (
                   'REPOSITORY_SINK_JOB_STATE_QUEUED',
                   'REPOSITORY_SINK_JOB_STATE_RUNNING'
               )
            RETURNING cancel_requested, cancel_reason
            """;

    private static final String PREPARE_COMMIT_SQL =
            """
            UPDATE repo_sink.sink_jobs
               SET state = ?,
                   job_pb = ?,
                   request_pb = ?,
                   pending_provenance_pb = ?,
                   lease_expires_at = ?,
                   updated_at = now()
             WHERE account_id = ?
               AND operation_id = ?
               AND owner_id = ?
               AND lease_epoch = ?
               AND state = 'REPOSITORY_SINK_JOB_STATE_RUNNING'
               AND cancel_requested = FALSE
            """;

    private static final String TRANSITION_OWNED_SQL =
            """
            UPDATE repo_sink.sink_jobs
               SET state = ?,
                   job_pb = ?,
                   request_pb = ?,
                   pending_provenance_pb = ?,
                   lease_expires_at = ?,
                   updated_at = now()
             WHERE account_id = ?
               AND operation_id = ?
               AND owner_id = ?
               AND lease_epoch = ?
               AND state NOT IN (
                   'REPOSITORY_SINK_JOB_STATE_SUCCEEDED',
                   'REPOSITORY_SINK_JOB_STATE_FAILED',
                   'REPOSITORY_SINK_JOB_STATE_CANCELLED'
               )
            """;

    private static final String REQUEST_CANCEL_SQL =
            """
            UPDATE repo_sink.sink_jobs
               SET cancel_requested = TRUE,
                   cancel_reason = CASE WHEN cancel_reason = '' THEN ? ELSE cancel_reason END,
                   updated_at = now()
             WHERE account_id = ?
               AND operation_id = ?
               AND state IN (
                   'REPOSITORY_SINK_JOB_STATE_QUEUED',
                   'REPOSITORY_SINK_JOB_STATE_RUNNING'
               )
            """;

    private static final String CLAIM_EXPIRED_SQL =
            """
            UPDATE repo_sink.sink_jobs
               SET owner_id = ?,
                   lease_epoch = lease_epoch + 1,
                   lease_expires_at = ?,
                   updated_at = now()
             WHERE account_id = ?
               AND operation_id = ?
               AND lease_epoch = ?
               AND lease_expires_at <= ?
               AND state IN (
                   'REPOSITORY_SINK_JOB_STATE_QUEUED',
                   'REPOSITORY_SINK_JOB_STATE_RUNNING',
                   'REPOSITORY_SINK_JOB_STATE_UNRESOLVED'
               )
            RETURNING job_pb, request_pb, pending_provenance_pb,
                      owner_id, lease_epoch, lease_expires_at,
                      cancel_requested, cancel_reason
            """;

    private static final String CLAIM_RETRYABLE_FAILURE_SQL =
            """
            UPDATE repo_sink.sink_jobs
               SET state = 'REPOSITORY_SINK_JOB_STATE_QUEUED',
                   job_pb = ?,
                   pending_provenance_pb = NULL,
                   owner_id = ?,
                   lease_epoch = lease_epoch + 1,
                   lease_expires_at = ?,
                   cancel_requested = FALSE,
                   cancel_reason = '',
                   updated_at = now()
             WHERE account_id = ?
               AND operation_id = ?
               AND lease_epoch = ?
               AND state = 'REPOSITORY_SINK_JOB_STATE_FAILED'
            RETURNING job_pb, request_pb, pending_provenance_pb,
                      owner_id, lease_epoch, lease_expires_at,
                      cancel_requested, cancel_reason
            """;

    private final DataSource dataSource;

    JdbcRepositorySinkJobStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        initializeSchema();
    }

    private void initializeSchema() {
        withInterruptCleared(() -> {
            try (Connection conn = openAutocommitConnection();
                    Statement stmt = conn.createStatement()) {
                stmt.setQueryTimeout(STATEMENT_TIMEOUT_SECONDS);
                stmt.execute(CREATE_SCHEMA_SQL);
                stmt.execute(CREATE_TABLE_SQL);
                stmt.execute(ALTER_OWNER_SQL);
                stmt.execute(ALTER_EPOCH_SQL);
                stmt.execute(ALTER_EXPIRES_SQL);
                stmt.execute(ALTER_CANCEL_REQ_SQL);
                stmt.execute(ALTER_CANCEL_REASON_SQL);
                return null;
            } catch (SQLException e) {
                throw new IllegalStateException(
                        "Failed to initialize repo_sink.sink_jobs schema", e);
            }
        });
    }

    @Override
    public Optional<StoredJob> find(String accountId, String operationId) {
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(operationId, "operationId");
        return withInterruptCleared(() -> findDirect(accountId, operationId));
    }

    private Optional<StoredJob> findDirect(String accountId, String operationId) {
        try (Connection conn = openAutocommitConnection();
                PreparedStatement ps = conn.prepareStatement(SELECT_SQL)) {
            ps.setQueryTimeout(STATEMENT_TIMEOUT_SECONDS);
            ps.setString(1, accountId);
            ps.setString(2, operationId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                return Optional.of(decodeRow(rs, accountId, operationId));
            }
        } catch (SQLException e) {
            throw Status.UNAVAILABLE
                    .withDescription("Repository sink job store read failed")
                    .withCause(e)
                    .asRuntimeException();
        }
    }

    @Override
    public StoredJob insertOrGet(StoredJob candidate) {
        Objects.requireNonNull(candidate, "candidate");
        return withInterruptCleared(() -> {
            String accountId = candidate.job().getAccountId();
            String operationId = candidate.job().getOperationId();
            try (Connection conn = openAutocommitConnection();
                    PreparedStatement ps = conn.prepareStatement(INSERT_IF_ABSENT_SQL)) {
                ps.setQueryTimeout(STATEMENT_TIMEOUT_SECONDS);
                RepositorySinkJob job = candidate.job();
                ps.setString(1, accountId);
                ps.setString(2, operationId);
                ps.setString(3, job.getState().name());
                ps.setBytes(4, job.toByteArray());
                ps.setBytes(5, candidate.request().toByteArray());
                bindNullableProvenance(ps, 6, candidate.pendingProvenance());
                ps.setString(7, candidate.lease().ownerId());
                ps.setLong(8, candidate.lease().leaseEpoch());
                ps.setTimestamp(9, Timestamp.from(candidate.lease().leaseExpiresAt()));
                ps.setBoolean(10, candidate.cancelRequested());
                ps.setString(11, candidate.cancelReason());
                int inserted = ps.executeUpdate();
                if (inserted == 1) {
                    return candidate;
                }
            } catch (SQLException e) {
                throw Status.UNAVAILABLE
                        .withDescription("Repository sink job store admission failed")
                        .withCause(e)
                        .asRuntimeException();
            }
            return findDirect(accountId, operationId)
                    .orElseThrow(() -> Status.INTERNAL
                            .withDescription(
                                    "Concurrent repository sink job row disappeared for "
                                            + operationId)
                            .asRuntimeException());
        });
    }

    @Override
    public Optional<LeaseRenewal> renewLease(
            String accountId,
            String operationId,
            String ownerId,
            long leaseEpoch,
            Instant newExpiresAt) {
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(operationId, "operationId");
        Objects.requireNonNull(ownerId, "ownerId");
        Objects.requireNonNull(newExpiresAt, "newExpiresAt");
        return withInterruptCleared(() -> {
            try (Connection conn = openAutocommitConnection();
                    PreparedStatement ps = conn.prepareStatement(RENEW_LEASE_SQL)) {
                ps.setQueryTimeout(STATEMENT_TIMEOUT_SECONDS);
                ps.setTimestamp(1, Timestamp.from(newExpiresAt));
                ps.setString(2, accountId);
                ps.setString(3, operationId);
                ps.setString(4, ownerId);
                ps.setLong(5, leaseEpoch);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return Optional.empty();
                    }
                    JobLease renewed = new JobLease(ownerId, leaseEpoch, newExpiresAt);
                    return Optional.of(
                            new LeaseRenewal(renewed, rs.getBoolean(1), rs.getString(2)));
                }
            } catch (SQLException e) {
                throw Status.UNAVAILABLE
                        .withDescription("Repository sink job lease renewal failed")
                        .withCause(e)
                        .asRuntimeException();
            }
        });
    }

    @Override
    public PreCommitDecision prepareCommit(StoredJob preCommit, String ownerId, long leaseEpoch) {
        Objects.requireNonNull(preCommit, "preCommit");
        Objects.requireNonNull(ownerId, "ownerId");
        return withInterruptCleared(() -> {
            RepositorySinkJob job = preCommit.job();
            String accountId = job.getAccountId();
            String operationId = job.getOperationId();
            try (Connection conn = openAutocommitConnection();
                    PreparedStatement ps = conn.prepareStatement(PREPARE_COMMIT_SQL)) {
                ps.setQueryTimeout(STATEMENT_TIMEOUT_SECONDS);
                bindTransitionColumns(ps, preCommit, ownerId, leaseEpoch);
                if (ps.executeUpdate() == 1) {
                    return new PreCommitDecision(PreCommitStatus.READY, preCommit);
                }
            } catch (SQLException e) {
                throw Status.UNAVAILABLE
                        .withDescription("Repository sink pre-commit lease check failed")
                        .withCause(e)
                        .asRuntimeException();
            }
            Optional<StoredJob> currentOpt = findDirect(accountId, operationId);
            if (currentOpt.isPresent()) {
                StoredJob current = currentOpt.get();
                if (current.lease().matches(ownerId, leaseEpoch)
                        && current.job().getState()
                                == RepositorySinkJobState.REPOSITORY_SINK_JOB_STATE_RUNNING
                        && current.cancelRequested()) {
                    return new PreCommitDecision(PreCommitStatus.CANCEL_REQUESTED, current);
                }
                return new PreCommitDecision(PreCommitStatus.LEASE_LOST, current);
            }
            return new PreCommitDecision(PreCommitStatus.LEASE_LOST, preCommit);
        });
    }

    @Override
    public boolean transitionOwned(StoredJob updated, String ownerId, long leaseEpoch) {
        Objects.requireNonNull(updated, "updated");
        Objects.requireNonNull(ownerId, "ownerId");
        return withInterruptCleared(() -> {
            try (Connection conn = openAutocommitConnection();
                    PreparedStatement ps = conn.prepareStatement(TRANSITION_OWNED_SQL)) {
                ps.setQueryTimeout(STATEMENT_TIMEOUT_SECONDS);
                bindTransitionColumns(ps, updated, ownerId, leaseEpoch);
                return ps.executeUpdate() == 1;
            } catch (SQLException e) {
                throw Status.UNAVAILABLE
                        .withDescription("Repository sink job conditional transition failed")
                        .withCause(e)
                        .asRuntimeException();
            }
        });
    }

    @Override
    public Optional<StoredJob> requestCancel(String accountId, String operationId, String reason) {
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(operationId, "operationId");
        String normalizedReason = reason == null ? "" : reason.trim();
        return withInterruptCleared(() -> {
            try (Connection conn = openAutocommitConnection();
                    PreparedStatement ps = conn.prepareStatement(REQUEST_CANCEL_SQL)) {
                ps.setQueryTimeout(STATEMENT_TIMEOUT_SECONDS);
                ps.setString(1, normalizedReason);
                ps.setString(2, accountId);
                ps.setString(3, operationId);
                ps.executeUpdate();
            } catch (SQLException e) {
                throw Status.UNAVAILABLE
                        .withDescription("Repository sink job cancellation request failed")
                        .withCause(e)
                        .asRuntimeException();
            }
            return findDirect(accountId, operationId);
        });
    }

    @Override
    public Optional<StoredJob> tryClaimExpiredOrUnresolved(
            String accountId,
            String operationId,
            long expectedLeaseEpoch,
            String newOwnerId,
            Instant now,
            Instant newExpiresAt) {
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(operationId, "operationId");
        Objects.requireNonNull(newOwnerId, "newOwnerId");
        Objects.requireNonNull(now, "now");
        Objects.requireNonNull(newExpiresAt, "newExpiresAt");
        return withInterruptCleared(() -> {
            try (Connection conn = openAutocommitConnection();
                    PreparedStatement ps = conn.prepareStatement(CLAIM_EXPIRED_SQL)) {
                ps.setQueryTimeout(STATEMENT_TIMEOUT_SECONDS);
                ps.setString(1, newOwnerId);
                ps.setTimestamp(2, Timestamp.from(newExpiresAt));
                ps.setString(3, accountId);
                ps.setString(4, operationId);
                ps.setLong(5, expectedLeaseEpoch);
                ps.setTimestamp(6, Timestamp.from(now));
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return Optional.empty();
                    }
                    return Optional.of(decodeRow(rs, accountId, operationId));
                }
            } catch (SQLException e) {
                throw Status.UNAVAILABLE
                        .withDescription("Repository sink expired-lease claim failed")
                        .withCause(e)
                        .asRuntimeException();
            }
        });
    }

    @Override
    public Optional<StoredJob> tryClaimRetryableFailure(
            String accountId,
            String operationId,
            long expectedLeaseEpoch,
            RepositorySinkJob requeuedJob,
            String newOwnerId,
            Instant newExpiresAt) {
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(operationId, "operationId");
        Objects.requireNonNull(requeuedJob, "requeuedJob");
        Objects.requireNonNull(newOwnerId, "newOwnerId");
        Objects.requireNonNull(newExpiresAt, "newExpiresAt");
        return withInterruptCleared(() -> {
            try (Connection conn = openAutocommitConnection();
                    PreparedStatement ps = conn.prepareStatement(CLAIM_RETRYABLE_FAILURE_SQL)) {
                ps.setQueryTimeout(STATEMENT_TIMEOUT_SECONDS);
                ps.setBytes(1, requeuedJob.toByteArray());
                ps.setString(2, newOwnerId);
                ps.setTimestamp(3, Timestamp.from(newExpiresAt));
                ps.setString(4, accountId);
                ps.setString(5, operationId);
                ps.setLong(6, expectedLeaseEpoch);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return Optional.empty();
                    }
                    return Optional.of(decodeRow(rs, accountId, operationId));
                }
            } catch (SQLException e) {
                throw Status.UNAVAILABLE
                        .withDescription("Repository sink retry lease claim failed")
                        .withCause(e)
                        .asRuntimeException();
            }
        });
    }

    private Connection openAutocommitConnection() throws SQLException {
        Connection conn = dataSource.getConnection();
        if (!conn.getAutoCommit()) {
            conn.setAutoCommit(true);
        }
        return conn;
    }

    private static void bindTransitionColumns(
            PreparedStatement ps, StoredJob stored, String ownerId, long leaseEpoch)
            throws SQLException {
        RepositorySinkJob job = stored.job();
        ps.setString(1, job.getState().name());
        ps.setBytes(2, job.toByteArray());
        ps.setBytes(3, stored.request().toByteArray());
        bindNullableProvenance(ps, 4, stored.pendingProvenance());
        ps.setTimestamp(5, Timestamp.from(stored.lease().leaseExpiresAt()));
        ps.setString(6, job.getAccountId());
        ps.setString(7, job.getOperationId());
        ps.setString(8, ownerId);
        ps.setLong(9, leaseEpoch);
    }

    private static void bindNullableProvenance(
            PreparedStatement ps, int index, RenditionProvenance provenance) throws SQLException {
        if (provenance != null) {
            ps.setBytes(index, provenance.toByteArray());
        } else {
            ps.setNull(index, Types.BINARY);
        }
    }

    private static StoredJob decodeRow(ResultSet rs, String accountId, String operationId)
            throws SQLException {
        byte[] jobBytes = rs.getBytes(1);
        byte[] requestBytes = rs.getBytes(2);
        byte[] pendingProvBytes = rs.getBytes(3);
        String ownerId = rs.getString(4);
        long leaseEpoch = rs.getLong(5);
        Timestamp expiresTs = rs.getTimestamp(6);
        boolean cancelRequested = rs.getBoolean(7);
        String cancelReason = rs.getString(8);
        try {
            RepositorySinkJob job = RepositorySinkJob.parseFrom(jobBytes);
            PublishRepositorySinkRequest request =
                    PublishRepositorySinkRequest.parseFrom(requestBytes);
            RenditionProvenance pendingProvenance =
                    pendingProvBytes != null ? RenditionProvenance.parseFrom(pendingProvBytes) : null;
            Instant expiresAt = expiresTs != null ? expiresTs.toInstant() : Instant.EPOCH;
            JobLease lease = new JobLease(
                    ownerId == null || ownerId.isBlank() ? "unowned" : ownerId,
                    Math.max(1L, leaseEpoch),
                    expiresAt);
            return new StoredJob(
                    job,
                    request,
                    pendingProvenance,
                    lease,
                    cancelRequested,
                    cancelReason);
        } catch (InvalidProtocolBufferException corrupt) {
            throw Status.DATA_LOSS
                    .withDescription(
                            "Corrupt persisted repository sink job record for "
                                    + accountId
                                    + "/"
                                    + operationId)
                    .withCause(corrupt)
                    .asRuntimeException();
        }
    }

    private static <T> T withInterruptCleared(Supplier<T> action) {
        boolean restoreInterrupt = Thread.interrupted();
        try {
            return action.get();
        } finally {
            if (restoreInterrupt) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
