package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.archive.RepositorySinkJobStore;
import ai.protomolt.proto.repo.sink.v1.PublishRepositorySinkRequest;
import ai.protomolt.proto.repo.sink.v1.RenditionProvenance;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJob;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;

/**
 * Durable PostgreSQL implementation of {@link RepositorySinkJobStore} backed by isolated table
 * {@code repo_sink.sink_jobs}.
 *
 * <p>All reads and state transitions execute as short autocommit statements with bounded query
 * timeouts so no database connection or transaction is ever held open across source reads,
 * renderer execution, or streamed repository uploads.
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
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (account_id, operation_id)
            )
            """;

    private static final String SELECT_SQL =
            """
            SELECT job_pb, request_pb, pending_provenance_pb
              FROM repo_sink.sink_jobs
             WHERE account_id = ? AND operation_id = ?
            """;

    private static final String INSERT_IF_ABSENT_SQL =
            """
            INSERT INTO repo_sink.sink_jobs (
                account_id, operation_id, state, job_pb, request_pb, pending_provenance_pb, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (account_id, operation_id) DO NOTHING
            """;

    private static final String UPSERT_SQL =
            """
            INSERT INTO repo_sink.sink_jobs (
                account_id, operation_id, state, job_pb, request_pb, pending_provenance_pb, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (account_id, operation_id) DO UPDATE SET
                state = EXCLUDED.state,
                job_pb = EXCLUDED.job_pb,
                request_pb = EXCLUDED.request_pb,
                pending_provenance_pb = EXCLUDED.pending_provenance_pb,
                updated_at = now()
            """;

    private final DataSource dataSource;

    JdbcRepositorySinkJobStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        initializeSchema();
    }

    private void initializeSchema() {
        try (Connection conn = openAutocommitConnection();
                Statement stmt = conn.createStatement()) {
            stmt.setQueryTimeout(STATEMENT_TIMEOUT_SECONDS);
            stmt.execute(CREATE_SCHEMA_SQL);
            stmt.execute(CREATE_TABLE_SQL);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to initialize repo_sink.sink_jobs schema", e);
        }
    }

    @Override
    public Optional<StoredJob> find(String accountId, String operationId) {
        Objects.requireNonNull(accountId, "accountId");
        Objects.requireNonNull(operationId, "operationId");
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
        String accountId = candidate.job().getAccountId();
        String operationId = candidate.job().getOperationId();
        try (Connection conn = openAutocommitConnection();
                PreparedStatement ps = conn.prepareStatement(INSERT_IF_ABSENT_SQL)) {
            ps.setQueryTimeout(STATEMENT_TIMEOUT_SECONDS);
            bindRowParameters(ps, candidate);
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
        return find(accountId, operationId)
                .orElseThrow(() -> Status.INTERNAL
                        .withDescription(
                                "Concurrent repository sink job row disappeared for " + operationId)
                        .asRuntimeException());
    }

    @Override
    public void save(StoredJob updated) {
        Objects.requireNonNull(updated, "updated");
        try (Connection conn = openAutocommitConnection();
                PreparedStatement ps = conn.prepareStatement(UPSERT_SQL)) {
            ps.setQueryTimeout(STATEMENT_TIMEOUT_SECONDS);
            bindRowParameters(ps, updated);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw Status.UNAVAILABLE
                    .withDescription("Repository sink job store update failed")
                    .withCause(e)
                    .asRuntimeException();
        }
    }

    private Connection openAutocommitConnection() throws SQLException {
        Connection conn = dataSource.getConnection();
        if (!conn.getAutoCommit()) {
            conn.setAutoCommit(true);
        }
        return conn;
    }

    private static void bindRowParameters(PreparedStatement ps, StoredJob stored)
            throws SQLException {
        RepositorySinkJob job = stored.job();
        ps.setString(1, job.getAccountId());
        ps.setString(2, job.getOperationId());
        ps.setString(3, job.getState().name());
        ps.setBytes(4, job.toByteArray());
        ps.setBytes(5, stored.request().toByteArray());
        if (stored.pendingProvenance() != null) {
            ps.setBytes(6, stored.pendingProvenance().toByteArray());
        } else {
            ps.setNull(6, Types.BINARY);
        }
    }

    private static StoredJob decodeRow(ResultSet rs, String accountId, String operationId)
            throws SQLException {
        byte[] jobBytes = rs.getBytes(1);
        byte[] requestBytes = rs.getBytes(2);
        byte[] pendingProvBytes = rs.getBytes(3);
        try {
            RepositorySinkJob job = RepositorySinkJob.parseFrom(jobBytes);
            PublishRepositorySinkRequest request =
                    PublishRepositorySinkRequest.parseFrom(requestBytes);
            RenditionProvenance pendingProvenance =
                    pendingProvBytes != null ? RenditionProvenance.parseFrom(pendingProvBytes) : null;
            return new StoredJob(job, request, pendingProvenance);
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
}
