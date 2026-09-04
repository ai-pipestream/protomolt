package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.ChannelRecord;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** PostgreSQL record plus outbox journal. Authoritative bodies are protobuf bytea. */
final class JdbcProcessorChannelJournal implements ProcessorChannelJournal {

    private final DataSource dataSource;
    private final String channelId;
    private final Connection writerLease;
    private boolean closed;

    JdbcProcessorChannelJournal(DataSource dataSource, String channelId) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        if (channelId == null || !channelId.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")) {
            throw new IllegalArgumentException("transactional channel id must be path-safe");
        }
        this.channelId = channelId;
        migrate();
        this.writerLease = acquireWriterLease();
    }

    @Override
    public synchronized List<ChannelRecord> load() {
        requireOpen();
        String sql = "SELECT record_body FROM mesh_processor_channel_record "
                + "WHERE channel_id = ? ORDER BY sequence";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, channelId);
            List<ChannelRecord> records = new ArrayList<>();
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    try {
                        records.add(ChannelRecord.parseFrom(rows.getBytes(1)));
                    } catch (InvalidProtocolBufferException e) {
                        throw new IllegalArgumentException(
                                "transactional-channel-protobuf-corrupt: channel "
                                        + channelId, e);
                    }
                }
            }
            return List.copyOf(records);
        } catch (SQLException e) {
            throw unavailable("load transactional channel", e);
        }
    }

    @Override
    public synchronized void append(ChannelRecord record) {
        requireOpen();
        byte[] bytes = record.toByteArray();
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                byte[] existing = existing(connection, record.getSequence());
                if (existing != null) {
                    if (!Arrays.equals(existing, bytes)) {
                        throw new IllegalArgumentException(
                                "transactional-channel-sequence-conflict: sequence "
                                        + record.getSequence());
                    }
                    connection.rollback();
                    return;
                }
                try (PreparedStatement statement = connection.prepareStatement("""
                        INSERT INTO mesh_processor_channel_record
                          (channel_id, sequence, record_body, recorded_at)
                        VALUES (?, ?, ?, ?)
                        """)) {
                    statement.setString(1, channelId);
                    statement.setLong(2, record.getSequence());
                    statement.setBytes(3, bytes);
                    statement.setObject(4, Instant.ofEpochSecond(
                            record.getRecordedAt().getSeconds(),
                            record.getRecordedAt().getNanos()).atOffset(ZoneOffset.UTC));
                    statement.executeUpdate();
                }
                try (PreparedStatement statement = connection.prepareStatement("""
                        INSERT INTO mesh_processor_channel_outbox
                          (event_id, channel_id, sequence, record_body, status)
                        VALUES (?, ?, ?, ?, 'PENDING')
                        """)) {
                    statement.setObject(1, UUID.nameUUIDFromBytes(
                            (channelId + '\0' + record.getSequence()).getBytes(
                                    java.nio.charset.StandardCharsets.UTF_8)));
                    statement.setString(2, channelId);
                    statement.setLong(3, record.getSequence());
                    statement.setBytes(4, bytes);
                    statement.executeUpdate();
                }
                connection.commit();
            } catch (RuntimeException | SQLException e) {
                rollback(connection, e);
                if (e instanceof RuntimeException runtime) {
                    throw runtime;
                }
                throw unavailable("append transactional channel", (SQLException) e);
            }
        } catch (SQLException e) {
            throw unavailable("open transactional channel transaction", e);
        }
    }

    synchronized List<OutboxEntry> claimOutbox(
            String owner, int limit, Duration leaseDuration, Instant now) {
        requireOpen();
        if (owner == null || owner.isBlank() || limit < 1 || limit > 10_000
                || leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("invalid transactional outbox claim");
        }
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement("""
                    WITH claimed AS (
                      SELECT event_id FROM mesh_processor_channel_outbox
                       WHERE channel_id = ?
                         AND (status = 'PENDING' OR
                              (status = 'CLAIMED' AND lease_until < ?))
                       ORDER BY sequence FOR UPDATE SKIP LOCKED LIMIT ?
                    )
                    UPDATE mesh_processor_channel_outbox o
                       SET status = 'CLAIMED', lease_owner = ?, lease_until = ?,
                           attempts = attempts + 1
                      FROM claimed WHERE o.event_id = claimed.event_id
                    RETURNING o.event_id, o.sequence, o.record_body, o.attempts
                    """)) {
                statement.setString(1, channelId);
                statement.setObject(2, now.atOffset(ZoneOffset.UTC));
                statement.setInt(3, limit);
                statement.setString(4, owner);
                statement.setObject(5, now.plus(leaseDuration).atOffset(ZoneOffset.UTC));
                List<OutboxEntry> entries = new ArrayList<>();
                try (ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) {
                        entries.add(new OutboxEntry((UUID) rows.getObject(1),
                                rows.getLong(2), rows.getBytes(3), rows.getInt(4)));
                    }
                }
                connection.commit();
                return List.copyOf(entries);
            } catch (SQLException e) {
                rollback(connection, e);
                throw unavailable("claim transactional outbox", e);
            }
        } catch (SQLException e) {
            throw unavailable("open transactional outbox transaction", e);
        }
    }

    synchronized void settleOutbox(UUID eventId, String owner) {
        updateOutbox(eventId, owner, "PUBLISHED", "");
    }

    synchronized void failOutbox(UUID eventId, String owner, String error) {
        updateOutbox(eventId, owner, "PENDING",
                error == null ? "relay failed" : error.substring(0, Math.min(4000, error.length())));
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            writerLease.close();
        } catch (SQLException e) {
            throw unavailable("release transactional channel writer lease", e);
        }
    }

    private void migrate() {
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("""
                    CREATE TABLE IF NOT EXISTS mesh_processor_channel_record (
                       channel_id varchar(128) NOT NULL,
                       sequence bigint NOT NULL,
                       record_body bytea NOT NULL,
                       recorded_at timestamptz NOT NULL,
                       PRIMARY KEY (channel_id, sequence)
                    )
                    """)) {
                statement.execute();
            }
            try (PreparedStatement statement = connection.prepareStatement("""
                    CREATE TABLE IF NOT EXISTS mesh_processor_channel_outbox (
                       event_id uuid PRIMARY KEY,
                       channel_id varchar(128) NOT NULL,
                       sequence bigint NOT NULL,
                       record_body bytea NOT NULL,
                       status varchar(16) NOT NULL,
                       attempts integer NOT NULL DEFAULT 0,
                       lease_owner varchar(256),
                       lease_until timestamptz,
                       last_error varchar(4000),
                       published_at timestamptz,
                       UNIQUE (channel_id, sequence)
                    )
                    """)) {
                statement.execute();
            }
        } catch (SQLException e) {
            throw unavailable("migrate transactional channel", e);
        }
    }

    private Connection acquireWriterLease() {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT pg_try_advisory_lock(hashtextextended(?, 0))")) {
                statement.setString(1, "protomolt-mesh-channel:" + channelId);
                try (ResultSet result = statement.executeQuery()) {
                    if (!result.next() || !result.getBoolean(1)) {
                        connection.close();
                        throw new IllegalStateException(
                                "transactional-channel-writer-fenced: channel '"
                                        + channelId + "' already has a durable writer");
                    }
                }
            }
            return connection;
        } catch (SQLException e) {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException closeFailure) {
                    e.addSuppressed(closeFailure);
                }
            }
            throw unavailable("acquire transactional channel writer lease", e);
        }
    }

    private byte[] existing(Connection connection, long sequence) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT record_body FROM mesh_processor_channel_record "
                        + "WHERE channel_id = ? AND sequence = ? FOR UPDATE")) {
            statement.setString(1, channelId);
            statement.setLong(2, sequence);
            try (ResultSet row = statement.executeQuery()) {
                return row.next() ? row.getBytes(1) : null;
            }
        }
    }

    private void updateOutbox(UUID eventId, String owner, String status, String error) {
        requireOpen();
        String sql = "UPDATE mesh_processor_channel_outbox SET status = ?, "
                + "lease_owner = NULL, lease_until = NULL, last_error = ?, "
                + "published_at = CASE WHEN ? = 'PUBLISHED' THEN now() ELSE published_at END "
                + "WHERE event_id = ? AND status = 'CLAIMED' AND lease_owner = ?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, status);
            statement.setString(2, error.isBlank() ? null : error);
            statement.setString(3, status);
            statement.setObject(4, eventId);
            statement.setString(5, owner);
            if (statement.executeUpdate() != 1) {
                throw new IllegalArgumentException(
                        "transactional-outbox-fence: event is not held by relay owner");
            }
        } catch (SQLException e) {
            throw unavailable("update transactional outbox", e);
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("transactional processor channel is closed");
        }
    }

    private static IllegalStateException unavailable(String operation, SQLException cause) {
        return new IllegalStateException(
                "transactional-channel-unavailable: " + operation + " failed", cause);
    }

    private static void rollback(Connection connection, Exception original) {
        try {
            connection.rollback();
        } catch (SQLException rollback) {
            original.addSuppressed(rollback);
        }
    }

    record OutboxEntry(UUID eventId, long sequence, byte[] recordBody, int attempts) {
        OutboxEntry {
            recordBody = recordBody.clone();
        }

        @Override
        public byte[] recordBody() {
            return recordBody.clone();
        }
    }
}
