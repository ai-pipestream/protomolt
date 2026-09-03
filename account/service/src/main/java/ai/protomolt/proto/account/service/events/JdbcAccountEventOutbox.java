package ai.protomolt.proto.account.service.events;

import ai.protomolt.proto.account.service.store.AccountDatabase;
import ai.protomolt.proto.account.service.store.AccountStoreException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * The transactional outbox ({@code account_events_outbox}) over plain JDBC
 * via the {@link AccountDatabase} wrapper — every method is one unit of
 * work, except {@link #enqueue(Connection, AccountEventRecord)}, which rides
 * the CALLER's transaction: that is the outbox pattern's whole point, the
 * event row commits or rolls back with the account mutation it describes.
 * <p>
 * {@link #claimBatch(int)} claims with {@code FOR UPDATE SKIP LOCKED} — that
 * bounds contention WITHIN a claim window (two relays draining at the same
 * instant never select the same row), and nothing more. The claim transaction
 * commits before the relay publishes, so the row is still PENDING — and
 * claimable by another relay — until the broker ack lands and it is marked
 * PUBLISHED. A second relay can therefore republish a row the first is still
 * working on; that redelivery is absorbed by the at-least-once contract
 * (consumers dedupe on {@code event_id}), not prevented here. Terminal
 * transitions are conditional updates ({@code WHERE status = 'PENDING'}) so a
 * record settled by a competing relay is never re-settled.
 */
public final class JdbcAccountEventOutbox {

    /** Cap on the {@code last_error} column's content. */
    private static final int MAX_ERROR_LENGTH = 4000;

    private static final String COLUMNS =
            "event_id, event_type, payload, kafka_key, attempts, status,"
                    + " created_at, published_at, last_error";

    private final AccountDatabase database;

    /**
     * @param database the account store's database wrapper
     */
    public JdbcAccountEventOutbox(AccountDatabase database) {
        this.database = database;
    }

    /**
     * Insert the event row into the caller's transaction: the outbox write is
     * atomic with the account mutation the caller is committing.
     *
     * @param c the caller's connection (its transaction is the commit unit)
     * @param record the event to outbox
     */
    public void enqueue(Connection c, AccountEventRecord record) {
        String sql = "INSERT INTO account_events_outbox (event_id, event_type, payload, kafka_key,"
                + " attempts, status, created_at) VALUES (?, ?, ?, ?, 0, 'PENDING', ?)";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setObject(1, record.eventId);
            ps.setString(2, record.eventType);
            ps.setBytes(3, record.payload);
            ps.setString(4, record.kafkaKey);
            ps.setObject(5, record.createdAt.atOffset(ZoneOffset.UTC));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw AccountStoreException.wrap("outbox enqueue failed", e);
        }
    }

    /**
     * Claim up to {@code limit} PENDING records, oldest first
     * ({@code FOR UPDATE SKIP LOCKED}). The lock releases when this method's
     * transaction commits — claimed rows stay PENDING and re-claimable until
     * the caller settles them, so this is at-least-once claiming, not
     * exclusive ownership (see the class javadoc).
     *
     * @param limit the batch size
     * @return the claimed records (detached), possibly empty
     */
    public List<AccountEventRecord> claimBatch(int limit) {
        return database.inTransaction(c -> {
            // Oldest event first so relay lag is bounded by the outbox, not
            // by chance.
            String sql = "SELECT " + COLUMNS + " FROM account_events_outbox"
                    + " WHERE status = 'PENDING' ORDER BY created_at ASC, event_id ASC"
                    + " LIMIT ? FOR UPDATE SKIP LOCKED";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setInt(1, limit);
                List<AccountEventRecord> claimed = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        claimed.add(map(rs));
                    }
                }
                return claimed;
            } catch (SQLException e) {
                throw AccountStoreException.wrap("outbox claim failed", e);
            }
        });
    }

    /**
     * Mark a claimed record PUBLISHED after the broker ack. Conditional on
     * PENDING: a record settled by a competing relay is not re-settled.
     *
     * @param eventId the record's id
     * @param publishedAt when the broker ack landed
     * @return true when this call moved the record
     */
    public boolean markPublished(UUID eventId, Instant publishedAt) {
        return database.inTransaction(c -> {
            String sql = "UPDATE account_events_outbox SET status = 'PUBLISHED', published_at = ?"
                    + " WHERE event_id = ? AND status = 'PENDING'";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setObject(1, publishedAt.atOffset(ZoneOffset.UTC));
                ps.setObject(2, eventId);
                return ps.executeUpdate() == 1;
            } catch (SQLException e) {
                throw AccountStoreException.wrap("outbox mark-published failed", e);
            }
        });
    }

    /**
     * Record a relay failure: attempts + 1 and the error detail, landing the
     * record in FAILED (the DLQ) at the attempts ceiling. Returns the updated
     * record, or empty when a competing relay already settled it.
     *
     * @param record the record whose publication failed
     * @param error the failure detail (truncated to the column's cap)
     * @return the updated record, or empty when it was no longer PENDING
     */
    public Optional<AccountEventRecord> markFailed(AccountEventRecord record, String error) {
        return database.inTransaction(c -> {
            String select = "SELECT " + COLUMNS + " FROM account_events_outbox"
                    + " WHERE event_id = ? FOR UPDATE";
            try (PreparedStatement ps = c.prepareStatement(select)) {
                ps.setObject(1, record.eventId);
                AccountEventRecord managed;
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()
                            || !AccountEventRecord.STATUS_PENDING.equals(rs.getString("status"))) {
                        return Optional.empty();
                    }
                    managed = map(rs);
                }
                managed.attempts = managed.attempts + 1;
                managed.lastError = truncate(error);
                if (managed.attempts >= AccountEventRecord.MAX_ATTEMPTS) {
                    managed.status = AccountEventRecord.STATUS_FAILED;
                }
                try (PreparedStatement update = c.prepareStatement(
                        "UPDATE account_events_outbox SET attempts = ?, status = ?, last_error = ?"
                                + " WHERE event_id = ?")) {
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
                throw AccountStoreException.wrap("outbox mark-failed failed", e);
            }
        });
    }

    /**
     * Look up a record by primary key (test/introspection path).
     *
     * @param eventId the record's id
     * @return the record, or empty
     */
    public Optional<AccountEventRecord> findById(UUID eventId) {
        return database.readOnly(c -> {
            String sql = "SELECT " + COLUMNS + " FROM account_events_outbox WHERE event_id = ?";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setObject(1, eventId);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? Optional.of(map(rs)) : Optional.empty();
                }
            } catch (SQLException e) {
                throw AccountStoreException.wrap("outbox find failed", e);
            }
        });
    }

    /**
     * Row counts per status (test/introspection path).
     *
     * @return status to row count
     */
    public Map<String, Long> countByStatus() {
        return database.readOnly(c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT status, COUNT(*) FROM account_events_outbox GROUP BY status");
                    ResultSet rs = ps.executeQuery()) {
                Map<String, Long> counts = new HashMap<>();
                while (rs.next()) {
                    counts.put(rs.getString(1), rs.getLong(2));
                }
                return counts;
            } catch (SQLException e) {
                throw AccountStoreException.wrap("outbox count failed", e);
            }
        });
    }

    private static AccountEventRecord map(ResultSet rs) throws SQLException {
        AccountEventRecord record = new AccountEventRecord();
        record.eventId = rs.getObject("event_id", UUID.class);
        record.eventType = rs.getString("event_type");
        record.payload = rs.getBytes("payload");
        record.kafkaKey = rs.getString("kafka_key");
        record.attempts = rs.getInt("attempts");
        record.status = rs.getString("status");
        record.createdAt = rs.getObject("created_at", OffsetDateTime.class).toInstant();
        OffsetDateTime published = rs.getObject("published_at", OffsetDateTime.class);
        record.publishedAt = published == null ? null : published.toInstant();
        record.lastError = rs.getString("last_error");
        return record;
    }

    private static String truncate(String error) {
        if (error == null) {
            return null;
        }
        return error.length() <= MAX_ERROR_LENGTH ? error : error.substring(0, MAX_ERROR_LENGTH);
    }
}
