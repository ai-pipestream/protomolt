package ai.pipestream.proto.account.service.store;

import ai.pipestream.proto.account.v1.AccountStatus;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The Postgres default {@link AccountStore}: stateless SQL over the caller's
 * connection (see the SPI's transaction contract). All SQL is
 * prepared-statement parameterized; the metadata map rides the
 * {@code accounts.metadata} JSONB column as a JSON object string.
 */
public final class JdbcAccountStore implements AccountStore {

    /** Postgres unique-violation SQLState (duplicate account_id). */
    private static final String UNIQUE_VIOLATION = "23505";

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, String>> METADATA_TYPE = new TypeReference<>() {
    };

    @Override
    public AccountRecord create(Connection c, AccountRecord record) {
        // RETURNING reads the DB-stamped timestamps back, so the response and
        // every later read agree on created_at/updated_at.
        String sql = "INSERT INTO accounts (account_id, display_name, status, metadata)"
                + " VALUES (?, ?, ?, ?::jsonb)"
                + " RETURNING created_at, updated_at";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, record.accountId);
            ps.setString(2, record.displayName == null ? "" : record.displayName);
            ps.setString(3, statusToDb(record.status));
            setMetadata(ps, 4, record.metadata);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                record.createdAt = rs.getObject(1, OffsetDateTime.class).toInstant();
                record.updatedAt = rs.getObject(2, OffsetDateTime.class).toInstant();
            }
            return record;
        } catch (SQLException e) {
            if (UNIQUE_VIOLATION.equals(e.getSQLState())) {
                throw AccountStoreException.conflict(record.accountId);
            }
            throw AccountStoreException.wrap("create account failed", e);
        }
    }

    @Override
    public Optional<AccountRecord> findById(Connection c, String accountId) {
        return find(c, accountId, false);
    }

    @Override
    public Optional<AccountRecord> findByIdForUpdate(Connection c, String accountId) {
        return find(c, accountId, true);
    }

    private Optional<AccountRecord> find(Connection c, String accountId, boolean forUpdate) {
        String sql = "SELECT account_id, display_name, status, metadata, created_at, updated_at"
                + " FROM accounts WHERE account_id = ?" + (forUpdate ? " FOR UPDATE" : "");
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, accountId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(map(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw AccountStoreException.wrap("find account failed", e);
        }
    }

    @Override
    public ListAccountsResult list(Connection c, AccountStatus statusFilter, int limit,
            long offset) {
        String where = statusFilter == null || statusFilter == AccountStatus.ACCOUNT_STATUS_UNSPECIFIED
                ? "" : " WHERE status = ?";
        try {
            long total;
            try (PreparedStatement count = c.prepareStatement(
                    "SELECT COUNT(*) FROM accounts" + where)) {
                if (!where.isEmpty()) {
                    count.setString(1, statusToDb(statusFilter));
                }
                try (ResultSet rs = count.executeQuery()) {
                    rs.next();
                    total = rs.getLong(1);
                }
            }
            List<AccountRecord> rows = new ArrayList<>();
            try (PreparedStatement page = c.prepareStatement(
                    "SELECT account_id, display_name, status, metadata, created_at, updated_at"
                            + " FROM accounts" + where
                            // Stable order: the offset continuation token is
                            // only meaningful against a deterministic scan.
                            + " ORDER BY created_at ASC, account_id ASC LIMIT ? OFFSET ?")) {
                int p = 1;
                if (!where.isEmpty()) {
                    page.setString(p++, statusToDb(statusFilter));
                }
                page.setInt(p++, limit);
                page.setLong(p, offset);
                try (ResultSet rs = page.executeQuery()) {
                    while (rs.next()) {
                        rows.add(map(rs));
                    }
                }
            }
            return new ListAccountsResult(rows, total);
        } catch (SQLException e) {
            throw AccountStoreException.wrap("list accounts failed", e);
        }
    }

    @Override
    public AccountRecord updateStatus(Connection c, String accountId, AccountStatus to,
            Instant when) {
        String sql = "UPDATE accounts SET status = ?, updated_at = ? WHERE account_id = ?"
                + " RETURNING account_id, display_name, status, metadata, created_at, updated_at";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, statusToDb(to));
            ps.setObject(2, when.atOffset(ZoneOffset.UTC));
            ps.setString(3, accountId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    // The caller holds the row lock, so reaching here means
                    // the row vanished mid-transaction — not found either way.
                    throw AccountStoreException.notFound(accountId);
                }
                return map(rs);
            }
        } catch (SQLException e) {
            throw AccountStoreException.wrap("update account status failed", e);
        }
    }

    /** The database spelling of a status: the enum name minus the prefix. */
    static String statusToDb(AccountStatus status) {
        return status.name().substring("ACCOUNT_STATUS_".length());
    }

    /** The enum value for a database spelling. */
    static AccountStatus statusFromDb(String status) {
        try {
            return AccountStatus.valueOf("ACCOUNT_STATUS_" + status);
        } catch (IllegalArgumentException e) {
            // A value the enum doesn't know is a server-side data problem
            // (the CHECK constraint should have kept it out), never a client
            // error: an unclassified store failure → INTERNAL on the wire,
            // not INVALID_ARGUMENT.
            throw AccountStoreException.wrap("unknown account status in store: " + status, e);
        }
    }

    private static AccountRecord map(ResultSet rs) throws SQLException {
        AccountRecord record = new AccountRecord();
        record.accountId = rs.getString("account_id");
        record.displayName = rs.getString("display_name");
        record.status = statusFromDb(rs.getString("status"));
        record.metadata = readMetadata(rs.getString("metadata"));
        record.createdAt = rs.getObject("created_at", OffsetDateTime.class).toInstant();
        record.updatedAt = rs.getObject("updated_at", OffsetDateTime.class).toInstant();
        return record;
    }

    private static void setMetadata(PreparedStatement ps, int index, Map<String, String> metadata)
            throws SQLException {
        if (metadata == null || metadata.isEmpty()) {
            ps.setNull(index, Types.OTHER);
            return;
        }
        try {
            ps.setObject(index, JSON.writeValueAsString(metadata), Types.OTHER);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw AccountStoreException.wrap("metadata is not serializable", e);
        }
    }

    private static Map<String, String> readMetadata(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        try {
            return JSON.readValue(json, METADATA_TYPE);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw AccountStoreException.wrap("stored metadata is not readable", e);
        }
    }
}
