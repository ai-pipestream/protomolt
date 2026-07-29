package ai.pipestream.proto.account.service.store;

import ai.pipestream.proto.account.v1.Account;
import ai.pipestream.proto.account.v1.AccountStatus;
import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.util.Map;

/**
 * One account row, detached: the store's in-memory shape. Public fields, no
 * behavior beyond the wire conversion — same convention as the repo module's
 * ledger records.
 * <p>
 * {@code status} rides the proto enum (the store module already owns the
 * contract); the database column carries the same name minus the
 * {@code ACCOUNT_STATUS_} prefix.
 */
public final class AccountRecord {

    /** The tenancy key (primary key; caller-minted, never the service). */
    public String accountId;
    /** Human-facing label. */
    public String displayName;
    /** Lifecycle state. */
    public AccountStatus status;
    /** Extensible key-value metadata; never null (empty when unset). */
    public Map<String, String> metadata = Map.of();
    /** When the row was created. */
    public Instant createdAt;
    /** When the row last changed. */
    public Instant updatedAt;

    /**
     * The wire shape of this row.
     *
     * @return the Account protobuf
     */
    public Account toProto() {
        Account.Builder builder = Account.newBuilder()
                .setAccountId(accountId)
                .setDisplayName(displayName == null ? "" : displayName)
                .setStatus(status)
                .putAllMetadata(metadata);
        if (createdAt != null) {
            builder.setCreatedAt(timestamp(createdAt));
        }
        if (updatedAt != null) {
            builder.setUpdatedAt(timestamp(updatedAt));
        }
        return builder.build();
    }

    private static Timestamp timestamp(Instant when) {
        return Timestamp.newBuilder().setSeconds(when.getEpochSecond()).setNanos(when.getNano())
                .build();
    }
}
