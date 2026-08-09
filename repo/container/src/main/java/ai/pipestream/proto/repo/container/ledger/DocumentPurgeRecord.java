package ai.pipestream.proto.repo.container.ledger;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * One purge request of the two-phase delete (V3, {@code document_purges}).
 * <p>
 * Phase A (the synchronous delete call) tombstones the {@link DocumentRecord}
 * row to {@link DocumentStatus#PENDING_PURGE} and inserts one record here IN
 * THE SAME TRANSACTION, snapshotting every object key to delete into
 * {@link #objectKeys} — Phase B never recomputes keys, so a body re-staged
 * after the request can only be protected by the staleness guard, never
 * silently deleted under new keys. Phase B (the background purger) claims
 * PENDING records, re-reads the document row under a row lock, applies the
 * staleness guard ({@code updated_at} strictly after {@link #requestedAt} →
 * the purge is VOID), batch-deletes the snapshot keys and removes the row.
 * <p>
 * Status machine: {@code PENDING} → {@code PURGED} (done), {@code VOID}
 * (cancelled by the staleness guard — objects and row left alone) or
 * {@code FAILED} ({@link #MAX_ATTEMPTS} attempts exhausted; the FAILED record
 * IS the dead-letter queue for now — the sweeper deliberately never
 * re-enqueues it; recovery is operator territory).
 * <p>
 * The primary key is a surrogate ({@link #purgeId}), NOT the deterministic
 * {@code node_id}: rebirth (re-ingest of the same document) regenerates the
 * SAME node id, so identity is not unique over time and every purge request
 * needs its own row.
 */
@Entity
@Table(name = "document_purges", check = @jakarta.persistence.CheckConstraint(
        name = "chk_document_purges_status",
        constraint = "status IN ('PENDING', 'PURGED', 'FAILED', 'VOID')"))
public class DocumentPurgeRecord {

    /** Queued, awaiting (or between) drain attempts. */
    public static final String STATUS_PENDING = "PENDING";
    /** Objects deleted and document row removed. Terminal. */
    public static final String STATUS_PURGED = "PURGED";
    /** Drain failed {@link #MAX_ATTEMPTS} times. Terminal — the DLQ. */
    public static final String STATUS_FAILED = "FAILED";
    /** Cancelled by the staleness guard (the row was re-staged). Terminal. */
    public static final String STATUS_VOID = "VOID";

    /** Attempts ceiling: a drain failure at or past it lands the record in FAILED. */
    public static final int MAX_ATTEMPTS = 10;

    /** Default constructor required by the JPA/Hibernate persistence provider. */
    public DocumentPurgeRecord() {
    }

    /**
     * Surrogate record id, minted by the app per purge request. Surrogate
     * rather than the deterministic node id because rebirth makes identity
     * non-unique over time (see the class Javadoc).
     */
    @Id
    @Column(name = "purge_id", nullable = false)
    public UUID purgeId;

    /** The deterministic id of the documents row to purge. NOT unique. */
    @Column(name = "node_id", nullable = false)
    public UUID nodeId;

    /** Document identifier of the row being purged. */
    @Column(name = "doc_id", nullable = false)
    public String docId;

    /** Graph address id of the row being purged. */
    @Column(name = "graph_address_id", nullable = false)
    public String graphAddressId;

    /** Identifier of the account that owns the row being purged. */
    @Column(name = "account_id", nullable = false)
    public String accountId;

    /** Graph segment of the row's storage identity. */
    @Column(name = "graph_id", nullable = false)
    public String graphId;

    /** Name of the drive whose bucket holds the objects. */
    @Column(name = "drive_name", nullable = false)
    public String driveName;

    /**
     * The snapshot of every object key to delete, as a JSON array
     * ({@code jsonb}) captured at tombstone time. Use
     * {@link #readObjectKeys()}/{@link #writeObjectKeys(List)} rather than
     * handling the raw JSON.
     */
    @org.hibernate.annotations.JdbcTypeCode(org.hibernate.type.SqlTypes.JSON)
    @Column(name = "object_keys", nullable = false)
    public String objectKeys;

    /**
     * When the delete was requested. The staleness guard voids the purge when
     * the document row's {@code updated_at} is strictly after this instant
     * (someone re-staged the body after the delete was requested).
     */
    @Column(name = "requested_at", nullable = false)
    public Instant requestedAt;

    /** Drain attempts so far; at {@link #MAX_ATTEMPTS} the record goes FAILED. */
    @Column(name = "attempts", nullable = false)
    public int attempts;

    /** Queue state: PENDING (default), PURGED, FAILED or VOID. */
    @Column(name = "status", nullable = false)
    public String status = STATUS_PENDING;

    /** The last drain failure's detail (truncated), for the DLQ record. */
    @Column(name = "last_error")
    public String lastError;

    /**
     * When the Kafka-backed purge queue relayed this record to the purge
     * topic; null until relayed, and null forever on the JDBC queue (V5).
     * The relay republishes any PENDING row whose relayed_at is still null,
     * so a crash between the broker ack and this stamp causes a duplicate on
     * the topic - tolerated, because settling is conditional on PENDING.
     */
    @Column(name = "relayed_at")
    public Instant relayedAt;

    /**
     * Deserialize {@link #objectKeys} (a JSON array of strings) into the
     * snapshot key list.
     *
     * @return the object keys to delete
     */
    public List<String> readObjectKeys() {
        if (objectKeys == null || objectKeys.isBlank()) {
            return List.of();
        }
        try {
            ListValue.Builder builder = ListValue.newBuilder();
            JsonFormat.parser().merge(objectKeys, builder);
            return builder.build().getValuesList().stream()
                    .map(Value::getStringValue)
                    .toList();
        } catch (InvalidProtocolBufferException e) {
            throw new LedgerException("unparseable object_keys JSON on purge record " + purgeId, e);
        }
    }

    /**
     * Serialize the snapshot key list into {@link #objectKeys} as a JSON
     * array of strings.
     *
     * @param keys every object key Phase B must delete
     */
    public void writeObjectKeys(List<String> keys) {
        ListValue.Builder builder = ListValue.newBuilder();
        for (String key : keys) {
            builder.addValues(Value.newBuilder().setStringValue(key));
        }
        try {
            this.objectKeys = JsonFormat.printer().omittingInsignificantWhitespace().print(builder);
        } catch (InvalidProtocolBufferException e) {
            throw new LedgerException("unprintable object key list", e);
        }
    }
}
