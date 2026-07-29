package ai.pipestream.proto.account.service.events;

import java.time.Instant;
import java.util.UUID;

/**
 * One {@code account_events_outbox} row, detached. Public fields, no
 * behavior — same convention as the repo module's outbox record.
 * <p>
 * {@code eventId} is a random UUID minted per event (events have no natural
 * id; the account's identity determinism tenet applies to accounts, not to
 * their event log) and is echoed into the protobuf envelope as the consumer
 * dedupe key under at-least-once delivery.
 */
public final class AccountEventRecord {

    /** Outbox status: awaiting/between relay attempts. */
    public static final String STATUS_PENDING = "PENDING";
    /** Outbox status: acked by the broker. */
    public static final String STATUS_PUBLISHED = "PUBLISHED";
    /** Outbox status: attempts exhausted — the DLQ. */
    public static final String STATUS_FAILED = "FAILED";

    /** Event type: the AccountEvent.created arm. */
    public static final String TYPE_CREATED = "AccountCreated";
    /** Event type: the AccountEvent.activated arm. */
    public static final String TYPE_ACTIVATED = "AccountActivated";
    /** Event type: the AccountEvent.deactivated arm. */
    public static final String TYPE_DEACTIVATED = "AccountDeactivated";

    /** Relay attempts ceiling: at this many, the row lands FAILED (the DLQ). */
    public static final int MAX_ATTEMPTS = 10;

    /** Surrogate event id; the consumer dedupe key. */
    public UUID eventId;
    /** The AccountEvent oneof arm name. */
    public String eventType;
    /** The serialized AccountEvent protobuf. */
    public byte[] payload;
    /** The Kafka record key: the account id. */
    public String kafkaKey;
    /** Relay attempts so far. */
    public int attempts;
    /** PENDING / PUBLISHED / FAILED. */
    public String status = STATUS_PENDING;
    /** When the event's transaction committed (the relay's drain order). */
    public Instant createdAt;
    /** When the broker acked the record; null until PUBLISHED. */
    public Instant publishedAt;
    /** The last relay failure's detail (truncated), for the DLQ record. */
    public String lastError;
}
