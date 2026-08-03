package ai.pipestream.proto.jobs.service.store;

import java.time.Instant;
import java.util.UUID;

/**
 * One {@code chain_job_events_outbox} row, detached. Public fields, no
 * behavior — same convention as the account module's outbox record.
 * <p>
 * {@code eventId} is a random UUID minted per event and is echoed into the
 * {@code ChainJobEvent} protobuf envelope as the consumer dedupe key under
 * at-least-once delivery.
 */
public final class ChainJobEventRecord {

    /** Outbox status: awaiting/between relay attempts. */
    public static final String STATUS_PENDING = "PENDING";
    /** Outbox status: acked by the broker. */
    public static final String STATUS_PUBLISHED = "PUBLISHED";
    /** Outbox status: attempts exhausted — the DLQ. */
    public static final String STATUS_FAILED = "FAILED";

    /** Event type: the job was accepted and queued. */
    public static final String TYPE_ACCEPTED = "ACCEPTED";
    /** Event type: a step's response was checkpointed. */
    public static final String TYPE_STEP_CHECKPOINT = "STEP_CHECKPOINT";
    /** Event type: the job parked on an external-completion step. */
    public static final String TYPE_WAITING = "WAITING";
    /** Event type: the chain ran to completion. */
    public static final String TYPE_COMPLETED = "COMPLETED";
    /** Event type: terminal failure (a verdict or a non-retryable error). */
    public static final String TYPE_FAILED = "FAILED";
    /** Event type: retries exhausted — dead-lettered. */
    public static final String TYPE_DEAD = "DEAD";

    /** Relay attempts ceiling: at this many, the row lands FAILED (the DLQ). */
    public static final int MAX_ATTEMPTS = 10;

    /** Surrogate event id; the consumer dedupe key. */
    public UUID eventId;
    /** The ChainJobEvent.Type name (sans the proto TYPE_ prefix). */
    public String eventType;
    /** The serialized ChainJobEvent protobuf. */
    public byte[] payload;
    /** The Kafka record key: the job id. */
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
