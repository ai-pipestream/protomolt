package ai.protomolt.proto.mesh.runtime;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

/** Bounded relay from committed PostgreSQL outbox rows to an idempotent broker producer. */
public final class TransactionalOutboxRelay {

    private final JdbcTransactionalProcessorChannel channel;
    private final BrokerPublisher publisher;
    private final String relayOwner;
    private final int batchSize;
    private final Duration leaseDuration;
    private final Clock clock;

    public TransactionalOutboxRelay(
            JdbcTransactionalProcessorChannel channel,
            BrokerPublisher publisher,
            String relayOwner,
            int batchSize,
            Duration leaseDuration,
            Clock clock) {
        this.channel = Objects.requireNonNull(channel, "channel");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        if (relayOwner == null || relayOwner.isBlank() || relayOwner.length() > 256) {
            throw new IllegalArgumentException(
                    "relayOwner must contain 1 to 256 characters");
        }
        if (batchSize < 1 || batchSize > 10_000) {
            throw new IllegalArgumentException("batchSize must be between 1 and 10000");
        }
        if (leaseDuration == null || leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException("leaseDuration must be positive");
        }
        this.relayOwner = relayOwner;
        this.batchSize = batchSize;
        this.leaseDuration = leaseDuration;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** Publishes at most one configured batch, retaining stable event ids across retries. */
    public BatchResult runOnce() {
        var claimed = channel.claimOutbox(
                relayOwner, batchSize, leaseDuration, clock.instant());
        int published = 0;
        int failed = 0;
        for (var entry : claimed) {
            try {
                publisher.publish(entry.eventId(), entry.sequence(), entry.protobufRecord());
                channel.markOutboxPublished(entry.eventId(), relayOwner);
                published++;
            } catch (Exception failure) {
                channel.releaseOutbox(entry.eventId(), relayOwner, describe(failure));
                failed++;
            }
        }
        return new BatchResult(claimed.size(), published, failed);
    }

    private static String describe(Exception failure) {
        String message = failure.getMessage();
        return failure.getClass().getSimpleName()
                + (message == null || message.isBlank() ? "" : ": " + message);
    }

    /** Broker writes must use {@code eventId} as their idempotency key. */
    @FunctionalInterface
    public interface BrokerPublisher {
        void publish(UUID eventId, long sequence, byte[] protobufRecord) throws Exception;
    }

    public record BatchResult(int claimed, int published, int failed) {
    }
}
