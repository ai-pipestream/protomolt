package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorRegistry;

import javax.sql.DataSource;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** PostgreSQL-backed channel with atomic protobuf state and relay outbox writes. */
public final class JdbcTransactionalProcessorChannel extends FileDurableProcessorChannel {

    private final JdbcProcessorChannelJournal jdbc;

    public JdbcTransactionalProcessorChannel(
            DataSource dataSource,
            String channelId,
            DescriptorRegistry descriptors,
            Clock clock) {
        this(new JdbcProcessorChannelJournal(dataSource, channelId), descriptors, clock);
    }

    private JdbcTransactionalProcessorChannel(
            JdbcProcessorChannelJournal journal,
            DescriptorRegistry descriptors,
            Clock clock) {
        super(journal, descriptors, clock);
        this.jdbc = journal;
    }

    public List<OutboxDelivery> claimOutbox(
            String relayOwner, int limit, Duration leaseDuration, Instant now) {
        return jdbc.claimOutbox(relayOwner, limit, leaseDuration, now).stream()
                .map(entry -> new OutboxDelivery(entry.eventId(), entry.sequence(),
                        entry.recordBody(), entry.attempts()))
                .toList();
    }

    public void markOutboxPublished(UUID eventId, String relayOwner) {
        jdbc.settleOutbox(eventId, relayOwner);
    }

    public void releaseOutbox(UUID eventId, String relayOwner, String error) {
        jdbc.failOutbox(eventId, relayOwner, error);
    }

    public record OutboxDelivery(
            UUID eventId, long sequence, byte[] protobufRecord, int attempts) {
        public OutboxDelivery {
            protobufRecord = protobufRecord.clone();
        }

        @Override
        public byte[] protobufRecord() {
            return protobufRecord.clone();
        }
    }
}
