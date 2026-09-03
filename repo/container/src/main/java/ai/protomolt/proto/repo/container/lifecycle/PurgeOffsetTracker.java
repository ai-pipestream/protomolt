package ai.protomolt.proto.repo.container.lifecycle;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * Per-partition settled-offset contiguity for {@link KafkaPurgeQueue}.
 * <p>
 * A consumer group's committed position is one offset per partition - the
 * next record to read - so an in-flight batch can only be partially settled:
 * settling offset 7 while 5 is still open must not commit past 5. This
 * tracker folds {@link #settle} calls into the highest CONTIGUOUS settled
 * offset per partition and reports only the advances since the last commit.
 * Settlements are idempotent (a double settle folds into the high water and
 * advances nothing), and settling an offset this instance never tracked is a
 * no-op: after a restart the group position replays the record, and the new
 * instance tracks it fresh.
 * <p>
 * Not thread-safe: owned by the single purger loop, like the consumer.
 */
final class PurgeOffsetTracker {

    /** One partition's fold state. */
    private static final class PartitionState {
        /** The first offset ever tracked (the fold floor; nothing below it is ours). */
        long firstOffset = -1;
        /** The lowest offset not yet folded into the high water. */
        long nextRequired = -1;
        /** The position last reported as committed, so only advances are recommitted. */
        long committed = -1;
        /** Settled offsets at or past {@link #nextRequired}, ascending. */
        final TreeSet<Long> settled = new TreeSet<>();
    }

    private final Map<TopicPartition, PartitionState> partitions = new HashMap<>();

    /**
     * Register an in-flight record. The first tracked offset of a partition
     * becomes the fold floor: records arrive from the poll in ascending
     * offset order per partition, so it is the lowest offset this instance
     * can owe.
     *
     * @param partition the record's partition
     * @param offset the record's offset
     */
    void track(TopicPartition partition, long offset) {
        PartitionState state = partitions.computeIfAbsent(partition, k -> new PartitionState());
        if (state.nextRequired < 0) {
            state.firstOffset = offset;
            state.nextRequired = offset;
        }
    }

    /**
     * Fold one settled offset: advance the partition's high water over every
     * contiguous settled offset. A settle below the high water (a duplicate
     * delivery settled twice) is discarded; a settle for a partition this
     * instance never tracked is ignored entirely.
     *
     * @param partition the settled record's partition
     * @param offset the settled record's offset
     */
    void settle(TopicPartition partition, long offset) {
        PartitionState state = partitions.get(partition);
        if (state == null) {
            return;
        }
        state.settled.add(offset);
        // Anything below the high water is already folded in (double settle).
        while (!state.settled.isEmpty() && state.settled.first() < state.nextRequired) {
            state.settled.pollFirst();
        }
        while (state.settled.remove(state.nextRequired)) {
            state.nextRequired++;
        }
    }

    /**
     * The commit positions that advanced since the last {@link #committed}:
     * per partition, the highest contiguous settled offset + 1 (Kafka commits
     * the next-to-read position). A partition whose high water never moved
     * past its first tracked offset reports nothing - nothing of it is done.
     *
     * @return partition to next-to-read position, possibly empty
     */
    Map<TopicPartition, OffsetAndMetadata> committable() {
        Map<TopicPartition, OffsetAndMetadata> out = new HashMap<>();
        for (Map.Entry<TopicPartition, PartitionState> entry : partitions.entrySet()) {
            PartitionState state = entry.getValue();
            if (state.nextRequired > state.firstOffset && state.nextRequired > state.committed) {
                out.put(entry.getKey(), new OffsetAndMetadata(state.nextRequired));
            }
        }
        return out;
    }

    /**
     * Record a successful commit so {@link #committable()} reports only newer
     * advances from here.
     *
     * @param committed the positions just committed
     */
    void committed(Map<TopicPartition, OffsetAndMetadata> committed) {
        committed.forEach((partition, metadata) -> {
            PartitionState state = partitions.get(partition);
            if (state != null) {
                state.committed = metadata.offset();
            }
        });
    }
}
