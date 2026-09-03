package ai.protomolt.proto.repo.container.lifecycle;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The settled-offset contiguity fold: in order, out of order, gaps that fill
 * later, double settles, and offsets this instance never tracked.
 */
class PurgeOffsetTrackerTest {

    private static final TopicPartition A = new TopicPartition("purges", 0);
    private static final TopicPartition B = new TopicPartition("purges", 1);

    @Test
    void inOrderSettlesAdvanceTheHighWater() {
        PurgeOffsetTracker tracker = new PurgeOffsetTracker();
        tracker.track(A, 0);
        tracker.track(A, 1);
        tracker.track(A, 2);
        assertThat(tracker.committable()).isEmpty();

        tracker.settle(A, 0);
        assertThat(tracker.committable()).containsExactlyEntriesOf(Map.of(A, new OffsetAndMetadata(1)));
        tracker.committed(tracker.committable());

        tracker.settle(A, 1);
        tracker.settle(A, 2);
        assertThat(tracker.committable()).containsExactlyEntriesOf(Map.of(A, new OffsetAndMetadata(3)));
    }

    @Test
    void outOfOrderSettleParksUntilTheGapFills() {
        PurgeOffsetTracker tracker = new PurgeOffsetTracker();
        tracker.track(A, 0);
        tracker.track(A, 1);
        tracker.track(A, 2);

        tracker.settle(A, 0);
        tracker.settle(A, 2);
        // Offset 1 is still open: the high water stops at it.
        assertThat(tracker.committable()).containsExactlyEntriesOf(Map.of(A, new OffsetAndMetadata(1)));
        tracker.committed(tracker.committable());
        assertThat(tracker.committable()).isEmpty();

        // The gap fills: 1 and the parked 2 fold in at once.
        tracker.settle(A, 1);
        assertThat(tracker.committable()).containsExactlyEntriesOf(Map.of(A, new OffsetAndMetadata(3)));
    }

    @Test
    void doubleSettleAdvancesNothing() {
        PurgeOffsetTracker tracker = new PurgeOffsetTracker();
        tracker.track(A, 0);
        tracker.settle(A, 0);
        tracker.committed(tracker.committable());

        tracker.settle(A, 0);
        assertThat(tracker.committable()).isEmpty();
    }

    @Test
    void settlingAnUntrackedPartitionIsANoOp() {
        PurgeOffsetTracker tracker = new PurgeOffsetTracker();
        tracker.settle(A, 7);
        assertThat(tracker.committable()).isEmpty();
    }

    @Test
    void partitionsFoldIndependently() {
        PurgeOffsetTracker tracker = new PurgeOffsetTracker();
        tracker.track(A, 0);
        tracker.track(A, 1);
        tracker.track(B, 4);
        tracker.track(B, 5);

        tracker.settle(B, 4);
        tracker.settle(B, 5);
        assertThat(tracker.committable()).containsExactlyEntriesOf(Map.of(B, new OffsetAndMetadata(6)));

        tracker.settle(A, 1);
        // A's first offset is still open: B's progress cannot leak across.
        assertThat(tracker.committable()).containsExactlyEntriesOf(Map.of(B, new OffsetAndMetadata(6)));
    }

    @Test
    void nothingSettledBeyondTheFirstTrackedOffsetReportsNothing() {
        PurgeOffsetTracker tracker = new PurgeOffsetTracker();
        // A consumer joining a compacted topic can start above zero; position
        // 5 is where reading starts, not work this instance finished.
        tracker.track(A, 5);
        assertThat(tracker.committable()).isEmpty();

        tracker.settle(A, 5);
        assertThat(tracker.committable()).containsExactlyEntriesOf(Map.of(A, new OffsetAndMetadata(6)));
    }
}
