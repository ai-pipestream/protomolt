package ai.pipestream.proto.mesh.cluster;

import ai.pipestream.proto.mesh.cluster.v1.ClusterDescriptor;
import ai.pipestream.proto.mesh.cluster.v1.ClusterEvent;
import ai.pipestream.proto.mesh.cluster.v1.ClusterSnapshot;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PersistentClusterDirectoryTest {

    @Test
    void newFacadeRestoresDirectoryFromSavedEventLog() {
        InMemoryClusterEventRepository events = new InMemoryClusterEventRepository();
        PersistentClusterDirectory first = new PersistentClusterDirectory(
                ClusterFixtures.cluster(), new ClusterFixtures.MutableClock(ClusterFixtures.T0),
                events);
        first.register(ClusterFixtures.node("node-1"));
        first.registerProcessor(ClusterFixtures.processorBuilder("proc-1", "node-1").build());

        PersistentClusterDirectory restored = new PersistentClusterDirectory(
                ClusterFixtures.cluster(), new ClusterFixtures.MutableClock(ClusterFixtures.T0),
                events);

        assertThat(restored.node("node-1")).isPresent();
        assertThat(restored.processor("proc-1")).isPresent();
        assertThat(restored.eventLog()).isEqualTo(first.eventLog());
        assertThat(restored.snapshot()).isEqualTo(first.snapshot());
    }

    @Test
    void failedSaveDoesNotExposeMemoryOnlyMembership() {
        FailingRepository events = new FailingRepository();
        PersistentClusterDirectory directory = new PersistentClusterDirectory(
                ClusterFixtures.cluster(), new ClusterFixtures.MutableClock(ClusterFixtures.T0),
                events);
        events.fail = true;

        assertThatThrownBy(() -> directory.register(ClusterFixtures.node("node-1")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("repository unavailable");
        assertThat(directory.node("node-1")).isEmpty();
        assertThat(directory.eventLog()).isEmpty();

        events.fail = false;
        directory.register(ClusterFixtures.node("node-1"));
        assertThat(directory.eventLog()).singleElement()
                .satisfies(event -> assertThat(event.getSeq()).isEqualTo(1));
    }

    @Test
    void unchangedRefreshDoesNotWriteRepository() {
        CountingRepository events = new CountingRepository();
        PersistentClusterDirectory directory = new PersistentClusterDirectory(
                ClusterFixtures.cluster(), new ClusterFixtures.MutableClock(ClusterFixtures.T0),
                events);
        directory.register(ClusterFixtures.node("node-1"));

        directory.register(ClusterFixtures.node("node-1"));

        assertThat(events.saves).isEqualTo(1);
    }

    @Test
    void readsDoNotWaitForAnInFlightDurableWrite() throws Exception {
        BlockingRepository events = new BlockingRepository();
        PersistentClusterDirectory directory = new PersistentClusterDirectory(
                ClusterFixtures.cluster(), new ClusterFixtures.MutableClock(ClusterFixtures.T0),
                events);

        Thread writer = new Thread(() -> directory.register(ClusterFixtures.node("node-1")),
                "blocked-directory-write");
        writer.start();
        assertThat(events.entered.await(5, TimeUnit.SECONDS))
                .as("the repository write should have started").isTrue();

        // The reader runs on its own thread so that a directory which serializes reads
        // behind writes fails this test on the timeout instead of hanging the suite.
        ExecutorService reader = Executors.newSingleThreadExecutor();
        try {
            ClusterSnapshot duringWrite =
                    reader.submit(directory::snapshot).get(5, TimeUnit.SECONDS);
            assertThat(duringWrite.getNodesList()).isEmpty();
            assertThat(duringWrite.getSnapshotSeq()).isZero();
        } finally {
            reader.shutdownNow();
            events.release.countDown();
            writer.join(TimeUnit.SECONDS.toMillis(5));
        }

        assertThat(directory.node("node-1")).isPresent();
        assertThat(directory.snapshot().getSnapshotSeq()).isEqualTo(1);
    }

    private static class CountingRepository implements ClusterEventRepository {
        private List<ClusterEvent> current = List.of();
        private int saves;

        @Override
        public Optional<List<ClusterEvent>> load(ClusterDescriptor cluster) {
            return current.isEmpty() ? Optional.empty() : Optional.of(current);
        }

        @Override
        public void save(ClusterDescriptor cluster, List<ClusterEvent> events) {
            saves++;
            current = List.copyOf(events);
        }
    }

    /** Holds one save open so a reader can be observed while a commit is in flight. */
    private static final class BlockingRepository extends CountingRepository {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        @Override
        public void save(ClusterDescriptor cluster, List<ClusterEvent> events) {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while persisting", e);
            }
            super.save(cluster, events);
        }
    }

    private static final class FailingRepository extends CountingRepository {
        private boolean fail;

        @Override
        public void save(ClusterDescriptor cluster, List<ClusterEvent> events) {
            if (fail) {
                throw new IllegalStateException("repository unavailable");
            }
            super.save(cluster, events);
        }
    }
}
