package ai.protomolt.proto.mesh.cluster;

import ai.protomolt.proto.mesh.cluster.ClusterEventRepository.StoredDirectory;
import ai.protomolt.proto.mesh.cluster.v1.ClusterDescriptor;
import ai.protomolt.proto.mesh.cluster.v1.ClusterEvent;
import ai.protomolt.proto.mesh.cluster.v1.ClusterSnapshot;
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
        assertThat(directory.persistenceFailure()).hasValueSatisfying(failure ->
                assertThat(failure).hasMessageContaining("repository unavailable"));

        events.fail = false;
        directory.register(ClusterFixtures.node("node-1"));
        assertThat(directory.persistenceFailure()).isEmpty();
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

    @Test
    void durableChangesFoldTheLogInsteadOfGrowingItWithoutBound() {
        CountingRepository events = new CountingRepository();
        ClusterFixtures.MutableClock clock = new ClusterFixtures.MutableClock(ClusterFixtures.T0);
        PersistentClusterDirectory directory = new PersistentClusterDirectory(
                ClusterFixtures.cluster(), clock, events, 4);
        directory.register(ClusterFixtures.node("node-1"));

        for (int update = 1; update <= 40; update++) {
            directory.updateCapacity(ClusterFixtures.capacityBuilder("node-1", update)
                    .setInFlight(update % 16)
                    .build());
            assertThat(directory.eventLog())
                    .as("retained events after capacity update %d", update)
                    .hasSizeLessThanOrEqualTo(4);
        }

        assertThat(directory.checkpoint()).isPresent();
        assertThat(directory.snapshot().getSnapshotSeq()).isEqualTo(41);

        // A directory restored from what was stored is the directory that stored it: the
        // fold is only sound if the events it dropped left nothing behind.
        PersistentClusterDirectory restored = new PersistentClusterDirectory(
                ClusterFixtures.cluster(), clock, events, 4);
        assertThat(restored.snapshot()).isEqualTo(directory.snapshot());
        assertThat(restored.eventLog()).isEqualTo(directory.eventLog());
    }

    @Test
    void aHeartbeatCostsNoDurableWriteAndLeavesTheLogAlone() {
        CountingRepository events = new CountingRepository();
        ClusterFixtures.MutableClock clock = new ClusterFixtures.MutableClock(ClusterFixtures.T0);
        PersistentClusterDirectory directory = new PersistentClusterDirectory(
                ClusterFixtures.cluster(), clock, events, 4);
        directory.register(ClusterFixtures.node("node-1"));
        int savesAfterRegistration = events.saves;
        List<ClusterEvent> logAfterRegistration = directory.eventLog();

        for (int heartbeat = 2; heartbeat <= 500; heartbeat++) {
            directory.heartbeat(ClusterFixtures.presenceBuilder("node-1", heartbeat).build());
        }

        // This is the whole point of the change: the cluster's most frequent call is off the
        // durable write path. Five hundred heartbeats, not one repository round trip, and a
        // log that never moved, so nothing here can ever reach a fold or a storage wall.
        assertThat(events.saves).isEqualTo(savesAfterRegistration);
        assertThat(directory.eventLog()).isEqualTo(logAfterRegistration);
        assertThat(directory.checkpoint()).isEmpty();
        assertThat(directory.presence("node-1").orElseThrow().getHeartbeatSeq()).isEqualTo(500);
    }

    @Test
    void aRenewingNodeCostsNothingDurableAndStaysLive() {
        CountingRepository events = new CountingRepository();
        ClusterFixtures.MutableClock clock = new ClusterFixtures.MutableClock(ClusterFixtures.T0);
        PersistentClusterDirectory directory = new PersistentClusterDirectory(
                ClusterFixtures.cluster(), clock, events, 4);
        directory.register(ClusterFixtures.node("node-1"));
        directory.registerProcessor(ClusterFixtures.processorBuilder("proc-1", "node-1").build());
        directory.updateCapacity(ClusterFixtures.capacityBuilder("node-1", 1)
                .setProcessorId("proc-1").build());
        int savesAfterSetup = events.saves;
        List<ClusterEvent> logAfterSetup = directory.eventLog();

        // What a publisher actually does: the same identity, over and over, with a moved
        // lease window. Nothing about the cluster changes, so nothing should be recorded.
        for (int cycle = 2; cycle <= 200; cycle++) {
            java.time.Instant at = ClusterFixtures.T0.plusSeconds(30L * cycle);
            directory.register(ClusterFixtures.nodeBuilder("node-1", 1, cycle)
                    .setAdvertisedAt(ClusterFixtures.ts(at)).build());
            directory.registerProcessor(ClusterFixtures.processorBuilder("proc-1", "node-1")
                    .setSeq(cycle)
                    .setAdvertisedAt(ClusterFixtures.ts(at))
                    .setLeaseExpiresAt(ClusterFixtures.ts(at.plusSeconds(90))).build());
            directory.updateCapacity(ClusterFixtures.capacityBuilder("node-1", cycle)
                    .setProcessorId("proc-1")
                    .setObservedAt(ClusterFixtures.ts(at)).build());
        }

        assertThat(events.saves).isEqualTo(savesAfterSetup);
        assertThat(directory.eventLog()).isEqualTo(logAfterSetup);
        assertThat(directory.checkpoint()).isEmpty();

        // The renewals still count. The original windows closed thousands of seconds ago;
        // only the carried-over refreshes keep this identity alive, so a sweep here proves
        // the soft state survived every rebuild in the loop above. The clock stops short of
        // the last renewal's own expiry, which is the node presence at T0 + 6000 + ttl.
        clock.advance(java.time.Duration.ofSeconds(30L * 200 - 10));
        assertThat(directory.sweep()).isEmpty();
        assertThat(directory.processor("proc-1")).isPresent();
    }

    @Test
    void aSubstantiveChangeStillCostsADurableWrite() {
        CountingRepository events = new CountingRepository();
        ClusterFixtures.MutableClock clock = new ClusterFixtures.MutableClock(ClusterFixtures.T0);
        PersistentClusterDirectory directory = new PersistentClusterDirectory(
                ClusterFixtures.cluster(), clock, events, 64);
        directory.register(ClusterFixtures.node("node-1"));
        int savesAfterRegistration = events.saves;

        // A capability is identity, not a lease window. The refresh test must not be a
        // licence to drop real changes on the floor.
        directory.register(ClusterFixtures.nodeBuilder("node-1", 1, 2)
                .addCapabilities("gpu").build());

        assertThat(events.saves).isGreaterThan(savesAfterRegistration);
        assertThat(directory.node("node-1").orElseThrow().getCapabilitiesList())
                .contains("gpu");
    }

    @Test
    void aDurableChangeKeepsPresenceThatOnlyMemoryKnows() {
        CountingRepository events = new CountingRepository();
        ClusterFixtures.MutableClock clock = new ClusterFixtures.MutableClock(ClusterFixtures.T0);
        PersistentClusterDirectory directory = new PersistentClusterDirectory(
                ClusterFixtures.cluster(), clock, events, 4);
        directory.register(ClusterFixtures.node("node-1"));
        directory.heartbeat(ClusterFixtures.presenceBuilder("node-1", 9)
                .setTtl(com.google.protobuf.Duration.newBuilder().setSeconds(600))
                .setExpiresAt(ClusterFixtures.ts(ClusterFixtures.T0.plusSeconds(600)))
                .build());

        // An unrelated durable mutation rebuilds its candidate from the log, which has never
        // seen a heartbeat. Without carrying live presence across that rebuild the node's
        // liveness silently reverts to whatever registration armed, and the next sweep takes
        // a node that is heartbeating perfectly well.
        directory.updateCapacity(ClusterFixtures.capacityBuilder("node-1", 1).build());

        assertThat(directory.presence("node-1").orElseThrow().getHeartbeatSeq()).isEqualTo(9);
        assertThat(directory.presence("node-1").orElseThrow().getExpiresAt())
                .isEqualTo(ClusterFixtures.ts(ClusterFixtures.T0.plusSeconds(600)));

        clock.advance(java.time.Duration.ofSeconds(120));
        assertThat(directory.sweep()).isEmpty();
        assertThat(directory.node("node-1")).isPresent();
    }

    @Test
    void aSupersededIncarnationCannotHeartbeatEvenThoughNoHeartbeatIsDurable() {
        CountingRepository events = new CountingRepository();
        ClusterFixtures.MutableClock clock = new ClusterFixtures.MutableClock(ClusterFixtures.T0);
        PersistentClusterDirectory directory = new PersistentClusterDirectory(
                ClusterFixtures.cluster(), clock, events, 4);
        directory.register(ClusterFixtures.nodeBuilder("node-1", 7, 1).build());
        directory.register(ClusterFixtures.nodeBuilder("node-1", 9, 2).build());

        // Presence emits nothing, so the fence cannot live in the heartbeat history. It lives
        // in the registered epoch, which is durable, and that is what refuses the old frame.
        assertThatThrownBy(() -> directory.heartbeat(
                ClusterFixtures.presenceBuilder("node-1", 400).setNodeEpoch(7).build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("node_epoch");
    }

    @Test
    void aFoldedDirectoryStillFencesASupersededIncarnation() {
        CountingRepository events = new CountingRepository();
        ClusterFixtures.MutableClock clock = new ClusterFixtures.MutableClock(ClusterFixtures.T0);
        PersistentClusterDirectory directory = new PersistentClusterDirectory(
                ClusterFixtures.cluster(), clock, events, 1);
        directory.register(ClusterFixtures.nodeBuilder("node-1", 7, 2).build());
        directory.register(ClusterFixtures.nodeBuilder("node-1", 9, 1).build());

        assertThat(directory.checkpoint()).isPresent();
        PersistentClusterDirectory restored = new PersistentClusterDirectory(
                ClusterFixtures.cluster(), clock, events, 1);

        // The events that established epoch 9 are folded away, so only the checkpoint's
        // fencing tombstone can still refuse the superseded epoch.
        assertThatThrownBy(() -> restored.register(
                ClusterFixtures.nodeBuilder("node-1", 7, 3).build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aNodeThatExpiresAfterSoftRenewalsLeavesAReplayableLog() {
        CountingRepository events = new CountingRepository();
        ClusterFixtures.MutableClock clock = new ClusterFixtures.MutableClock(ClusterFixtures.T0);
        PersistentClusterDirectory directory = new PersistentClusterDirectory(
                ClusterFixtures.cluster(), clock, events, 64);
        directory.register(ClusterFixtures.node("node-1"));

        // Refresh-only re-advertisements live in memory alone: the log still holds the
        // registration at seq 1, while the directory holds the renewal at seq 5.
        for (int cycle = 2; cycle <= 5; cycle++) {
            java.time.Instant at = ClusterFixtures.T0.plusSeconds(10L * cycle);
            clock.advance(java.time.Duration.ofSeconds(10));
            directory.register(ClusterFixtures.nodeBuilder("node-1", 1, cycle)
                    .setAdvertisedAt(ClusterFixtures.ts(at)).build());
        }
        int savesBeforeExpiry = events.saves;

        // The node goes quiet and its presence lapses. The expiry that the sweep records
        // is durable, and it names the advertisement the directory currently holds.
        clock.advance(java.time.Duration.ofSeconds(120));
        assertThat(directory.sweep()).hasSize(1);
        assertThat(events.saves).isEqualTo(savesBeforeExpiry + 1);
        assertThat(directory.node("node-1")).isEmpty();

        // The log the directory just wrote must replay. Every later mutation rebuilds
        // from it, and so does a restart; a log the directory refuses is a directory
        // that can never change again.
        assertThat(directory.register(ClusterFixtures.nodeBuilder("node-1", 1, 6).build()))
                .isEqualTo(ClusterDirectory.ApplyOutcome.REGISTERED);
        PersistentClusterDirectory restored = new PersistentClusterDirectory(
                ClusterFixtures.cluster(), clock, events, 64);
        assertThat(restored.node("node-1")).isPresent();
        assertThat(restored.snapshot().getSnapshotSeq())
                .isEqualTo(directory.snapshot().getSnapshotSeq());
    }

    @Test
    void aProcessorWhoseLeaseLapsesAfterSoftRenewalsLeavesAReplayableLog() {
        CountingRepository events = new CountingRepository();
        ClusterFixtures.MutableClock clock = new ClusterFixtures.MutableClock(ClusterFixtures.T0);
        PersistentClusterDirectory directory = new PersistentClusterDirectory(
                ClusterFixtures.cluster(), clock, events, 64);
        directory.register(ClusterFixtures.node("node-1"));
        directory.registerProcessor(ClusterFixtures.processorBuilder("proc-1", "node-1").build());
        // Keep the node itself alive well past the processor lease, so only the processor
        // expires and the failure, if any, is the processor path's own.
        directory.heartbeat(ClusterFixtures.presenceBuilder("node-1", 2)
                .setTtl(com.google.protobuf.Duration.newBuilder().setSeconds(3600))
                .setExpiresAt(ClusterFixtures.ts(ClusterFixtures.T0.plusSeconds(3600)))
                .build());

        for (int cycle = 2; cycle <= 5; cycle++) {
            java.time.Instant at = ClusterFixtures.T0.plusSeconds(10L * cycle);
            clock.advance(java.time.Duration.ofSeconds(10));
            directory.registerProcessor(ClusterFixtures.processorBuilder("proc-1", "node-1")
                    .setSeq(cycle)
                    .setAdvertisedAt(ClusterFixtures.ts(at))
                    .setLeaseExpiresAt(ClusterFixtures.ts(at.plusSeconds(60))).build());
        }
        int savesBeforeExpiry = events.saves;

        clock.advance(java.time.Duration.ofSeconds(120));
        assertThat(directory.sweep()).hasSize(1);
        assertThat(events.saves).isEqualTo(savesBeforeExpiry + 1);
        assertThat(directory.processor("proc-1")).isEmpty();
        assertThat(directory.node("node-1")).isPresent();

        assertThat(directory.registerProcessor(
                ClusterFixtures.processorBuilder("proc-1", "node-1").setSeq(6)
                        .setLeaseExpiresAt(ClusterFixtures.ts(clock.instant().plusSeconds(60)))
                        .build()))
                .isEqualTo(ClusterDirectory.ApplyOutcome.REGISTERED);
        PersistentClusterDirectory restored = new PersistentClusterDirectory(
                ClusterFixtures.cluster(), clock, events, 64);
        assertThat(restored.processor("proc-1")).isPresent();
        assertThat(restored.snapshot().getSnapshotSeq())
                .isEqualTo(directory.snapshot().getSnapshotSeq());
    }

    private static class CountingRepository implements ClusterEventRepository {
        private StoredDirectory current = StoredDirectory.of(List.of());
        private int saves;

        @Override
        public Optional<StoredDirectory> load(ClusterDescriptor cluster) {
            return current.events().isEmpty() && !current.compacted()
                    ? Optional.empty()
                    : Optional.of(current);
        }

        @Override
        public void save(ClusterDescriptor cluster, StoredDirectory directory) {
            saves++;
            current = directory;
        }
    }

    /** Holds one save open so a reader can be observed while a commit is in flight. */
    private static final class BlockingRepository extends CountingRepository {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        @Override
        public void save(ClusterDescriptor cluster, StoredDirectory directory) {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while persisting", e);
            }
            super.save(cluster, directory);
        }
    }

    private static final class FailingRepository extends CountingRepository {
        private boolean fail;

        @Override
        public void save(ClusterDescriptor cluster, StoredDirectory directory) {
            if (fail) {
                throw new IllegalStateException("repository unavailable");
            }
            super.save(cluster, directory);
        }
    }
}
