package ai.protomolt.proto.repo.container.lifecycle;

import ai.protomolt.proto.repo.container.ledger.DocumentPurgeRecord;
import com.google.protobuf.Message;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityTransaction;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Kafka-backed purge queue against real testcontainers PostgreSQL 17 and
 * Redpanda: Phase A transactional atomicity (a rolled-back enqueue is never
 * relayed), the relay/claim/settle happy path, crash redelivery through the
 * consumer group, duplicate delivery settling to one winner, the markFailed
 * republication ladder up to FAILED (the DLQ), and a raw poison frame that
 * must not wedge the group.
 * <p>
 * Every test gets its own topic and consumer group so the shared database's
 * rows cannot cross-talk; a claim that finds a leftover row from another
 * test settles it VOID on the spot (a legitimate settle of seeded state).
 */
@Testcontainers(disabledWithoutDocker = true)
class KafkaPurgeQueueIT extends AbstractLifecycleIT {

    // Same baseline image as the serde lane (testcontainers' own pinned tag).
    @Container
    static final RedpandaContainer REDPANDA = new RedpandaContainer(
            DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v22.2.1"));

    private static final Duration POLL = Duration.ofSeconds(5);

    /** A queue plus its Kafka pair, so a test can close (crash) it mid-flight. */
    private record QueueHandle(KafkaPurgeQueue queue, KafkaProducer<String, Message> producer,
            KafkaConsumer<String, byte[]> consumer) implements AutoCloseable {
        @Override
        public void close() {
            consumer.close();
            producer.close();
        }
    }

    private final List<QueueHandle> handles = new ArrayList<>();

    @AfterEach
    void closeKafka() {
        handles.forEach(QueueHandle::close);
        handles.clear();
    }

    @Test
    void aRolledBackEnqueueIsNeverRelayedOrClaimable() {
        QueueHandle handle = newQueue(unique("purge-it-rb"), unique("purge-it-rb"));
        DocumentPurgeRecord record = newRecord(Instant.now());

        EntityManager em = database.entityManagerFactory().createEntityManager();
        EntityTransaction transaction = em.getTransaction();
        transaction.begin();
        try {
            handle.queue().enqueue(em, record);
        } finally {
            transaction.rollback();
            em.close();
        }

        assertThat(findPurge(record.purgeId)).isEmpty();
        // The relay scan sees nothing and the topic stays empty for it.
        assertThat(handle.queue().claimBatch(10))
                .noneMatch(r -> r.purgeId.equals(record.purgeId));
    }

    @Test
    void happyPathRelaysClaimsAndSettlesOnce() {
        QueueHandle handle = newQueue(unique("purge-it-happy"), unique("purge-it-happy"));
        DocumentPurgeRecord record = enqueue(handle.queue(), Instant.now());

        DocumentPurgeRecord claimed = awaitClaim(handle.queue(), record.purgeId);
        assertThat(claimed.nodeId).isEqualTo(record.nodeId);
        assertThat(claimed.accountId).isEqualTo(record.accountId);
        assertThat(claimed.driveName).isEqualTo(record.driveName);
        assertThat(claimed.readObjectKeys()).isEqualTo(record.readObjectKeys());
        assertThat(claimed.requestedAt.getEpochSecond())
                .isEqualTo(record.requestedAt.getEpochSecond());
        // The relay stamped the row after the broker ack.
        assertThat(findPurge(record.purgeId).orElseThrow().relayedAt).isNotNull();

        assertThat(handle.queue().markPurged(record.purgeId)).isTrue();
        assertThat(handle.queue().markPurged(record.purgeId)).isFalse();
        assertThat(handle.queue().claimBatch(10))
                .noneMatch(r -> r.purgeId.equals(record.purgeId));
        assertThat(findPurge(record.purgeId).orElseThrow().status)
                .isEqualTo(DocumentPurgeRecord.STATUS_PURGED);
    }

    @Test
    void crashBeforeOffsetCommitRedeliversToTheRestartedConsumer() {
        String topic = unique("purge-it-crash");
        String group = unique("purge-it-crash");
        QueueHandle first = newQueue(topic, group);
        DocumentPurgeRecord record = enqueue(first.queue(), Instant.now());

        DocumentPurgeRecord claimed = awaitClaim(first.queue(), record.purgeId);
        assertThat(claimed.purgeId).isEqualTo(record.purgeId);
        // The crash: nothing settles, nothing commits, the consumer leaves.
        first.close();
        handles.remove(first);

        QueueHandle second = newQueue(topic, group);
        DocumentPurgeRecord redelivered = awaitClaim(second.queue(), record.purgeId);
        assertThat(redelivered.purgeId).isEqualTo(record.purgeId);

        // Settling the second time works; the already-settled transition
        // returning false is tolerated.
        assertThat(second.queue().markPurged(record.purgeId)).isTrue();
        assertThat(second.queue().markPurged(record.purgeId)).isFalse();
        assertThat(second.queue().claimBatch(10))
                .noneMatch(r -> r.purgeId.equals(record.purgeId));
        assertThat(findPurge(record.purgeId).orElseThrow().status)
                .isEqualTo(DocumentPurgeRecord.STATUS_PURGED);
    }

    @Test
    void aDuplicateOnTheTopicSettlesToOneWinner() {
        QueueHandle handle = newQueue(unique("purge-it-dup"), unique("purge-it-dup"));
        DocumentPurgeRecord record = enqueue(handle.queue(), Instant.now());

        awaitClaim(handle.queue(), record.purgeId);
        // Crash between the broker ack and the relayed_at stamp: the row is
        // PENDING and unrelayed again, so the next claim re-publishes it.
        clearRelayedAt(record.purgeId);
        DocumentPurgeRecord duplicate = awaitClaim(handle.queue(), record.purgeId);
        assertThat(duplicate.purgeId).isEqualTo(record.purgeId);

        // One winner; both offsets of the duplicate delivery settle.
        assertThat(handle.queue().markPurged(record.purgeId)).isTrue();
        assertThat(handle.queue().markPurged(record.purgeId)).isFalse();
        assertThat(handle.queue().claimBatch(10))
                .noneMatch(r -> r.purgeId.equals(record.purgeId));
        assertThat(findPurge(record.purgeId).orElseThrow().status)
                .isEqualTo(DocumentPurgeRecord.STATUS_PURGED);
    }

    @Test
    void theFailureLadderRepublishesBelowTheCeilingAndDeadLettersAtIt() {
        QueueHandle handle = newQueue(unique("purge-it-ladder"), unique("purge-it-ladder"));
        Map<String, Long> before = queueCounts();
        DocumentPurgeRecord record = enqueue(handle.queue(), Instant.now());

        DocumentPurgeRecord current = awaitClaim(handle.queue(), record.purgeId);
        for (int attempt = 1; attempt < DocumentPurgeRecord.MAX_ATTEMPTS; attempt++) {
            Optional<DocumentPurgeRecord> updated =
                    handle.queue().markFailed(current, "boom " + attempt);
            assertThat(updated).isPresent();
            assertThat(updated.get().attempts).isEqualTo(attempt);
            assertThat(updated.get().status).isEqualTo(DocumentPurgeRecord.STATUS_PENDING);
            assertThat(updated.get().lastError).isEqualTo("boom " + attempt);
            // Below the ceiling the retry rides the tail of the topic.
            current = awaitClaim(handle.queue(), record.purgeId);
            assertThat(current.attempts).isEqualTo(attempt);
        }

        Optional<DocumentPurgeRecord> terminal = handle.queue().markFailed(current, "boom final");
        assertThat(terminal).isPresent();
        assertThat(terminal.get().attempts).isEqualTo(DocumentPurgeRecord.MAX_ATTEMPTS);
        assertThat(terminal.get().status).isEqualTo(DocumentPurgeRecord.STATUS_FAILED);
        // At the ceiling the FAILED row IS the DLQ: nothing is republished.
        assertThat(handle.queue().claimBatch(10))
                .noneMatch(r -> r.purgeId.equals(record.purgeId));
        assertThat(queueCounts().getOrDefault(DocumentPurgeRecord.STATUS_FAILED, 0L)
                - before.getOrDefault(DocumentPurgeRecord.STATUS_FAILED, 0L)).isEqualTo(1L);
    }

    @Test
    void aCorruptFrameOnTheTopicIsSkippedWithoutWedgingTheGroup() {
        String topic = unique("purge-it-poison");
        QueueHandle handle = newQueue(topic, unique("purge-it-poison"));

        // Bytes that are not a protomolt frame, produced straight onto the
        // topic behind the queue's back.
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, REDPANDA.getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, byte[]> raw = new KafkaProducer<>(props,
                new StringSerializer(), new ByteArraySerializer())) {
            raw.send(new ProducerRecord<>(topic, "poison",
                    "definitely-not-a-frame".getBytes(StandardCharsets.UTF_8))).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted producing the poison frame", e);
        } catch (java.util.concurrent.ExecutionException e) {
            throw new AssertionError("producing the poison frame failed", e);
        }

        // The valid command right behind the poison frame still claims; the
        // poison record is skipped and settled past.
        DocumentPurgeRecord record = enqueue(handle.queue(), Instant.now());
        DocumentPurgeRecord claimed = awaitClaim(handle.queue(), record.purgeId);
        assertThat(claimed.purgeId).isEqualTo(record.purgeId);
        assertThat(handle.queue().markPurged(record.purgeId)).isTrue();
        assertThat(handle.queue().claimBatch(10))
                .noneMatch(r -> r.purgeId.equals(record.purgeId));
    }

    // ------------------------------------------------------------------ helpers

    private QueueHandle newQueue(String topic, String groupId) {
        KafkaProducer<String, Message> producer =
                KafkaPurgeQueue.newProducer(REDPANDA.getBootstrapServers());
        KafkaConsumer<String, byte[]> consumer =
                KafkaPurgeQueue.newConsumer(REDPANDA.getBootstrapServers(), groupId);
        QueueHandle handle = new QueueHandle(
                KafkaPurgeQueue.create(tx, producer, consumer, topic, POLL), producer, consumer);
        handles.add(handle);
        return handle;
    }

    private static String unique(String prefix) {
        return prefix + "-" + Long.toUnsignedString(System.nanoTime(), 36);
    }

    private static DocumentPurgeRecord newRecord(Instant requestedAt) {
        DocumentPurgeRecord record = new DocumentPurgeRecord();
        record.purgeId = UUID.randomUUID();
        record.nodeId = UUID.randomUUID();
        record.docId = "doc-" + record.purgeId;
        record.graphAddressId = "ds-kq";
        record.accountId = "acct-kafka-queue";
        record.graphId = "intake:acct-kafka-queue";
        record.driveName = "docs";
        record.writeObjectKeys(List.of("k/" + record.purgeId));
        record.requestedAt = requestedAt;
        return record;
    }

    /** Enqueue one record in its own (committing) transaction. */
    private static DocumentPurgeRecord enqueue(KafkaPurgeQueue queue, Instant requestedAt) {
        DocumentPurgeRecord record = newRecord(requestedAt);
        tx.inTransaction(em -> {
            queue.enqueue(em, record);
        });
        return record;
    }

    /**
     * Claim until {@code purgeId} shows up (group joins and redeliveries can
     * eat a poll or two). Claims of leftover rows from sibling tests are
     * settled VOID on the spot.
     */
    private static DocumentPurgeRecord awaitClaim(KafkaPurgeQueue queue, UUID purgeId) {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            for (DocumentPurgeRecord claimed : queue.claimBatch(10)) {
                if (claimed.purgeId.equals(purgeId)) {
                    return claimed;
                }
                queue.markVoid(claimed.purgeId);
            }
        }
        throw new AssertionError("no claim for " + purgeId + " within 60s");
    }

    /** Simulate a crash between the broker ack and the relayed_at stamp. */
    private static void clearRelayedAt(UUID purgeId) {
        tx.inTransaction(em -> {
            em.createQuery("UPDATE DocumentPurgeRecord p SET p.relayedAt = NULL"
                            + " WHERE p.purgeId = :id")
                    .setParameter("id", purgeId)
                    .executeUpdate();
        });
    }
}
