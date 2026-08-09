package ai.pipestream.proto.jobs.service.events;

import ai.pipestream.proto.jobs.service.store.ChainJobEventRecord;
import ai.pipestream.proto.jobs.service.store.ChainJobRecord;
import ai.pipestream.proto.jobs.service.store.InMemoryChainJobStore;
import ai.pipestream.proto.jobs.v1.ChainJobEvent;
import com.google.protobuf.Message;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The outbox relay's drain against a {@link MockProducer} and the in-memory
 * store — no broker: a successful publish settles the row PUBLISHED keyed by
 * job id, a send failure or an undecodable payload bumps the relay attempts
 * (landing the row in the DLQ at the ceiling), and the virtual-thread loop
 * drains on its own and stops on close.
 */
class ChainJobEventRelayTest {

    private static final String TOPIC = "chain-job-events";

    InMemoryChainJobStore store;
    MockProducer<String, Message> producer;
    ChainJobEventRelay relay;

    @BeforeEach
    void fresh() {
        store = new InMemoryChainJobStore();
        producer = new MockProducer<>(true, new StringSerializer(),
                (topic, data) -> data == null ? null : data.toByteArray());
        relay = new ChainJobEventRelay(store, producer, TOPIC, Duration.ofMillis(10), 100);
    }

    @AfterEach
    void stop() {
        relay.close();
    }

    /** Insert one QUEUED job; the ACCEPTED event lands in the outbox. */
    private ChainJobRecord insertJob() {
        ChainJobRecord record = new ChainJobRecord();
        record.jobId = UUID.randomUUID();
        record.chainName = "embed-text";
        record.chainDefinition = "{\"name\": \"embed-text\"}";
        record.input = "{\"text\": \"hi\"}";
        record.status = ChainJobRecord.STATUS_QUEUED;
        record.maxAttempts = 3;
        record.runAfter = Instant.now();
        store.insert(record, ChainJobEventFactory.accepted(record));
        return record;
    }

    @Test
    void relayOncePublishesEveryPendingEventKeyedByJobId() throws Exception {
        ChainJobRecord first = insertJob();
        ChainJobRecord second = insertJob();

        assertThat(relay.relayOnce()).isEqualTo(2);

        assertThat(producer.history()).hasSize(2);
        ProducerRecord<String, Message> sent = producer.history().get(0);
        assertThat(sent.topic()).isEqualTo(TOPIC);
        assertThat(sent.key()).isEqualTo(first.jobId.toString());
        ChainJobEvent event = (ChainJobEvent) sent.value();
        assertThat(event.getJobId()).isEqualTo(first.jobId.toString());
        assertThat(event.getType()).isEqualTo(ChainJobEvent.Type.TYPE_ACCEPTED);
        assertThat(producer.history().get(1).key()).isEqualTo(second.jobId.toString());

        // Both outbox rows settled PUBLISHED with the ack timestamp.
        assertThat(store.events()).allSatisfy(row -> {
            assertThat(row.status).isEqualTo(ChainJobEventRecord.STATUS_PUBLISHED);
            assertThat(row.publishedAt).isNotNull();
        });
    }

    @Test
    void anEmptyDrainPublishesNothing() {
        assertThat(relay.relayOnce()).isZero();
        assertThat(producer.history()).isEmpty();
    }

    @Test
    void aSendFailureBumpsAttemptsAndTheNextDrainRecovers() {
        insertJob();
        producer.sendException = new RuntimeException("broker down");

        assertThat(relay.relayOnce()).isZero();
        assertThat(producer.history()).isEmpty();
        ChainJobEventRecord row = store.events().get(0);
        assertThat(row.attempts).isEqualTo(1);
        assertThat(row.lastError).isEqualTo("broker down");
        // Still PENDING: the failure never settles the row.
        assertThat(row.status).isEqualTo(ChainJobEventRecord.STATUS_PENDING);

        // The broker comes back: the very next drain publishes the same row.
        producer.sendException = null;
        assertThat(relay.relayOnce()).isEqualTo(1);
        assertThat(store.events().get(0).status)
                .isEqualTo(ChainJobEventRecord.STATUS_PUBLISHED);
        assertThat(((ChainJobEvent) producer.history().get(0).value()).getEventId())
                .isEqualTo(row.eventId.toString());
    }

    @Test
    void anUndecodablePayloadIsMarkedFailedWithoutReachingTheBroker() {
        ChainJobRecord job = new ChainJobRecord();
        job.jobId = UUID.randomUUID();
        job.chainName = "embed-text";
        job.chainDefinition = "{\"name\": \"embed-text\"}";
        job.input = "{\"text\": \"hi\"}";
        job.status = ChainJobRecord.STATUS_QUEUED;
        job.maxAttempts = 3;
        job.runAfter = Instant.now();
        ChainJobEventRecord garbage = new ChainJobEventRecord();
        garbage.eventId = UUID.randomUUID();
        garbage.eventType = ChainJobEventRecord.TYPE_ACCEPTED;
        garbage.payload = new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        garbage.kafkaKey = job.jobId.toString();
        garbage.createdAt = Instant.now();
        store.insert(job, garbage);

        assertThat(relay.relayOnce()).isZero();
        assertThat(producer.history()).isEmpty();
        ChainJobEventRecord row = store.events().stream()
                .filter(e -> e.eventId.equals(garbage.eventId)).findFirst().orElseThrow();
        assertThat(row.attempts).isEqualTo(1);
        assertThat(row.status).isEqualTo(ChainJobEventRecord.STATUS_PENDING);
    }

    @Test
    void persistentFailuresLandTheRowInTheDlqAtTheAttemptsCeiling() {
        insertJob();
        producer.sendException = new RuntimeException("broker down");

        for (int i = 0; i < ChainJobEventRecord.MAX_ATTEMPTS; i++) {
            assertThat(relay.relayOnce()).isZero();
        }
        ChainJobEventRecord row = store.events().get(0);
        assertThat(row.attempts).isEqualTo(ChainJobEventRecord.MAX_ATTEMPTS);
        assertThat(row.status).isEqualTo(ChainJobEventRecord.STATUS_FAILED);
        assertThat(row.lastError).isEqualTo("broker down");

        // The DLQ row leaves the drain for good, even when the broker recovers.
        producer.sendException = null;
        assertThat(relay.relayOnce()).isZero();
        assertThat(producer.history()).isEmpty();
    }

    @Test
    void theLoopDrainsNewRowsAndStopsOnClose() throws Exception {
        ChainJobRecord job = insertJob();
        relay.start();

        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            if (ChainJobEventRecord.STATUS_PUBLISHED.equals(store.events().get(0).status)) {
                break;
            }
            Thread.sleep(20);
        }
        relay.close();

        assertThat(store.events().get(0).status)
                .isEqualTo(ChainJobEventRecord.STATUS_PUBLISHED);
        assertThat(producer.history()).hasSize(1);
        assertThat(producer.history().get(0).key()).isEqualTo(job.jobId.toString());
    }
}
