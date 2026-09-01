package ai.pipestream.proto.jobs.service.events;

import ai.pipestream.proto.jobs.service.store.WorkflowRunEventRecord;
import ai.pipestream.proto.jobs.service.store.WorkflowRunRecord;
import ai.pipestream.proto.jobs.service.store.InMemoryWorkflowRunStore;
import ai.pipestream.proto.jobs.v1.WorkflowRunEvent;
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
class WorkflowRunEventRelayTest {

    private static final String TOPIC = "workflow-run-events";

    InMemoryWorkflowRunStore store;
    MockProducer<String, Message> producer;
    WorkflowRunEventRelay relay;

    @BeforeEach
    void fresh() {
        store = new InMemoryWorkflowRunStore();
        producer = new MockProducer<>(true, null, new StringSerializer(),
                (topic, data) -> data == null ? null : data.toByteArray());
        relay = new WorkflowRunEventRelay(store, producer, TOPIC, Duration.ofMillis(10), 100);
    }

    @AfterEach
    void stop() {
        relay.close();
    }

    /** Insert one QUEUED job; the ACCEPTED event lands in the outbox. */
    private WorkflowRunRecord insertJob() {
        WorkflowRunRecord record = new WorkflowRunRecord();
        record.jobId = UUID.randomUUID();
        record.workflowName = "embed-text";
        record.workflowDefinition = "{\"name\": \"embed-text\"}";
        record.input = "{\"text\": \"hi\"}";
        record.status = WorkflowRunRecord.STATUS_QUEUED;
        record.maxAttempts = 3;
        record.runAfter = Instant.now();
        store.insert(record, WorkflowRunEventFactory.accepted(record));
        return record;
    }

    @Test
    void relayOncePublishesEveryPendingEventKeyedByJobId() throws Exception {
        WorkflowRunRecord first = insertJob();
        WorkflowRunRecord second = insertJob();

        assertThat(relay.relayOnce()).isEqualTo(2);

        assertThat(producer.history()).hasSize(2);
        ProducerRecord<String, Message> sent = producer.history().get(0);
        assertThat(sent.topic()).isEqualTo(TOPIC);
        assertThat(sent.key()).isEqualTo(first.jobId.toString());
        WorkflowRunEvent event = (WorkflowRunEvent) sent.value();
        assertThat(event.getJobId()).isEqualTo(first.jobId.toString());
        assertThat(event.getType()).isEqualTo(WorkflowRunEvent.Type.TYPE_ACCEPTED);
        assertThat(producer.history().get(1).key()).isEqualTo(second.jobId.toString());

        // Both outbox rows settled PUBLISHED with the ack timestamp.
        assertThat(store.events()).allSatisfy(row -> {
            assertThat(row.status).isEqualTo(WorkflowRunEventRecord.STATUS_PUBLISHED);
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
        WorkflowRunEventRecord row = store.events().get(0);
        assertThat(row.attempts).isEqualTo(1);
        assertThat(row.lastError).isEqualTo("broker down");
        // Still PENDING: the failure never settles the row.
        assertThat(row.status).isEqualTo(WorkflowRunEventRecord.STATUS_PENDING);

        // The broker comes back: the very next drain publishes the same row.
        producer.sendException = null;
        assertThat(relay.relayOnce()).isEqualTo(1);
        assertThat(store.events().get(0).status)
                .isEqualTo(WorkflowRunEventRecord.STATUS_PUBLISHED);
        assertThat(((WorkflowRunEvent) producer.history().get(0).value()).getEventId())
                .isEqualTo(row.eventId.toString());
    }

    @Test
    void anUndecodablePayloadIsMarkedFailedWithoutReachingTheBroker() {
        WorkflowRunRecord job = new WorkflowRunRecord();
        job.jobId = UUID.randomUUID();
        job.workflowName = "embed-text";
        job.workflowDefinition = "{\"name\": \"embed-text\"}";
        job.input = "{\"text\": \"hi\"}";
        job.status = WorkflowRunRecord.STATUS_QUEUED;
        job.maxAttempts = 3;
        job.runAfter = Instant.now();
        WorkflowRunEventRecord garbage = new WorkflowRunEventRecord();
        garbage.eventId = UUID.randomUUID();
        garbage.eventType = WorkflowRunEventRecord.TYPE_ACCEPTED;
        garbage.payload = new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        garbage.kafkaKey = job.jobId.toString();
        garbage.createdAt = Instant.now();
        store.insert(job, garbage);

        assertThat(relay.relayOnce()).isZero();
        assertThat(producer.history()).isEmpty();
        WorkflowRunEventRecord row = store.events().stream()
                .filter(e -> e.eventId.equals(garbage.eventId)).findFirst().orElseThrow();
        assertThat(row.attempts).isEqualTo(1);
        assertThat(row.status).isEqualTo(WorkflowRunEventRecord.STATUS_PENDING);
    }

    @Test
    void persistentFailuresLandTheRowInTheDlqAtTheAttemptsCeiling() {
        insertJob();
        producer.sendException = new RuntimeException("broker down");

        for (int i = 0; i < WorkflowRunEventRecord.MAX_ATTEMPTS; i++) {
            assertThat(relay.relayOnce()).isZero();
        }
        WorkflowRunEventRecord row = store.events().get(0);
        assertThat(row.attempts).isEqualTo(WorkflowRunEventRecord.MAX_ATTEMPTS);
        assertThat(row.status).isEqualTo(WorkflowRunEventRecord.STATUS_FAILED);
        assertThat(row.lastError).isEqualTo("broker down");

        // The DLQ row leaves the drain for good, even when the broker recovers.
        producer.sendException = null;
        assertThat(relay.relayOnce()).isZero();
        assertThat(producer.history()).isEmpty();
    }

    @Test
    void theLoopDrainsNewRowsAndStopsOnClose() throws Exception {
        WorkflowRunRecord job = insertJob();
        relay.start();

        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            if (WorkflowRunEventRecord.STATUS_PUBLISHED.equals(store.events().get(0).status)) {
                break;
            }
            Thread.sleep(20);
        }
        relay.close();

        assertThat(store.events().get(0).status)
                .isEqualTo(WorkflowRunEventRecord.STATUS_PUBLISHED);
        assertThat(producer.history()).hasSize(1);
        assertThat(producer.history().get(0).key()).isEqualTo(job.jobId.toString());
    }
}
