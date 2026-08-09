package ai.pipestream.proto.jobs.service.events;

import ai.pipestream.proto.jobs.service.store.ChainJobEventRecord;
import ai.pipestream.proto.jobs.service.store.ChainJobRecord;
import ai.pipestream.proto.jobs.v1.ChainJobEvent;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The outbox-row factory: every commit point's event carries the job's
 * identity (event_id is the consumer dedupe key, kafka_key the job id for
 * partition ordering), the lifecycle payload for its type, and a parseable
 * ChainJobEvent protobuf. {@code descriptorSetBase64} packages jobs.proto and
 * its transitive imports from the generated classes' own descriptors.
 */
class ChainJobEventFactoryTest {

    private static ChainJobRecord job() {
        ChainJobRecord job = new ChainJobRecord();
        job.jobId = UUID.randomUUID();
        job.chainName = "embed-text";
        job.status = ChainJobRecord.STATUS_QUEUED;
        job.attempt = 2;
        return job;
    }

    private static ChainJobEvent parse(ChainJobEventRecord record) throws Exception {
        return ChainJobEvent.parseFrom(record.payload);
    }

    /** The envelope every event shares, regardless of type. */
    private static void assertEnvelope(ChainJobEventRecord record, ChainJobRecord job,
            String eventType) throws Exception {
        assertThat(record.eventId).isNotNull();
        assertThat(record.eventType).isEqualTo(eventType);
        assertThat(record.kafkaKey).isEqualTo(job.jobId.toString());
        assertThat(record.createdAt).isNotNull();
        assertThat(record.status).isEqualTo(ChainJobEventRecord.STATUS_PENDING);
        assertThat(record.attempts).isZero();
        ChainJobEvent event = parse(record);
        // The dedupe key the protobuf echoes is the outbox row id.
        assertThat(event.getEventId()).isEqualTo(record.eventId.toString());
        assertThat(event.getJobId()).isEqualTo(job.jobId.toString());
        assertThat(event.getChainName()).isEqualTo(job.chainName);
        assertThat(event.getAttempt()).isEqualTo(job.attempt);
        assertThat(event.getDetail().getFieldsOrThrow("status").getStringValue())
                .isEqualTo(job.status);
        assertThat(event.getOccurredAt().getSeconds()).isPositive();
    }

    @Test
    void acceptedCarriesNoStepVerdictOrError() throws Exception {
        ChainJobRecord job = job();
        ChainJobEventRecord record = ChainJobEventFactory.accepted(job);
        assertEnvelope(record, job, ChainJobEventRecord.TYPE_ACCEPTED);
        ChainJobEvent event = parse(record);
        assertThat(event.getType()).isEqualTo(ChainJobEvent.Type.TYPE_ACCEPTED);
        assertThat(event.getStep()).isEmpty();
        assertThat(event.getVerdict()).isEmpty();
        assertThat(event.getError()).isEmpty();
    }

    @Test
    void stepCheckpointAndWaitingCarryTheStep() throws Exception {
        ChainJobRecord job = job();
        ChainJobEventRecord checkpoint = ChainJobEventFactory.stepCheckpoint(job, "embed");
        assertEnvelope(checkpoint, job, ChainJobEventRecord.TYPE_STEP_CHECKPOINT);
        assertThat(parse(checkpoint).getType())
                .isEqualTo(ChainJobEvent.Type.TYPE_STEP_CHECKPOINT);
        assertThat(parse(checkpoint).getStep()).isEqualTo("embed");

        ChainJobEventRecord waiting = ChainJobEventFactory.waiting(job, "review");
        assertEnvelope(waiting, job, ChainJobEventRecord.TYPE_WAITING);
        assertThat(parse(waiting).getType()).isEqualTo(ChainJobEvent.Type.TYPE_WAITING);
        assertThat(parse(waiting).getStep()).isEqualTo("review");
    }

    @Test
    void completedCarriesTheVerdict() throws Exception {
        ChainJobRecord job = job();
        ChainJobEventRecord record =
                ChainJobEventFactory.completed(job, "2 steps, output jobs.test.Embedding");
        assertEnvelope(record, job, ChainJobEventRecord.TYPE_COMPLETED);
        ChainJobEvent event = parse(record);
        assertThat(event.getType()).isEqualTo(ChainJobEvent.Type.TYPE_COMPLETED);
        assertThat(event.getVerdict()).isEqualTo("2 steps, output jobs.test.Embedding");
        assertThat(event.getError()).isEmpty();
    }

    @Test
    void failedCarriesTheStepAndTheVerbatimErrorAndToleratesANullStep() throws Exception {
        ChainJobRecord job = job();
        ChainJobEventRecord record =
                ChainJobEventFactory.failed(job, "embed", "GRPC: UNAVAILABLE");
        assertEnvelope(record, job, ChainJobEventRecord.TYPE_FAILED);
        ChainJobEvent event = parse(record);
        assertThat(event.getType()).isEqualTo(ChainJobEvent.Type.TYPE_FAILED);
        assertThat(event.getStep()).isEqualTo("embed");
        assertThat(event.getError()).isEqualTo("GRPC: UNAVAILABLE");

        // A chain-level failure has no step; the proto carries "".
        ChainJobEventRecord chainLevel = ChainJobEventFactory.failed(job, null, "CHAIN: bad");
        assertThat(parse(chainLevel).getStep()).isEmpty();
        assertThat(parse(chainLevel).getError()).isEqualTo("CHAIN: bad");
    }

    @Test
    void deadCarriesTheLastError() throws Exception {
        ChainJobRecord job = job();
        ChainJobEventRecord record = ChainJobEventFactory.dead(job, "GRPC: model loading");
        assertEnvelope(record, job, ChainJobEventRecord.TYPE_DEAD);
        ChainJobEvent event = parse(record);
        assertThat(event.getType()).isEqualTo(ChainJobEvent.Type.TYPE_DEAD);
        assertThat(event.getError()).isEqualTo("GRPC: model loading");
    }

    @Test
    void aNullChainNameOrStatusBecomesAnEmptyString() throws Exception {
        ChainJobRecord job = job();
        job.chainName = null;
        job.status = null;
        ChainJobEvent event = parse(ChainJobEventFactory.accepted(job));
        assertThat(event.getChainName()).isEmpty();
        assertThat(event.getDetail().getFieldsOrThrow("status").getStringValue()).isEmpty();
    }

    @Test
    void theDescriptorSetPackagesJobsProtoAndItsTransitiveImports() throws Exception {
        byte[] decoded = Base64.getDecoder().decode(ChainJobEventFactory.descriptorSetBase64());
        FileDescriptorSet set = FileDescriptorSet.parseFrom(decoded);
        assertThat(set.getFileList()).isNotEmpty();
        assertThat(set.getFileList().stream().map(f -> f.getName()))
                .contains("ai/pipestream/proto/jobs/v1/jobs.proto",
                        "google/protobuf/struct.proto",
                        "google/protobuf/timestamp.proto");
        // The packaged jobs.proto declares the event the relay frames.
        assertThat(set.getFileList().stream()
                .filter(f -> f.getName().equals("ai/pipestream/proto/jobs/v1/jobs.proto"))
                .flatMap(f -> f.getMessageTypeList().stream().map(m -> m.getName())))
                .contains("ChainJobEvent", "ChainJobRequest");
    }
}
