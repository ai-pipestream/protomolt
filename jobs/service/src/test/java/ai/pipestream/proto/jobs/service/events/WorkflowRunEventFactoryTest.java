package ai.pipestream.proto.jobs.service.events;

import ai.pipestream.proto.jobs.service.store.WorkflowRunEventRecord;
import ai.pipestream.proto.jobs.service.store.WorkflowRunRecord;
import ai.pipestream.proto.jobs.v1.WorkflowRunEvent;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The outbox-row factory: every commit point's event carries the job's
 * identity (event_id is the consumer dedupe key, kafka_key the job id for
 * partition ordering), the lifecycle payload for its type, and a parseable
 * WorkflowRunEvent protobuf. {@code descriptorSetBase64} packages jobs.proto and
 * its transitive imports from the generated classes' own descriptors.
 */
class WorkflowRunEventFactoryTest {

    private static WorkflowRunRecord job() {
        WorkflowRunRecord job = new WorkflowRunRecord();
        job.jobId = UUID.randomUUID();
        job.workflowName = "embed-text";
        job.status = WorkflowRunRecord.STATUS_QUEUED;
        job.attempt = 2;
        return job;
    }

    private static WorkflowRunEvent parse(WorkflowRunEventRecord record) throws Exception {
        return WorkflowRunEvent.parseFrom(record.payload);
    }

    /** The envelope every event shares, regardless of type. */
    private static void assertEnvelope(WorkflowRunEventRecord record, WorkflowRunRecord job,
            String eventType) throws Exception {
        assertThat(record.eventId).isNotNull();
        assertThat(record.eventType).isEqualTo(eventType);
        assertThat(record.kafkaKey).isEqualTo(job.jobId.toString());
        assertThat(record.createdAt).isNotNull();
        assertThat(record.status).isEqualTo(WorkflowRunEventRecord.STATUS_PENDING);
        assertThat(record.attempts).isZero();
        WorkflowRunEvent event = parse(record);
        // The dedupe key the protobuf echoes is the outbox row id.
        assertThat(event.getEventId()).isEqualTo(record.eventId.toString());
        assertThat(event.getJobId()).isEqualTo(job.jobId.toString());
        assertThat(event.getWorkflowName()).isEqualTo(job.workflowName);
        assertThat(event.getAttempt()).isEqualTo(job.attempt);
        assertThat(event.getDetail().getFieldsOrThrow("status").getStringValue())
                .isEqualTo(job.status);
        assertThat(event.getOccurredAt().getSeconds()).isPositive();
    }

    @Test
    void acceptedCarriesNoStepVerdictOrError() throws Exception {
        WorkflowRunRecord job = job();
        WorkflowRunEventRecord record = WorkflowRunEventFactory.accepted(job);
        assertEnvelope(record, job, WorkflowRunEventRecord.TYPE_ACCEPTED);
        WorkflowRunEvent event = parse(record);
        assertThat(event.getType()).isEqualTo(WorkflowRunEvent.Type.TYPE_ACCEPTED);
        assertThat(event.getStep()).isEmpty();
        assertThat(event.getVerdict()).isEmpty();
        assertThat(event.getError()).isEmpty();
    }

    @Test
    void stepCheckpointAndWaitingCarryTheStep() throws Exception {
        WorkflowRunRecord job = job();
        WorkflowRunEventRecord checkpoint = WorkflowRunEventFactory.stepCheckpoint(job, "embed");
        assertEnvelope(checkpoint, job, WorkflowRunEventRecord.TYPE_STEP_CHECKPOINT);
        assertThat(parse(checkpoint).getType())
                .isEqualTo(WorkflowRunEvent.Type.TYPE_STEP_CHECKPOINT);
        assertThat(parse(checkpoint).getStep()).isEqualTo("embed");

        WorkflowRunEventRecord waiting = WorkflowRunEventFactory.waiting(job, "review");
        assertEnvelope(waiting, job, WorkflowRunEventRecord.TYPE_WAITING);
        assertThat(parse(waiting).getType()).isEqualTo(WorkflowRunEvent.Type.TYPE_WAITING);
        assertThat(parse(waiting).getStep()).isEqualTo("review");
    }

    @Test
    void completedCarriesTheVerdict() throws Exception {
        WorkflowRunRecord job = job();
        WorkflowRunEventRecord record =
                WorkflowRunEventFactory.completed(job, "2 steps, output jobs.test.Embedding");
        assertEnvelope(record, job, WorkflowRunEventRecord.TYPE_COMPLETED);
        WorkflowRunEvent event = parse(record);
        assertThat(event.getType()).isEqualTo(WorkflowRunEvent.Type.TYPE_COMPLETED);
        assertThat(event.getVerdict()).isEqualTo("2 steps, output jobs.test.Embedding");
        assertThat(event.getError()).isEmpty();
    }

    @Test
    void failedCarriesTheStepAndTheVerbatimErrorAndToleratesANullStep() throws Exception {
        WorkflowRunRecord job = job();
        WorkflowRunEventRecord record =
                WorkflowRunEventFactory.failed(job, "embed", "GRPC: UNAVAILABLE");
        assertEnvelope(record, job, WorkflowRunEventRecord.TYPE_FAILED);
        WorkflowRunEvent event = parse(record);
        assertThat(event.getType()).isEqualTo(WorkflowRunEvent.Type.TYPE_FAILED);
        assertThat(event.getStep()).isEqualTo("embed");
        assertThat(event.getError()).isEqualTo("GRPC: UNAVAILABLE");

        // A workflow-level failure has no step; the proto carries "".
        WorkflowRunEventRecord workflowLevel = WorkflowRunEventFactory.failed(job, null, "WORKFLOW: bad");
        assertThat(parse(workflowLevel).getStep()).isEmpty();
        assertThat(parse(workflowLevel).getError()).isEqualTo("WORKFLOW: bad");
    }

    @Test
    void deadCarriesTheLastError() throws Exception {
        WorkflowRunRecord job = job();
        WorkflowRunEventRecord record = WorkflowRunEventFactory.dead(job, "GRPC: model loading");
        assertEnvelope(record, job, WorkflowRunEventRecord.TYPE_DEAD);
        WorkflowRunEvent event = parse(record);
        assertThat(event.getType()).isEqualTo(WorkflowRunEvent.Type.TYPE_DEAD);
        assertThat(event.getError()).isEqualTo("GRPC: model loading");
    }

    @Test
    void aNullWorkflowNameOrStatusBecomesAnEmptyString() throws Exception {
        WorkflowRunRecord job = job();
        job.workflowName = null;
        job.status = null;
        WorkflowRunEvent event = parse(WorkflowRunEventFactory.accepted(job));
        assertThat(event.getWorkflowName()).isEmpty();
        assertThat(event.getDetail().getFieldsOrThrow("status").getStringValue()).isEmpty();
    }

    @Test
    void theDescriptorSetPackagesJobsProtoAndItsTransitiveImports() throws Exception {
        byte[] decoded = Base64.getDecoder().decode(WorkflowRunEventFactory.descriptorSetBase64());
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
                .contains("WorkflowRunEvent", "WorkflowRunRequest");
    }
}
