package ai.pipestream.proto.jobs.service.events;

import ai.pipestream.proto.jobs.service.store.WorkflowRunEventRecord;
import ai.pipestream.proto.jobs.service.store.WorkflowRunRecord;
import ai.pipestream.proto.jobs.v1.WorkflowRunEvent;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Builds the outbox rows for the workflow-run commit points: one
 * {@link WorkflowRunEvent} protobuf per event, serialized into a
 * {@link WorkflowRunEventRecord} whose {@code kafka_key} is the job_id (one
 * job's events are partition-ordered) and whose {@code event_id} the
 * protobuf echoes (the consumer dedupe key under at-least-once delivery).
 * <p>
 * Also packages the producer's contract: {@link #descriptorSetBase64()} is
 * the serialized {@link FileDescriptorSet} of jobs.proto and its transitive
 * imports, built from the generated classes' own descriptors, so the
 * protomolt serde validates and frames every record against exactly the
 * schema this service was compiled with — no registry, no drift.
 */
public final class WorkflowRunEventFactory {

    private WorkflowRunEventFactory() {
    }

    /**
     * The submit commit point's event.
     *
     * @param job the accepted job row
     * @return the outbox row to persist in the same transaction
     */
    public static WorkflowRunEventRecord accepted(WorkflowRunRecord job) {
        return record(job, WorkflowRunEventRecord.TYPE_ACCEPTED, WorkflowRunEvent.Type.TYPE_ACCEPTED,
                "", "", "");
    }

    /**
     * A step checkpoint's event.
     *
     * @param job the job row
     * @param step the checkpointed step
     * @return the outbox row to persist in the same transaction
     */
    public static WorkflowRunEventRecord stepCheckpoint(WorkflowRunRecord job, String step) {
        return record(job, WorkflowRunEventRecord.TYPE_STEP_CHECKPOINT,
                WorkflowRunEvent.Type.TYPE_STEP_CHECKPOINT, step, "", "");
    }

    /**
     * The park commit point's event.
     *
     * @param job the job row
     * @param step the external step the job is parked on
     * @return the outbox row to persist in the same transaction
     */
    public static WorkflowRunEventRecord waiting(WorkflowRunRecord job, String step) {
        return record(job, WorkflowRunEventRecord.TYPE_WAITING, WorkflowRunEvent.Type.TYPE_WAITING,
                step, "", "");
    }

    /**
     * The completion commit point's event.
     *
     * @param job the job row
     * @param verdict the one-line completion summary
     * @return the outbox row to persist in the same transaction
     */
    public static WorkflowRunEventRecord completed(WorkflowRunRecord job, String verdict) {
        return record(job, WorkflowRunEventRecord.TYPE_COMPLETED, WorkflowRunEvent.Type.TYPE_COMPLETED,
                "", verdict, "");
    }

    /**
     * The terminal-failure commit point's event (a validation verdict or a
     * non-retryable error).
     *
     * @param job the job row
     * @param step the failing step, or empty
     * @param error the verbatim failure detail
     * @return the outbox row to persist in the same transaction
     */
    public static WorkflowRunEventRecord failed(WorkflowRunRecord job, String step, String error) {
        return record(job, WorkflowRunEventRecord.TYPE_FAILED, WorkflowRunEvent.Type.TYPE_FAILED,
                step == null ? "" : step, "", error);
    }

    /**
     * The dead-letter commit point's event (retries exhausted).
     *
     * @param job the job row
     * @param error the verbatim last error
     * @return the outbox row to persist in the same transaction
     */
    public static WorkflowRunEventRecord dead(WorkflowRunRecord job, String error) {
        return record(job, WorkflowRunEventRecord.TYPE_DEAD, WorkflowRunEvent.Type.TYPE_DEAD,
                "", "", error);
    }

    /**
     * The descriptor set the protomolt serde publishes and consumes against,
     * base64: jobs.proto plus its transitive imports (struct.proto,
     * timestamp.proto), taken from the generated classes' runtime
     * descriptors.
     *
     * @return the serialized FileDescriptorSet, base64-encoded
     */
    public static String descriptorSetBase64() {
        Map<String, com.google.protobuf.DescriptorProtos.FileDescriptorProto> files =
                new LinkedHashMap<>();
        ArrayDeque<FileDescriptor> queue =
                new ArrayDeque<>(java.util.List.of(WorkflowRunEvent.getDescriptor().getFile()));
        while (!queue.isEmpty()) {
            FileDescriptor file = queue.pop();
            if (files.put(file.getName(), file.toProto()) == null) {
                queue.addAll(file.getDependencies());
            }
        }
        return Base64.getEncoder().encodeToString(FileDescriptorSet.newBuilder()
                .addAllFile(files.values())
                .build().toByteArray());
    }

    private static WorkflowRunEventRecord record(WorkflowRunRecord job, String eventType,
            WorkflowRunEvent.Type type, String step, String verdict, String error) {
        UUID eventId = UUID.randomUUID();
        Instant when = Instant.now();
        WorkflowRunEvent.Builder event = WorkflowRunEvent.newBuilder()
                .setEventId(eventId.toString())
                .setJobId(job.jobId.toString())
                .setWorkflowName(job.workflowName == null ? "" : job.workflowName)
                .setType(type)
                .setStep(step)
                .setAttempt(job.attempt)
                .setVerdict(verdict)
                .setError(error)
                .setDetail(Struct.newBuilder()
                        .putFields("status", Value.newBuilder()
                                .setStringValue(job.status == null ? "" : job.status)
                                .build()))
                .setOccurredAt(Timestamp.newBuilder()
                        .setSeconds(when.getEpochSecond()).setNanos(when.getNano()));
        WorkflowRunEventRecord record = new WorkflowRunEventRecord();
        record.eventId = eventId;
        record.eventType = eventType;
        record.payload = event.build().toByteArray();
        record.kafkaKey = job.jobId.toString();
        record.createdAt = when;
        return record;
    }
}
