package ai.pipestream.proto.jobs.service.worker;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.workflow.CompiledWorkflow;
import ai.pipestream.proto.workflow.WorkflowJson;
import ai.pipestream.proto.workflow.WorkflowRepository;
import ai.pipestream.proto.workflow.WorkflowRunner;
import ai.pipestream.proto.workflow.WorkflowVerifier;
import ai.pipestream.proto.jobs.service.WorkflowRunSubmitter;
import ai.pipestream.proto.jobs.service.WorkflowRunsConfig;
import ai.pipestream.proto.jobs.service.events.WorkflowRunEventFactory;
import ai.pipestream.proto.jobs.service.store.WorkflowRunRecord;
import ai.pipestream.proto.jobs.service.store.WorkflowRunStore;
import ai.pipestream.proto.jobs.service.store.WorkflowRunStoreException;
import ai.pipestream.proto.jobs.v1.WorkflowRunRequest;
import ai.pipestream.proto.http.json.MalformedProtobufJsonException;
import ai.pipestream.proto.kafka.serde.ProtoMoltProtobufDeserializer;
import ai.pipestream.proto.kafka.serde.ProtoMoltSerdeConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * The workflow-runs worker: {@code workerCount} claim-execute loops on virtual
 * threads. Each loop claims the oldest eligible QUEUED job (one atomic
 * SKIP LOCKED update — concurrent workers never take the same row), executes
 * one segment of its workflow with the same {@link WorkflowRunner} semantics as
 * synchronous {@code run-workflow}, persists each step's response to the job
 * row as it lands, and parks, completes, retries, fails, or dead-letters it.
 * <p>
 * <b>Checkpointing is the whole game.</b> A step's response is persisted in
 * the same transaction as its STEP_CHECKPOINT event before the next step
 * runs, so a worker crash (or a lost lease, swept back to QUEUED) resumes at
 * the first missing checkpoint instead of at step zero. The un-persisted
 * in-flight step re-executes: <b>side-effecting steps must be idempotent on
 * {@code job_id + step_name}</b>. A store failure inside the checkpoint
 * observer surfaces as a WORKFLOW-kind failure whose cause is the store
 * exception; that case requeues (the response may not have persisted), it
 * never completes the job.
 * <p>
 * <b>Retry/verdict/poison semantics</b> (the {@link WorkflowRunner} contract):
 * retryable = kind GRPC with UNAVAILABLE/DEADLINE_EXCEEDED/
 * RESOURCE_EXHAUSTED, or kind DEADLINE. Retryable failures requeue with
 * exponential backoff ({@code base * 2^(attempt-1)} seconds) until
 * {@code attempt == max_attempts}, then land DEAD with the last error
 * verbatim. VALIDATION is a verdict — the job FAILS with the violations in
 * the record, no retry. GATE/MAPPING/WORKFLOW are deterministic corruption —
 * the job FAILS loud.
 * <p>
 * <b>Per-target concurrency</b> is a documented approximation: a worker
 * acquires a permit (bounded at {@code maxConcurrentPerTarget}) for the
 * target of the job's NEXT unexecuted step before running a segment, and
 * releases it when the segment ends. A segment may call several targets, so
 * the cap throttles the step each job is about to run rather than every call
 * in flight — correct-enough v1 for protecting a one-box inference tier.
 * <p>
 * When the config carries a broker and a request topic, one more virtual
 * thread consumes {@link WorkflowRunRequest} records (group
 * {@value WorkflowRunsConfig#CONSUMER_GROUP}): each record is a submit, and the
 * offset commits after the row does — at-least-once, idempotent on job_id.
 * An unknown workflow name writes a FAILED row loudly; nothing is dropped.
 */
public final class WorkflowRunWorker implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(WorkflowRunWorker.class);

    private final WorkflowRunStore store;
    private final ActionContext context;
    private final WorkflowRepository repository;
    private final WorkflowRunner runner;
    private final WorkflowRunsConfig config;
    private final WorkflowRunSubmitter submitter;
    private final ConcurrentHashMap<String, Semaphore> targetPermits = new ConcurrentHashMap<>();
    private final List<Thread> threads = new CopyOnWriteArrayList<>();
    private volatile boolean closed;

    /**
     * @param store the jobs store (the truth)
     * @param context type resolution and JSON machinery
     * @param repository resolves stored workflow names, or null for inline-only
     *        (broker-native submits of unknown names then fail loudly)
     * @param runner the workflow executor — injectable for tests (its
     *        ChannelFactory seam is how tests use in-process gRPC)
     * @param config worker/consumer settings
     */
    public WorkflowRunWorker(WorkflowRunStore store, ActionContext context,
            WorkflowRepository repository, WorkflowRunner runner, WorkflowRunsConfig config) {
        this.store = Objects.requireNonNull(store, "store");
        this.context = Objects.requireNonNull(context, "context");
        this.repository = repository;
        this.runner = Objects.requireNonNull(runner, "runner");
        this.config = Objects.requireNonNull(config, "config");
        this.submitter = new WorkflowRunSubmitter(store, repository, config.maxAttemptsDefault());
    }

    /**
     * Start the worker loops (and the request-topic consumer when
     * configured) on virtual threads. Idempotent-ish: call once.
     */
    public void start() {
        for (int i = 0; i < config.workerCount(); i++) {
            Thread thread = Thread.ofVirtual()
                    .name("workflow-run-worker-" + config.workerId() + "-" + i)
                    .start(this::claimLoop);
            threads.add(thread);
        }
        if (config.requestTopic() != null) {
            Thread thread = Thread.ofVirtual()
                    .name("workflow-run-request-consumer-" + config.workerId())
                    .start(this::consumeLoop);
            threads.add(thread);
        }
        LOG.info("workflow-run worker '{}' started: {} loop(s), lease {}, request topic {}",
                config.workerId(), config.workerCount(), config.leaseDuration(),
                config.requestTopic() == null ? "(none)" : config.requestTopic());
    }

    /**
     * One claim-execute iteration: claim the oldest eligible job and execute
     * its segment. Exposed for tests; the loops call it forever.
     *
     * @return true when a job was claimed (the loop drains again
     *         immediately), false when none was eligible
     */
    public boolean workOnce() {
        Optional<WorkflowRunRecord> claimed = store.claim(config.workerId(), config.leaseDuration());
        if (claimed.isEmpty()) {
            return false;
        }
        execute(claimed.get());
        return true;
    }

    private void claimLoop() {
        while (!closed) {
            boolean worked;
            try {
                worked = workOnce();
            } catch (RuntimeException e) {
                // A claimed job whose execution blew up unexpectedly keeps
                // its lease; the sweeper requeues it when the lease expires.
                LOG.error("workflow-run worker iteration failed (loop continues): {}",
                        e.getMessage(), e);
                worked = false;
            }
            if (!worked) {
                try {
                    store.requeueExpiredLeases();
                } catch (RuntimeException e) {
                    LOG.warn("lease sweep failed (loop continues): {}", e.getMessage(), e);
                }
                sleep(config.pollInterval().toMillis());
            }
        }
    }

    /**
     * Execute one segment of a claimed job: parse + verify the snapshotted
     * definition, rebuild the checkpoint prefix, run until completion or
     * park, and settle the row.
     */
    private void execute(WorkflowRunRecord job) {
        CompiledWorkflow definition = parseAndVerify(job);
        if (definition == null) {
            return;
        }
        DynamicMessage input;
        try {
            input = context.transcoder().fromJsonDynamic(job.input, definition.inputType());
        } catch (MalformedProtobufJsonException e) {
            fail(job, "", "MAPPING: stored input is not valid proto3 JSON for "
                    + definition.inputType().getFullName() + ": " + e.getMessage());
            return;
        }
        List<WorkflowRunner.Checkpoint> prior;
        try {
            prior = rebuildCheckpoints(job, definition);
        } catch (CorruptJobException e) {
            fail(job, "", "WORKFLOW: " + e.getMessage());
            return;
        }
        List<WorkflowRunner.Checkpoint> accumulated = new ArrayList<>(prior);
        Semaphore permit = acquirePermit(definition, prior.size());
        try {
            WorkflowRunner.Segment segment = runner.runSegment(definition, input, prior,
                    checkpoint -> {
                        accumulated.add(checkpoint);
                        // The FULL array so far, in the same transaction as
                        // the event — a crash never leaves a checkpoint
                        // without its event or vice versa.
                        store.saveCheckpoint(job.jobId, checkpointsJson(accumulated),
                                WorkflowRunEventFactory.stepCheckpoint(job, checkpoint.name()));
                    });
            // Segment is sealed: the switch is exhaustive, so a third outcome
            // would fail the compile rather than fall through to a cast.
            switch (segment) {
                case WorkflowRunner.Segment.Completed completed -> {
                    String resultJson = context.transcoder().toJson(completed.result().output());
                    String verdict = completed.result().steps().size() + " steps, output "
                            + completed.result().output().getDescriptorForType().getFullName();
                    store.markCompleted(job.jobId, resultJson, verdict,
                            WorkflowRunEventFactory.completed(job, verdict));
                    LOG.info("workflow run {} completed: {}", job.jobId, verdict);
                }
                case WorkflowRunner.Segment.Parked parked -> {
                    store.markWaiting(job.jobId, parked.step(), checkpointsJson(accumulated),
                            WorkflowRunEventFactory.waiting(job, parked.step()));
                    LOG.info("workflow run {} parked on external step '{}'",
                            job.jobId, parked.step());
                }
            }
        } catch (WorkflowRunner.WorkflowExecutionException e) {
            handleFailure(job, e);
        } finally {
            if (permit != null) {
                permit.release();
            }
        }
    }

    /** Parse + verify the snapshotted definition; null means the job was failed. */
    private CompiledWorkflow parseAndVerify(WorkflowRunRecord job) {
        JsonNode tree;
        try {
            tree = context.objectMapper().readTree(job.workflowDefinition);
        } catch (Exception e) {
            fail(job, "", "WORKFLOW: stored workflow definition is not readable JSON: "
                    + e.getMessage());
            return null;
        }
        if (!(tree instanceof ObjectNode workflowNode)) {
            fail(job, "", "WORKFLOW: stored workflow definition is not a JSON object");
            return null;
        }
        CompiledWorkflow definition;
        try {
            definition = WorkflowJson.parse(workflowNode, context);
        } catch (WorkflowJson.WorkflowParseException e) {
            fail(job, e.step, "WORKFLOW: stored workflow does not parse"
                    + (e.step == null || e.step.isEmpty() ? "" : " (step '" + e.step + "')")
                    + ": " + e.getMessage());
            return null;
        }
        List<WorkflowVerifier.Finding> findings = new WorkflowVerifier().verify(definition);
        if (!findings.isEmpty()) {
            StringBuilder detail = new StringBuilder("WORKFLOW: stored workflow does not verify (")
                    .append(findings.size()).append(" finding")
                    .append(findings.size() == 1 ? "" : "s").append(")");
            for (WorkflowVerifier.Finding finding : findings) {
                detail.append("; [").append(finding.kind()).append("] ")
                        .append(finding.step()).append(": ").append(finding.error());
            }
            fail(job, findings.getFirst().step(), detail.toString());
            return null;
        }
        return definition;
    }

    /**
     * Rebuild the resume prefix from the stored checkpoint array: entry i
     * must name step i of the definition (a workflow edited under a live job
     * fails loud, matching the runner's own replay check), and each
     * response is parsed against that step's output type.
     */
    private List<WorkflowRunner.Checkpoint> rebuildCheckpoints(WorkflowRunRecord job,
            CompiledWorkflow definition) throws CorruptJobException {
        JsonNode tree;
        try {
            tree = context.objectMapper().readTree(
                    job.checkpoints == null ? "[]" : job.checkpoints);
        } catch (Exception e) {
            throw new CorruptJobException("stored checkpoints are not readable JSON: "
                    + e.getMessage());
        }
        if (!(tree instanceof ArrayNode array)) {
            throw new CorruptJobException("stored checkpoints are not an array");
        }
        if (array.size() > definition.steps().size()) {
            throw new CorruptJobException("stored checkpoints (" + array.size()
                    + ") outnumber the workflow's steps (" + definition.steps().size() + ")");
        }
        List<WorkflowRunner.Checkpoint> prior = new ArrayList<>(array.size());
        for (int i = 0; i < array.size(); i++) {
            JsonNode entry = array.get(i);
            CompiledWorkflow.Step step = definition.steps().get(i);
            String name = entry.path("name").asText();
            if (!step.name().equals(name)) {
                throw new CorruptJobException("checkpoint " + i + " belongs to step '" + name
                        + "' but the workflow's step " + i + " is '" + step.name()
                        + "'; the workflow definition changed under the job");
            }
            boolean skipped = entry.path("skipped").asBoolean(false);
            JsonNode response = entry.get("response");
            if (skipped || response == null || response.isNull()) {
                prior.add(new WorkflowRunner.Checkpoint(name, skipped, null));
                continue;
            }
            try {
                prior.add(new WorkflowRunner.Checkpoint(name, false,
                        context.transcoder().fromJsonDynamic(response.toString(),
                                step.method().getOutputType())));
            } catch (MalformedProtobufJsonException e) {
                throw new CorruptJobException("checkpoint '" + name + "' response is not "
                        + "valid proto3 JSON for " + step.method().getOutputType().getFullName()
                        + ": " + e.getMessage());
            }
        }
        return prior;
    }

    /**
     * The failure settlement: retryable-with-attempts-left requeues with
     * exponential backoff; retryable-exhausted lands DEAD; everything else
     * FAILS — VALIDATION being the verdict path, its violations the error.
     */
    private void handleFailure(WorkflowRunRecord job, WorkflowRunner.WorkflowExecutionException e) {
        boolean retryable = isRetryable(e);
        String detail = e.kind() + ": " + e.getMessage();
        if (retryable && job.attempt < job.maxAttempts) {
            Duration backoff = backoff(job.attempt);
            store.requeue(job.jobId, backoff);
            LOG.info("workflow run {} attempt {}/{} failed retryably ({}); requeued in {}s",
                    job.jobId, job.attempt, job.maxAttempts, detail, backoff.toSeconds());
        } else if (retryable) {
            store.markDead(job.jobId, detail, WorkflowRunEventFactory.dead(job, detail));
            LOG.warn("workflow run {} is DEAD after {} attempt(s): {}",
                    job.jobId, job.attempt, detail);
        } else {
            fail(job, e.step(), detail);
        }
    }

    /**
     * The retry contract: GRPC with UNAVAILABLE/DEADLINE_EXCEEDED/
     * RESOURCE_EXHAUSTED, or DEADLINE — plus the checkpoint-observer case, a
     * WORKFLOW-kind failure whose cause is the store itself (the step's
     * response may not have persisted; never settle it as failed work).
     */
    private static boolean isRetryable(WorkflowRunner.WorkflowExecutionException e) {
        if (e.kind() == WorkflowRunner.FailureKind.DEADLINE) {
            return true;
        }
        if (e.kind() == WorkflowRunner.FailureKind.GRPC) {
            return e.grpcCode() == Status.Code.UNAVAILABLE
                    || e.grpcCode() == Status.Code.DEADLINE_EXCEEDED
                    || e.grpcCode() == Status.Code.RESOURCE_EXHAUSTED;
        }
        return e.kind() == WorkflowRunner.FailureKind.WORKFLOW
                && e.getCause() instanceof WorkflowRunStoreException;
    }

    private Duration backoff(int attempt) {
        // attempt is the 1-based counter of the attempt that just failed.
        long shift = Math.min(Math.max(attempt - 1, 0), 20);
        return Duration.ofSeconds(config.backoffBaseSeconds() * (1L << shift));
    }

    private void fail(WorkflowRunRecord job, String step, String detail) {
        store.markFailed(job.jobId, detail, WorkflowRunEventFactory.failed(job, step, detail));
        LOG.warn("workflow run {} FAILED: {}", job.jobId, detail);
    }

    /**
     * Acquire the per-target permit for the job's next unexecuted step; null
     * when nothing remains but the output mapping (all steps checkpointed).
     */
    private Semaphore acquirePermit(CompiledWorkflow definition, int nextStepIndex) {
        if (nextStepIndex >= definition.steps().size()) {
            return null;
        }
        String target = definition.steps().get(nextStepIndex).target();
        Semaphore semaphore = targetPermits.computeIfAbsent(target,
                key -> new Semaphore(config.maxConcurrentPerTarget()));
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted acquiring the per-target permit "
                    + "for " + target, e);
        }
        return semaphore;
    }

    private String checkpointsJson(List<WorkflowRunner.Checkpoint> checkpoints) {
        ArrayNode array = context.objectMapper().createArrayNode();
        for (WorkflowRunner.Checkpoint checkpoint : checkpoints) {
            ObjectNode entry = array.addObject();
            entry.put("name", checkpoint.name());
            entry.put("skipped", checkpoint.skipped());
            if (checkpoint.response() != null) {
                try {
                    entry.set("response", context.objectMapper()
                            .readTree(context.transcoder().toJson(checkpoint.response())));
                } catch (Exception e) {
                    // A response the transcoder just produced must re-parse;
                    // reaching here is a serialization bug — fail loud via
                    // the runner's WORKFLOW-kind wrap.
                    throw new IllegalStateException("could not render the checkpoint for '"
                            + checkpoint.name() + "' as JSON", e);
                }
            }
        }
        return array.toString();
    }

    // ---- the request-topic consumer (broker-native submission) ----

    private void consumeLoop() {
        ProtoMoltProtobufDeserializer deserializer = new ProtoMoltProtobufDeserializer();
        deserializer.configure(Map.of(
                ProtoMoltSerdeConfig.DESCRIPTOR_SET_BASE64,
                WorkflowRunEventFactory.descriptorSetBase64(),
                ProtoMoltSerdeConfig.MESSAGE_TYPE,
                WorkflowRunRequest.getDescriptor().getFullName()), false);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, WorkflowRunsConfig.CONSUMER_GROUP);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, Message> consumer =
                new KafkaConsumer<>(props, new StringDeserializer(), deserializer)) {
            consumer.subscribe(List.of(config.requestTopic()));
            while (!closed) {
                ConsumerRecords<String, Message> records =
                        consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, Message> record : records) {
                    submitRequest(record.value());
                }
                if (!records.isEmpty()) {
                    // The offset commits only after every row in the batch
                    // did — at-least-once; the insert is idempotent on
                    // job_id, so a redelivered request returns the existing
                    // job.
                    consumer.commitSync();
                }
            }
        } catch (RuntimeException e) {
            // A poison record or a dead store kills the consumer thread, not
            // the worker: the claim loops keep serving verb-submitted jobs,
            // and the broker retains the unconsumed records. Fail loud.
            LOG.error("workflow-run request consumer died: {}", e.getMessage(), e);
        }
    }

    private void submitRequest(Message value) {
        WorkflowRunRequest request;
        try {
            request = value instanceof WorkflowRunRequest typed
                    ? typed
                    : WorkflowRunRequest.parseFrom(value.toByteArray());
        } catch (Exception e) {
            // Not a WorkflowRunRequest at all: there is no job_id to key a row
            // on. Log loud and let the batch commit past the poison record.
            LOG.error("dropping an undecodable WorkflowRunRequest record: {}", e.getMessage(), e);
            return;
        }
        UUID jobId;
        try {
            jobId = UUID.fromString(request.getJobId());
        } catch (IllegalArgumentException e) {
            LOG.error("dropping a WorkflowRunRequest whose job_id is not a uuid: '{}'",
                    request.getJobId());
            return;
        }
        String inputJson;
        try {
            inputJson = JsonFormat.printer().print(request.getInput());
        } catch (Exception e) {
            failAtBirth(jobId, request.getWorkflowName(), null, "{}",
                    "the request's input Struct is not printable: " + e.getMessage());
            return;
        }
        WorkflowRunSubmitter.Outcome outcome;
        try {
            outcome = submitter.submit(null, request.getWorkflowName(),
                    context.objectMapper().readTree(inputJson), request.getJobId(), context);
        } catch (Exception e) {
            throw WorkflowRunStoreException.wrap("broker-native submit of job " + jobId
                    + " failed", e);
        }
        if (!outcome.ok()) {
            // No caller to answer: the failure lands on the row, loudly.
            ObjectNode resolved = repository == null
                    ? null
                    : repository.workflow(request.getWorkflowName()).orElse(null);
            failAtBirth(jobId, request.getWorkflowName(), resolved,
                    inputJson, outcome.error());
        }
    }

    /**
     * Write a FAILED row for a request that can never run (unknown workflow,
     * unverifiable definition). The row IS the loud failure — a broker-native
     * submitter watches the events topic, and nothing is silently dropped.
     */
    private void failAtBirth(UUID jobId, String workflowName, ObjectNode workflowDefinition,
            String inputJson, String error) {
        WorkflowRunRecord record = new WorkflowRunRecord();
        record.jobId = jobId;
        record.workflowName = workflowName == null || workflowName.isBlank() ? "(unknown)" : workflowName;
        record.workflowDefinition = workflowDefinition == null ? "{}" : workflowDefinition.toString();
        record.input = inputJson == null ? "{}" : inputJson;
        record.status = WorkflowRunRecord.STATUS_FAILED;
        record.error = error;
        record.maxAttempts = config.maxAttemptsDefault();
        record.runAfter = java.time.Instant.now();
        record.completedAt = java.time.Instant.now();
        store.insert(record, WorkflowRunEventFactory.failed(record, "", error));
        LOG.warn("workflow run {} FAILED at birth: {}", jobId, error);
    }

    /** Stop every loop; the store's lifecycle stays with the caller. */
    @Override
    public void close() {
        closed = true;
        for (Thread thread : threads) {
            thread.interrupt();
        }
        for (Thread thread : threads) {
            try {
                thread.join(TimeUnit.SECONDS.toMillis(10));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        threads.clear();
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** The stored checkpoint prefix is corrupt; the job FAILS as WORKFLOW. */
    private static final class CorruptJobException extends Exception {
        CorruptJobException(String message) {
            super(message);
        }
    }
}
