package ai.pipestream.proto.jobs.service.worker;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.chain.ChainDefinition;
import ai.pipestream.proto.chain.ChainJson;
import ai.pipestream.proto.chain.ChainRepository;
import ai.pipestream.proto.chain.ChainRunner;
import ai.pipestream.proto.chain.ChainVerifier;
import ai.pipestream.proto.jobs.service.ChainJobSubmitter;
import ai.pipestream.proto.jobs.service.ChainJobsConfig;
import ai.pipestream.proto.jobs.service.events.ChainJobEventFactory;
import ai.pipestream.proto.jobs.service.store.ChainJobRecord;
import ai.pipestream.proto.jobs.service.store.ChainJobStore;
import ai.pipestream.proto.jobs.service.store.ChainJobStoreException;
import ai.pipestream.proto.jobs.v1.ChainJobRequest;
import ai.pipestream.proto.json.MalformedProtobufJsonException;
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
 * The chain-jobs worker: {@code workerCount} claim-execute loops on virtual
 * threads. Each loop claims the oldest eligible QUEUED job (one atomic
 * SKIP LOCKED update — concurrent workers never take the same row), executes
 * one segment of its chain with the same {@link ChainRunner} semantics as
 * synchronous {@code run-chain}, persists each step's response to the job
 * row as it lands, and parks, completes, retries, fails, or dead-letters it.
 * <p>
 * <b>Checkpointing is the whole game.</b> A step's response is persisted in
 * the same transaction as its STEP_CHECKPOINT event before the next step
 * runs, so a worker crash (or a lost lease, swept back to QUEUED) resumes at
 * the first missing checkpoint instead of at step zero. The un-persisted
 * in-flight step re-executes: <b>side-effecting steps must be idempotent on
 * {@code job_id + step_name}</b>. A store failure inside the checkpoint
 * observer surfaces as a CHAIN-kind failure whose cause is the store
 * exception; that case requeues (the response may not have persisted), it
 * never completes the job.
 * <p>
 * <b>Retry/verdict/poison semantics</b> (the {@link ChainRunner} contract):
 * retryable = kind GRPC with UNAVAILABLE/DEADLINE_EXCEEDED/
 * RESOURCE_EXHAUSTED, or kind DEADLINE. Retryable failures requeue with
 * exponential backoff ({@code base * 2^(attempt-1)} seconds) until
 * {@code attempt == max_attempts}, then land DEAD with the last error
 * verbatim. VALIDATION is a verdict — the job FAILS with the violations in
 * the record, no retry. GATE/MAPPING/CHAIN are deterministic corruption —
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
 * thread consumes {@link ChainJobRequest} records (group
 * {@value ChainJobsConfig#CONSUMER_GROUP}): each record is a submit, and the
 * offset commits after the row does — at-least-once, idempotent on job_id.
 * An unknown chain name writes a FAILED row loudly; nothing is dropped.
 */
public final class ChainJobWorker implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ChainJobWorker.class);

    private final ChainJobStore store;
    private final ActionContext context;
    private final ChainRepository repository;
    private final ChainRunner runner;
    private final ChainJobsConfig config;
    private final ChainJobSubmitter submitter;
    private final ConcurrentHashMap<String, Semaphore> targetPermits = new ConcurrentHashMap<>();
    private final List<Thread> threads = new CopyOnWriteArrayList<>();
    private volatile boolean closed;

    /**
     * @param store the jobs store (the truth)
     * @param context type resolution and JSON machinery
     * @param repository resolves stored chain names, or null for inline-only
     *        (broker-native submits of unknown names then fail loudly)
     * @param runner the chain executor — injectable for tests (its
     *        ChannelFactory seam is how tests use in-process gRPC)
     * @param config worker/consumer settings
     */
    public ChainJobWorker(ChainJobStore store, ActionContext context,
            ChainRepository repository, ChainRunner runner, ChainJobsConfig config) {
        this.store = Objects.requireNonNull(store, "store");
        this.context = Objects.requireNonNull(context, "context");
        this.repository = repository;
        this.runner = Objects.requireNonNull(runner, "runner");
        this.config = Objects.requireNonNull(config, "config");
        this.submitter = new ChainJobSubmitter(store, repository, config.maxAttemptsDefault());
    }

    /**
     * Start the worker loops (and the request-topic consumer when
     * configured) on virtual threads. Idempotent-ish: call once.
     */
    public void start() {
        for (int i = 0; i < config.workerCount(); i++) {
            Thread thread = Thread.ofVirtual()
                    .name("chain-job-worker-" + config.workerId() + "-" + i)
                    .start(this::claimLoop);
            threads.add(thread);
        }
        if (config.requestTopic() != null) {
            Thread thread = Thread.ofVirtual()
                    .name("chain-job-request-consumer-" + config.workerId())
                    .start(this::consumeLoop);
            threads.add(thread);
        }
        LOG.info("chain-job worker '{}' started: {} loop(s), lease {}, request topic {}",
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
        Optional<ChainJobRecord> claimed = store.claim(config.workerId(), config.leaseDuration());
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
                LOG.error("chain-job worker iteration failed (loop continues): {}",
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
    private void execute(ChainJobRecord job) {
        ChainDefinition definition = parseAndVerify(job);
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
        List<ChainRunner.Checkpoint> prior;
        try {
            prior = rebuildCheckpoints(job, definition);
        } catch (CorruptJobException e) {
            fail(job, "", "CHAIN: " + e.getMessage());
            return;
        }
        List<ChainRunner.Checkpoint> accumulated = new ArrayList<>(prior);
        Semaphore permit = acquirePermit(definition, prior.size());
        try {
            ChainRunner.Segment segment = runner.runSegment(definition, input, prior,
                    checkpoint -> {
                        accumulated.add(checkpoint);
                        // The FULL array so far, in the same transaction as
                        // the event — a crash never leaves a checkpoint
                        // without its event or vice versa.
                        store.saveCheckpoint(job.jobId, checkpointsJson(accumulated),
                                ChainJobEventFactory.stepCheckpoint(job, checkpoint.name()));
                    });
            if (segment instanceof ChainRunner.Segment.Completed completed) {
                String resultJson = context.transcoder().toJson(completed.result().output());
                String verdict = completed.result().steps().size() + " steps, output "
                        + completed.result().output().getDescriptorForType().getFullName();
                store.markCompleted(job.jobId, resultJson, verdict,
                        ChainJobEventFactory.completed(job, verdict));
                LOG.info("chain job {} completed: {}", job.jobId, verdict);
            } else {
                ChainRunner.Segment.Parked parked = (ChainRunner.Segment.Parked) segment;
                store.markWaiting(job.jobId, parked.step(), checkpointsJson(accumulated),
                        ChainJobEventFactory.waiting(job, parked.step()));
                LOG.info("chain job {} parked on external step '{}'", job.jobId, parked.step());
            }
        } catch (ChainRunner.ChainExecutionException e) {
            handleFailure(job, e);
        } finally {
            if (permit != null) {
                permit.release();
            }
        }
    }

    /** Parse + verify the snapshotted definition; null means the job was failed. */
    private ChainDefinition parseAndVerify(ChainJobRecord job) {
        JsonNode tree;
        try {
            tree = context.objectMapper().readTree(job.chainDefinition);
        } catch (Exception e) {
            fail(job, "", "CHAIN: stored chain definition is not readable JSON: "
                    + e.getMessage());
            return null;
        }
        if (!(tree instanceof ObjectNode chainNode)) {
            fail(job, "", "CHAIN: stored chain definition is not a JSON object");
            return null;
        }
        ChainDefinition definition;
        try {
            definition = ChainJson.parse(chainNode, context);
        } catch (ChainJson.ChainParseException e) {
            fail(job, e.step, "CHAIN: stored chain does not parse"
                    + (e.step == null || e.step.isEmpty() ? "" : " (step '" + e.step + "')")
                    + ": " + e.getMessage());
            return null;
        }
        List<ChainVerifier.Finding> findings = new ChainVerifier().verify(definition);
        if (!findings.isEmpty()) {
            StringBuilder detail = new StringBuilder("CHAIN: stored chain does not verify (")
                    .append(findings.size()).append(" finding")
                    .append(findings.size() == 1 ? "" : "s").append(")");
            for (ChainVerifier.Finding finding : findings) {
                detail.append("; [").append(finding.kind()).append("] ")
                        .append(finding.step()).append(": ").append(finding.error());
            }
            fail(job, findings.get(0).step(), detail.toString());
            return null;
        }
        return definition;
    }

    /**
     * Rebuild the resume prefix from the stored checkpoint array: entry i
     * must name step i of the definition (a chain edited under a live job
     * fails loud, matching the runner's own replay check), and each
     * response is parsed against that step's output type.
     */
    private List<ChainRunner.Checkpoint> rebuildCheckpoints(ChainJobRecord job,
            ChainDefinition definition) throws CorruptJobException {
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
                    + ") outnumber the chain's steps (" + definition.steps().size() + ")");
        }
        List<ChainRunner.Checkpoint> prior = new ArrayList<>(array.size());
        for (int i = 0; i < array.size(); i++) {
            JsonNode entry = array.get(i);
            ChainDefinition.Step step = definition.steps().get(i);
            String name = entry.path("name").asText();
            if (!step.name().equals(name)) {
                throw new CorruptJobException("checkpoint " + i + " belongs to step '" + name
                        + "' but the chain's step " + i + " is '" + step.name()
                        + "'; the chain definition changed under the job");
            }
            boolean skipped = entry.path("skipped").asBoolean(false);
            JsonNode response = entry.get("response");
            if (skipped || response == null || response.isNull()) {
                prior.add(new ChainRunner.Checkpoint(name, skipped, null));
                continue;
            }
            try {
                prior.add(new ChainRunner.Checkpoint(name, false,
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
    private void handleFailure(ChainJobRecord job, ChainRunner.ChainExecutionException e) {
        boolean retryable = isRetryable(e);
        String detail = e.kind() + ": " + e.getMessage();
        if (retryable && job.attempt < job.maxAttempts) {
            Duration backoff = backoff(job.attempt);
            store.requeue(job.jobId, backoff);
            LOG.info("chain job {} attempt {}/{} failed retryably ({}); requeued in {}s",
                    job.jobId, job.attempt, job.maxAttempts, detail, backoff.toSeconds());
        } else if (retryable) {
            store.markDead(job.jobId, detail, ChainJobEventFactory.dead(job, detail));
            LOG.warn("chain job {} is DEAD after {} attempt(s): {}",
                    job.jobId, job.attempt, detail);
        } else {
            fail(job, e.step(), detail);
        }
    }

    /**
     * The retry contract: GRPC with UNAVAILABLE/DEADLINE_EXCEEDED/
     * RESOURCE_EXHAUSTED, or DEADLINE — plus the checkpoint-observer case, a
     * CHAIN-kind failure whose cause is the store itself (the step's
     * response may not have persisted; never settle it as failed work).
     */
    private static boolean isRetryable(ChainRunner.ChainExecutionException e) {
        if (e.kind() == ChainRunner.FailureKind.DEADLINE) {
            return true;
        }
        if (e.kind() == ChainRunner.FailureKind.GRPC) {
            return e.grpcCode() == Status.Code.UNAVAILABLE
                    || e.grpcCode() == Status.Code.DEADLINE_EXCEEDED
                    || e.grpcCode() == Status.Code.RESOURCE_EXHAUSTED;
        }
        return e.kind() == ChainRunner.FailureKind.CHAIN
                && e.getCause() instanceof ChainJobStoreException;
    }

    private Duration backoff(int attempt) {
        // attempt is the 1-based counter of the attempt that just failed.
        long shift = Math.min(Math.max(attempt - 1, 0), 20);
        return Duration.ofSeconds(config.backoffBaseSeconds() * (1L << shift));
    }

    private void fail(ChainJobRecord job, String step, String detail) {
        store.markFailed(job.jobId, detail, ChainJobEventFactory.failed(job, step, detail));
        LOG.warn("chain job {} FAILED: {}", job.jobId, detail);
    }

    /**
     * Acquire the per-target permit for the job's next unexecuted step; null
     * when nothing remains but the output mapping (all steps checkpointed).
     */
    private Semaphore acquirePermit(ChainDefinition definition, int nextStepIndex) {
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

    private String checkpointsJson(List<ChainRunner.Checkpoint> checkpoints) {
        ArrayNode array = context.objectMapper().createArrayNode();
        for (ChainRunner.Checkpoint checkpoint : checkpoints) {
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
                    // the runner's CHAIN-kind wrap.
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
                ChainJobEventFactory.descriptorSetBase64(),
                ProtoMoltSerdeConfig.MESSAGE_TYPE,
                ChainJobRequest.getDescriptor().getFullName()), false);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, ChainJobsConfig.CONSUMER_GROUP);
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
            LOG.error("chain-job request consumer died: {}", e.getMessage(), e);
        }
    }

    private void submitRequest(Message value) {
        ChainJobRequest request;
        try {
            request = value instanceof ChainJobRequest typed
                    ? typed
                    : ChainJobRequest.parseFrom(value.toByteArray());
        } catch (Exception e) {
            // Not a ChainJobRequest at all: there is no job_id to key a row
            // on. Log loud and let the batch commit past the poison record.
            LOG.error("dropping an undecodable ChainJobRequest record: {}", e.getMessage(), e);
            return;
        }
        UUID jobId;
        try {
            jobId = UUID.fromString(request.getJobId());
        } catch (IllegalArgumentException e) {
            LOG.error("dropping a ChainJobRequest whose job_id is not a uuid: '{}'",
                    request.getJobId());
            return;
        }
        String inputJson;
        try {
            inputJson = JsonFormat.printer().print(request.getInput());
        } catch (Exception e) {
            failAtBirth(jobId, request.getChainName(), null, "{}",
                    "the request's input Struct is not printable: " + e.getMessage());
            return;
        }
        ChainJobSubmitter.Outcome outcome;
        try {
            outcome = submitter.submit(null, request.getChainName(),
                    context.objectMapper().readTree(inputJson), request.getJobId(), context);
        } catch (Exception e) {
            throw ChainJobStoreException.wrap("broker-native submit of job " + jobId
                    + " failed", e);
        }
        if (!outcome.ok()) {
            // No caller to answer: the failure lands on the row, loudly.
            ObjectNode resolved = repository == null
                    ? null
                    : repository.chain(request.getChainName()).orElse(null);
            failAtBirth(jobId, request.getChainName(), resolved,
                    inputJson, outcome.error());
        }
    }

    /**
     * Write a FAILED row for a request that can never run (unknown chain,
     * unverifiable definition). The row IS the loud failure — a broker-native
     * submitter watches the events topic, and nothing is silently dropped.
     */
    private void failAtBirth(UUID jobId, String chainName, ObjectNode chainDefinition,
            String inputJson, String error) {
        ChainJobRecord record = new ChainJobRecord();
        record.jobId = jobId;
        record.chainName = chainName == null || chainName.isBlank() ? "(unknown)" : chainName;
        record.chainDefinition = chainDefinition == null ? "{}" : chainDefinition.toString();
        record.input = inputJson == null ? "{}" : inputJson;
        record.status = ChainJobRecord.STATUS_FAILED;
        record.error = error;
        record.maxAttempts = config.maxAttemptsDefault();
        record.runAfter = java.time.Instant.now();
        record.completedAt = java.time.Instant.now();
        store.insert(record, ChainJobEventFactory.failed(record, "", error));
        LOG.warn("chain job {} FAILED at birth: {}", jobId, error);
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

    /** The stored checkpoint prefix is corrupt; the job FAILS as CHAIN. */
    private static final class CorruptJobException extends Exception {
        CorruptJobException(String message) {
            super(message);
        }
    }
}
