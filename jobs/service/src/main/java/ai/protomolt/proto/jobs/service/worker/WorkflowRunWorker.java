package ai.protomolt.proto.jobs.service.worker;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.grpc.invoke.ChannelFactory;
import ai.protomolt.proto.grpc.policy.OutboundChannelPolicy;
import ai.protomolt.proto.grpc.profile.ServiceProfileRepository;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.inference.structured.StructuredGenerator;
import ai.protomolt.proto.jobs.service.PipelineJobCoordinator;
import ai.protomolt.proto.jobs.service.PipelineJobCoordinator.PipelineDefinitionException;
import ai.protomolt.proto.jobs.service.WorkflowRunSubmitter;
import ai.protomolt.proto.jobs.service.WorkflowRunsConfig;
import ai.protomolt.proto.jobs.service.events.WorkflowRunEventFactory;
import ai.protomolt.proto.jobs.service.store.ClaimLostException;
import ai.protomolt.proto.jobs.service.store.WorkerClaim;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStore;
import ai.protomolt.proto.jobs.v1.WorkflowRunRequest;
import ai.protomolt.proto.pipeline.PipelineExecutor;
import ai.protomolt.proto.workflow.WorkflowRepository;
import ai.protomolt.proto.workflow.WorkflowRunner;
import com.google.protobuf.Message;
import io.grpc.ChannelCredentials;
import io.grpc.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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

    /**
     * Resolves host-owned call headers and TLS channel credentials for a service endpoint.
     * Unconfigured credential or TLS references fail closed before any channel opens.
     */
    public interface EndpointCredentials {

        /** Fails closed whenever an endpoint names a credential, trust, or client-cert reference. */
        EndpointCredentials NONE = new EndpointCredentials() {
        };

        /**
         * Resolves call headers for {@code endpoint}.
         *
         * @param profile the owning service profile
         * @param endpoint the selected service endpoint
         * @return call metadata headers (empty when uncredentialed)
         */
        default Metadata headers(ServiceProfile profile, ServiceEndpoint endpoint) {
            if (!endpoint.getCredentialRef().isBlank()) {
                throw new IllegalArgumentException("endpoint '" + endpoint.getName()
                        + "' of service profile '" + profile.getName()
                        + "' requires credential_ref resolution, which is not configured");
            }
            return new Metadata();
        }

        /**
         * Resolves custom TLS channel credentials for {@code endpoint}, or {@code null} when the
         * endpoint uses default system trust roots.
         *
         * @param profile the owning service profile
         * @param endpoint the selected service endpoint
         * @return custom TLS channel credentials, or {@code null} when none are referenced
         */
        default ChannelCredentials channelCredentials(ServiceProfile profile,
                                                      ServiceEndpoint endpoint) {
            if (!endpoint.getTrustRef().isBlank()
                    || !endpoint.getClientCertificateRef().isBlank()) {
                throw new IllegalArgumentException("endpoint '" + endpoint.getName()
                        + "' of service profile '" + profile.getName()
                        + "' requires custom TLS credentials, which are not configured");
            }
            return null;
        }
    }

    private final WorkflowRunStore store;
    private final ServiceProfilePipelineTransport pipelineTransport;
    private final WorkflowRunsConfig config;
    private final WorkflowSegmentRunner workflowRunner;
    private final PipelineSegmentRunner pipelineRunner;
    private final WorkflowRequestConsumer requestConsumer;
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
        this(store, context, repository, runner, OutboundChannelPolicy.defaults(),
                null, null, config);
    }

    /**
     * Creates a worker whose durable pipeline execution resolves endpoints from
     * {@code serviceProfiles} and enforces {@code outboundPolicy} on every outbound gRPC call.
     *
     * @param store the jobs store (the truth)
     * @param context type resolution and JSON machinery
     * @param repository resolves stored workflow names, or null for inline-only
     * @param runner the workflow executor
     * @param outboundPolicy host outbound channel policy; null selects
     *        {@link OutboundChannelPolicy#defaults()}
     * @param serviceProfiles resolves pipeline {@link ServiceDependency} entries, or null when
     *        no service workspace is mounted
     * @param structured optional structured-generation coordinator for structured pipeline steps
     * @param config worker/consumer settings
     */
    public WorkflowRunWorker(WorkflowRunStore store, ActionContext context,
            WorkflowRepository repository, WorkflowRunner runner,
            OutboundChannelPolicy outboundPolicy,
            ServiceProfileRepository serviceProfiles,
            StructuredGenerator structured,
            WorkflowRunsConfig config) {
        this(store, context, repository, runner,
                ChannelFactory.standard(outboundPolicy == null
                        ? OutboundChannelPolicy.defaults() : outboundPolicy),
                serviceProfiles, structured, EndpointCredentials.NONE, config);
    }

    /**
     * Creates a worker whose durable pipeline execution uses a policy-enforcing
     * {@link ChannelFactory}, {@code serviceProfiles}, and host-owned {@code credentials}.
     *
     * @param store the jobs store (the truth)
     * @param context type resolution and JSON machinery
     * @param repository resolves stored workflow names, or null for inline-only
     * @param runner the workflow executor
     * @param channelFactory policy-enforcing outbound channel factory
     * @param serviceProfiles resolves pipeline {@link ServiceDependency} entries, or null when
     *        no service workspace is mounted
     * @param structured optional structured-generation coordinator for structured pipeline steps
     * @param credentials resolves host-owned call headers and custom TLS credentials
     * @param config worker/consumer settings
     */
    public WorkflowRunWorker(WorkflowRunStore store, ActionContext context,
            WorkflowRepository repository, WorkflowRunner runner,
            ChannelFactory channelFactory,
            ServiceProfileRepository serviceProfiles,
            StructuredGenerator structured,
            EndpointCredentials credentials,
            WorkflowRunsConfig config) {
        this.store = Objects.requireNonNull(store, "store");
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(runner, "runner");
        this.config = Objects.requireNonNull(config, "config");
        this.pipelineTransport = new ServiceProfilePipelineTransport(
                Objects.requireNonNull(channelFactory, "channelFactory"),
                serviceProfiles,
                Objects.requireNonNull(credentials, "credentials"));
        PipelineExecutor executor = new PipelineExecutor(this.pipelineTransport, structured);
        WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(
                store, repository, config.maxAttemptsDefault());
        this.workflowRunner = new WorkflowSegmentRunner(store, context, runner, this);
        this.pipelineRunner = new PipelineSegmentRunner(store, executor, this);
        this.requestConsumer = new WorkflowRequestConsumer(
                store, context, repository, config, submitter);
    }

    /**
     * Creates a worker with explicit workflow and pipeline executors.
     *
     * @param store the jobs store (the truth)
     * @param context type resolution and JSON machinery
     * @param repository resolves stored workflow names, or null for inline-only
     * @param runner the workflow executor
     * @param pipelineExecutor the pipeline executor used for durable pipeline runs
     * @param config worker/consumer settings
     */
    public WorkflowRunWorker(WorkflowRunStore store, ActionContext context,
            WorkflowRepository repository, WorkflowRunner runner,
            PipelineExecutor pipelineExecutor, WorkflowRunsConfig config) {
        this.store = Objects.requireNonNull(store, "store");
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(runner, "runner");
        Objects.requireNonNull(pipelineExecutor, "pipelineExecutor");
        this.pipelineTransport = null;
        this.config = Objects.requireNonNull(config, "config");
        WorkflowRunSubmitter submitter = new WorkflowRunSubmitter(
                store, repository, config.maxAttemptsDefault());
        this.workflowRunner = new WorkflowSegmentRunner(store, context, runner, this);
        this.pipelineRunner = new PipelineSegmentRunner(store, pipelineExecutor, this);
        this.requestConsumer = new WorkflowRequestConsumer(
                store, context, repository, config, submitter);
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
                    .start(() -> requestConsumer.consumeLoop(() -> closed));
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
        Optional<WorkflowRunRecord> claimed = store.claim(
                config.workerId(), config.leaseDuration());
        if (claimed.isEmpty()) {
            return false;
        }
        execute(claimed.get());
        return true;
    }

    void submitRequest(Message value) {
        requestConsumer.submitRequest(value);
    }

    String resolvePipelineTarget(ServiceDependency dependency) {
        return pipelineTransport != null
                ? pipelineTransport.resolveTargetKey(dependency)
                : ServiceProfilePipelineTransport.staticDependencyKey(dependency);
    }

    Semaphore acquireTargetPermit(String target) {
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

    Duration backoff(int attempt) {
        long shift = Math.min(Math.max(attempt - 1, 0), WorkflowRunsConfig.MAX_BACKOFF_EXPONENT);
        return Duration.ofSeconds(Math.multiplyExact(config.backoffBaseSeconds(), 1L << shift));
    }

    void failJob(WorkflowRunRecord job, WorkerClaim claim, String step, String detail) {
        store.markFailed(claim, detail, WorkflowRunEventFactory.failed(job, step, detail));
        LOG.warn("workflow run {} FAILED: {}", job.jobId, detail);
    }

    private void claimLoop() {
        while (!closed) {
            boolean worked;
            try {
                worked = workOnce();
            } catch (RuntimeException e) {
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

    private void execute(WorkflowRunRecord job) {
        WorkerClaim claim = WorkerClaim.from(job);
        try {
            executeClaimed(job, claim);
        } catch (ClaimLostException lost) {
            LOG.info("workflow run {} lost claim for attempt {} (owner {}); stopping",
                    claim.jobId(), claim.attempt(), claim.leaseOwner());
        }
    }

    private void executeClaimed(WorkflowRunRecord job, WorkerClaim claim) {
        boolean pipelineJob;
        try {
            pipelineJob = PipelineJobCoordinator.isPipelineJob(job);
        } catch (PipelineDefinitionException e) {
            failJob(job, claim, e.step(), "PIPELINE: " + e.getMessage());
            return;
        } catch (IllegalArgumentException e) {
            failJob(job, claim, "", "WORKFLOW: " + e.getMessage());
            return;
        }
        if (pipelineJob) {
            pipelineRunner.execute(job, claim);
            return;
        }
        workflowRunner.execute(job, claim);
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
                break;
            }
        }
        threads.clear();
        if (pipelineTransport != null) {
            pipelineTransport.close();
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
