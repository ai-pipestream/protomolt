package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.descriptors.DescriptorIdentity;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.runtime.v1.DeploymentPointer;
import ai.protomolt.proto.mesh.runtime.v1.DurableFlowRun;
import ai.protomolt.proto.mesh.runtime.v1.DurableRunState;
import ai.protomolt.proto.mesh.runtime.v1.FlowDefinition;
import ai.protomolt.proto.mesh.runtime.v1.FlowExecutionCheckpoint;
import ai.protomolt.proto.mesh.runtime.v1.FlowFailure;
import ai.protomolt.proto.mesh.runtime.v1.FlowHistory;
import ai.protomolt.proto.mesh.runtime.v1.FlowValidationFinding;
import ai.protomolt.proto.mesh.runtime.v1.FlowValidationReport;
import ai.protomolt.proto.mesh.runtime.v1.HistoryEvent;
import ai.protomolt.proto.mesh.runtime.v1.HistoryEventKind;
import ai.protomolt.proto.mesh.runtime.v1.PendingFlowMessage;
import ai.protomolt.proto.mesh.runtime.v1.PublishFlowResponse;
import ai.protomolt.proto.mesh.runtime.v1.PublishedFlowVersion;
import ai.protomolt.proto.mesh.runtime.v1.RunState;
import ai.protomolt.proto.mesh.runtime.v1.RunTransition;
import ai.protomolt.proto.mesh.v1.EntityEnvelope;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.regex.Pattern;

/**
 * Product lifecycle for immutable directed workflows and restart-safe runs.
 * The store is the authority; this coordinator never maintains a side history.
 */
public final class DurableFlowCoordinator {

    private static final Pattern SLUG = Pattern.compile("[a-z][a-z0-9-]{0,127}");

    private final DescriptorRegistry descriptors;
    private final FlowCompiler compiler;
    private final FlowLifecycleStore store;
    private final PayloadResolver payloads;
    private final PayloadLifecycle payloadLifecycle;
    private final Clock clock;
    private final Map<String, Thread> activeRuns = new HashMap<>();

    public DurableFlowCoordinator(
            DescriptorRegistry descriptors,
            ProcessorRegistry processors,
            FlowLifecycleStore store) {
        this(descriptors, processors, store,
                PayloadResolver.inlineOnly(descriptors), PayloadLifecycle.inlineOnly(),
                Clock.systemUTC());
    }

    public DurableFlowCoordinator(
            DescriptorRegistry descriptors,
            ProcessorRegistry processors,
            FlowLifecycleStore store,
            PayloadResolver payloads,
            Clock clock) {
        this(descriptors, processors, store, payloads,
                PayloadLifecycle.inlineOnly(), clock);
    }

    public DurableFlowCoordinator(
            DescriptorRegistry descriptors,
            ProcessorRegistry processors,
            FlowLifecycleStore store,
            PayloadResolver payloads,
            PayloadLifecycle payloadLifecycle,
            Clock clock) {
        this(descriptors, processors, store, payloads, payloadLifecycle,
                ChannelResourceCatalog.builtIns(), clock);
    }

    public DurableFlowCoordinator(
            DescriptorRegistry descriptors,
            ProcessorRegistry processors,
            FlowLifecycleStore store,
            PayloadResolver payloads,
            PayloadLifecycle payloadLifecycle,
            ChannelResourceCatalog channelResources,
            Clock clock) {
        this.descriptors = Objects.requireNonNull(descriptors, "descriptors");
        this.compiler = new FlowCompiler(descriptors,
                Objects.requireNonNull(processors, "processors"),
                Objects.requireNonNull(channelResources, "channelResources"));
        this.store = Objects.requireNonNull(store, "store");
        this.payloads = Objects.requireNonNull(payloads, "payloads");
        this.payloadLifecycle = Objects.requireNonNull(payloadLifecycle, "payloadLifecycle");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** Returns a bounded validation report without writing lifecycle state. */
    public FlowValidationReport validate(String version, FlowDefinition definition) {
        Objects.requireNonNull(definition, "definition");
        String fingerprint = DescriptorIdentity.sha256(
                FlowCompiler.deterministicBytes(definition));
        String reportName = validSlug(definition.getName())
                ? definition.getName() : "invalid-flow";
        String reportVersion = validSlug(version) ? version : "invalid-version";
        FlowValidationReport.Builder report = FlowValidationReport.newBuilder()
                .setWorkflowName(reportName)
                .setVersion(reportVersion)
                .setDefinitionFingerprint(fingerprint);
        if (!validSlug(version)) {
            return report.setValid(false)
                    .addFindings(finding("invalid-version", "version",
                            "version must be a lowercase slug of at most 128 characters"))
                    .build();
        }
        try {
            CompiledDirectedFlow compiled = compiler.compile(definition);
            return report.setWorkflowName(definition.getName())
                    .setVersion(version)
                    .setValid(true)
                    .setPlan(compiled.plan())
                    .build();
        } catch (RuntimeException failure) {
            return report.setValid(false)
                    .addFindings(finding("flow-invalid", "definition",
                            bounded(failure.getMessage(), failure.getClass().getSimpleName())))
                    .build();
        }
    }

    /** Compiles and immutably publishes one exact version when validation succeeds. */
    public PublishFlowResponse publish(String version, FlowDefinition definition) {
        FlowValidationReport validation = validate(version, definition);
        PublishFlowResponse.Builder response = PublishFlowResponse.newBuilder()
                .setValidation(validation);
        if (!validation.getValid()) {
            return response.build();
        }
        PublishedFlowVersion existing = store.published(
                validation.getWorkflowName(), version).orElse(null);
        if (existing != null) {
            if (!existing.getPlan().equals(validation.getPlan())) {
                throw new LifecycleConflictException("published workflow version is immutable: "
                        + validation.getWorkflowName() + "/" + version);
            }
            return response.setPublished(existing).build();
        }
        PublishedFlowVersion published = PublishedFlowVersion.newBuilder()
                .setWorkflowName(validation.getWorkflowName())
                .setVersion(version)
                .setPlan(validation.getPlan())
                .setPublishedAt(timestamp(clock.instant()))
                .build();
        try {
            return response.setPublished(store.publish(published)).build();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot publish workflow "
                    + validation.getWorkflowName() + "/" + version, e);
        }
    }

    public DeploymentPointer deploy(
            String workflowName, String version, OptionalLong expectedRevision) {
        try {
            return store.deploy(workflowName, version,
                    Objects.requireNonNull(expectedRevision, "expectedRevision"),
                    clock.instant());
        } catch (IOException e) {
            throw new UncheckedIOException("cannot deploy workflow "
                    + workflowName + "/" + version, e);
        }
    }

    /** Creates the durable run before invoking a processor, then executes it. */
    public DurableFlowRun start(
            String workflowName, String runId, EntityEnvelope input) {
        Objects.requireNonNull(input, "input");
        DurableFlowRun existing = store.run(runId).orElse(null);
        if (existing != null) {
            if (!existing.getWorkflowName().equals(workflowName)
                    || !existing.getInput().equals(input)) {
                throw new LifecycleConflictException(
                        "run_id already names different work: " + runId);
            }
            return execute(existing);
        }
        DeploymentPointer deployment = store.deployment(workflowName)
                .orElseThrow(() -> new LifecycleNotFoundException(
                        "workflow has no deployment pointer: " + workflowName));
        PublishedFlowVersion published = requirePublished(
                workflowName, deployment.getVersion());
        compiler.restore(published.getPlan());
        Instant now = clock.instant();
        FlowHistory history = FlowHistory.newBuilder()
                .setRunId(runId)
                .setFlowName(workflowName)
                .setPlanFingerprint(published.getPlan().getPlanFingerprint())
                .setState(RunState.RUN_STATE_RUNNING)
                .build();
        DurableFlowRun created = DurableFlowRun.newBuilder()
                .setRunId(runId)
                .setWorkflowName(workflowName)
                .setWorkflowVersion(published.getVersion())
                .setPlanFingerprint(published.getPlan().getPlanFingerprint())
                .setDeploymentRevision(deployment.getRevision())
                .setInput(input)
                .setState(DurableRunState.DURABLE_RUN_STATE_RUNNING)
                .setStorageRevision(1)
                .setCreatedAt(timestamp(now))
                .setUpdatedAt(timestamp(now))
                .setHistory(history)
                .build();
        try {
            return execute(store.createRun(created));
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create durable run " + runId, e);
        }
    }

    /** Resumes one non-terminal run from its persisted active invocation or queue. */
    public DurableFlowRun resume(String runId) {
        DurableFlowRun run = requireRun(runId);
        return execute(run);
    }

    /** Resumes every run the WAL recovered as non-terminal, in creation order. */
    public List<DurableFlowRun> resumeIncomplete() {
        List<DurableFlowRun> results = new ArrayList<>();
        for (DurableFlowRun run : store.incompleteRuns()) {
            results.add(execute(run));
        }
        return List.copyOf(results);
    }

    /** Persists cancellation before an executor observes or acts on it. */
    public DurableFlowRun cancel(String runId, String reason) {
        String boundedReason = bounded(reason, "run cancellation requested");
        while (true) {
            DurableFlowRun current = requireRun(runId);
            if (FileFlowLifecycleStore.terminal(current.getState())
                    || current.getState()
                    == DurableRunState.DURABLE_RUN_STATE_CANCELLATION_REQUESTED) {
                return current;
            }
            if (current.getCheckpoint().getSettlementStarted()) {
                throw new LifecycleConflictException("run " + runId
                        + " has crossed the descendant-settlement commit boundary");
            }
            Instant now = clock.instant();
            HistoryEvent event = HistoryEvent.newBuilder()
                    .setSequence(current.getHistory().getEventsCount() + 1L)
                    .setOccurredAt(timestamp(now))
                    .setKind(HistoryEventKind.HISTORY_EVENT_KIND_CANCELLATION_REQUESTED)
                    .setFailure(FlowFailure.newBuilder()
                            .setCode("cancellation-requested")
                            .setMessage(boundedReason))
                    .build();
            RunTransition transition = RunTransition.newBuilder()
                    .setRunId(runId)
                    .setExpectedStorageRevision(current.getStorageRevision())
                    .setState(DurableRunState.DURABLE_RUN_STATE_CANCELLATION_REQUESTED)
                    .addEvents(event)
                    .setCheckpoint(current.getCheckpoint())
                    .setCancellationReason(boundedReason)
                    .setUpdatedAt(timestamp(now))
                    .build();
            try {
                DurableFlowRun cancelled = store.transition(transition);
                Thread executor;
                synchronized (activeRuns) {
                    executor = activeRuns.get(runId);
                }
                if (executor != null && executor != Thread.currentThread()) {
                    executor.interrupt();
                }
                return cancelled;
            } catch (LifecycleConflictException retry) {
                // A processor checkpoint won the race; retry against its exact revision.
            } catch (IOException e) {
                throw new UncheckedIOException("cannot cancel durable run " + runId, e);
            }
        }
    }

    /**
     * Creates a new run from a strictly ordered set of persisted routed-message
     * events. The original version and plan remain pinned even after redeployment.
     */
    public DurableFlowRun replay(
            String sourceRunId, String runId, List<Long> frontierSequences) {
        Objects.requireNonNull(frontierSequences, "frontierSequences");
        if (frontierSequences.isEmpty() || frontierSequences.size() > 1024) {
            throw new IllegalArgumentException(
                    "frontier replay requires 1 to 1024 history sequences");
        }
        DurableFlowRun existing = store.run(runId).orElse(null);
        if (existing != null) {
            if (!existing.getReplayOfRunId().equals(sourceRunId)
                    || !existing.getReplayFrontierSequencesList()
                    .equals(frontierSequences)) {
                throw new LifecycleConflictException(
                        "run_id already names different replay work: " + runId);
            }
            return execute(existing);
        }
        DurableFlowRun source = requireRun(sourceRunId);
        PublishedFlowVersion published = requirePublished(
                source.getWorkflowName(), source.getWorkflowVersion());
        CompiledDirectedFlow compiled = compiler.restore(published.getPlan());
        List<HistoryEvent> frontier = resolveFrontier(source, frontierSequences);
        Instant now = clock.instant();
        Instant deadline = deadline(published.getPlan().getDefinition(), now);
        FlowHistory.Builder history = FlowHistory.newBuilder()
                .setRunId(runId)
                .setFlowName(source.getWorkflowName())
                .setPlanFingerprint(source.getPlanFingerprint())
                .setState(RunState.RUN_STATE_RUNNING)
                .setReplayOfRunId(sourceRunId)
                .addAllReplayFrontierSequences(frontierSequences);
        history.addEvents(baseEvent(1, now,
                HistoryEventKind.HISTORY_EVENT_KIND_RUN_STARTED));
        history.addEvents(baseEvent(2, now,
                HistoryEventKind.HISTORY_EVENT_KIND_REPLAY_REQUESTED));
        history.addEvents(baseEvent(3, now,
                HistoryEventKind.HISTORY_EVENT_KIND_REPLAY_STARTED));
        FlowExecutionCheckpoint.Builder checkpoint = FlowExecutionCheckpoint.newBuilder()
                .setDeadline(timestamp(deadline));
        long sequence = 3;
        for (int index = 0; index < frontier.size(); index++) {
            HistoryEvent original = frontier.get(index);
            EntityEnvelope replayed = EntityEnvelopes.replayRoot(
                    runId, frontierSequences.get(index), original.getMessage(), now, deadline);
            boolean existingClaimCheck = replayed.hasClaimCheck();
            CompiledDirectedFlow.EdgeBinding edge = compiled.edge(original.getEdgeId());
            PayloadExternalizer.Externalized staged = payloadLifecycle.stage(
                    replayed, edge.channelPolicy(), replayed.getHeader().getScopeId(),
                    "flow:" + runId + ":" + original.getEdgeId() + ":"
                            + replayed.getHeader().getEntityId(),
                    deadline);
            replayed = staged.envelope();
            if (staged.lease() != null) {
                checkpoint.addPayloadLeases(staged.lease());
                history.addEvents(HistoryEvent.newBuilder()
                        .setSequence(++sequence)
                        .setOccurredAt(timestamp(now))
                        .setKind(existingClaimCheck
                                ? HistoryEventKind.HISTORY_EVENT_KIND_PAYLOAD_RETAINED
                                : HistoryEventKind.HISTORY_EVENT_KIND_PAYLOAD_EXTERNALIZED)
                        .setNodeId(original.getNodeId())
                        .setProcessorId(original.getProcessorId())
                        .setEdgeId(original.getEdgeId())
                        .setMessageId(replayed.getHeader().getEntityId())
                        .setSchema(replayed.getSchema())
                        .setMessage(replayed));
            }
            long replaySequence = ++sequence;
            checkpoint.addPending(PendingFlowMessage.newBuilder()
                    .setNodeId(original.getNodeId())
                    .setEdgeId(original.getEdgeId())
                    .setInput(replayed)
                    .setSourceHistorySequence(replaySequence));
            history.addEvents(HistoryEvent.newBuilder()
                    .setSequence(replaySequence)
                    .setOccurredAt(timestamp(now))
                    .setKind(HistoryEventKind.HISTORY_EVENT_KIND_MESSAGE_ROUTED)
                    .setNodeId(original.getNodeId())
                    .setProcessorId(original.getProcessorId())
                    .setEdgeId(original.getEdgeId())
                    .setMessageId(replayed.getHeader().getEntityId())
                    .setSchema(replayed.getSchema())
                    .setMessage(replayed));
        }
        EntityEnvelope replayInput = EntityEnvelopes.replayRoot(
                runId, 0, source.getInput(), now, deadline);
        DurableFlowRun created = DurableFlowRun.newBuilder()
                .setRunId(runId)
                .setWorkflowName(source.getWorkflowName())
                .setWorkflowVersion(source.getWorkflowVersion())
                .setPlanFingerprint(source.getPlanFingerprint())
                .setDeploymentRevision(source.getDeploymentRevision())
                .setInput(replayInput)
                .setState(DurableRunState.DURABLE_RUN_STATE_RUNNING)
                .setStorageRevision(1)
                .setCreatedAt(timestamp(now))
                .setUpdatedAt(timestamp(now))
                .setHistory(history)
                .setCheckpoint(checkpoint)
                .setReplayOfRunId(sourceRunId)
                .addAllReplayFrontierSequences(frontierSequences)
                .build();
        try {
            return execute(store.createRun(created));
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create frontier replay " + runId, e);
        }
    }

    public DurableFlowRun get(String runId) {
        return requireRun(runId);
    }

    public FlowLifecycleStore.HistoryPage history(
            String runId, long afterSequence, int limit) {
        return store.history(runId, afterSequence, limit);
    }

    public PublishedFlowVersion published(String workflowName, String version) {
        return requirePublished(workflowName, version);
    }

    public DeploymentPointer deployment(String workflowName) {
        return store.deployment(workflowName)
                .orElseThrow(() -> new LifecycleNotFoundException(
                        "workflow has no deployment pointer: " + workflowName));
    }

    private DurableFlowRun execute(DurableFlowRun run) {
        if (FileFlowLifecycleStore.terminal(run.getState())) {
            return run;
        }
        synchronized (activeRuns) {
            if (activeRuns.containsKey(run.getRunId())) {
                return requireRun(run.getRunId());
            }
            activeRuns.put(run.getRunId(), Thread.currentThread());
        }
        try {
            DurableFlowRun current = requireRun(run.getRunId());
            PublishedFlowVersion published = requirePublished(
                    current.getWorkflowName(), current.getWorkflowVersion());
            CompiledDirectedFlow flow = compiler.restore(published.getPlan());
            StoreRunControl control = new StoreRunControl(current);
            try {
                new FlowRuntime(descriptors, payloads, clock,
                        new FlowRunMetadata(current.getWorkflowVersion(),
                                current.getDeploymentRevision()), payloadLifecycle).resume(
                        flow, current.getInput(), current.getRunId(),
                        current.getHistory(), current.getCheckpoint(), control);
            } catch (FlowCancellationException | FlowExecutionException terminal) {
                // The terminal transition was forced before the exception escaped.
            }
            return requireRun(current.getRunId());
        } finally {
            synchronized (activeRuns) {
                activeRuns.remove(run.getRunId(), Thread.currentThread());
            }
        }
    }

    private List<HistoryEvent> resolveFrontier(
            DurableFlowRun source, List<Long> sequences) {
        long previous = 0;
        List<HistoryEvent> result = new ArrayList<>(sequences.size());
        for (long sequence : sequences) {
            if (sequence <= previous) {
                throw new IllegalArgumentException(
                        "frontier sequences must be strictly increasing");
            }
            previous = sequence;
            HistoryEvent event = source.getHistory().getEventsList().stream()
                    .filter(candidate -> candidate.getSequence() == sequence)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(
                            "source run has no history sequence " + sequence));
            if (event.getKind() != HistoryEventKind.HISTORY_EVENT_KIND_MESSAGE_ROUTED
                    || !event.hasMessage()
                    || event.getNodeId().isBlank()
                    || event.getEdgeId().isBlank()) {
                throw new IllegalArgumentException("history sequence " + sequence
                        + " is not a replayable routed-message frontier");
            }
            result.add(event);
        }
        return List.copyOf(result);
    }

    private PublishedFlowVersion requirePublished(String workflowName, String version) {
        return store.published(workflowName, version)
                .orElseThrow(() -> new LifecycleNotFoundException(
                        "unknown published workflow " + workflowName + "/" + version));
    }

    private DurableFlowRun requireRun(String runId) {
        return store.run(runId)
                .orElseThrow(() -> new LifecycleNotFoundException(
                        "unknown run_id " + runId));
    }

    private static List<HistoryEvent> appended(
            FlowHistory durable, FlowHistory next) {
        if (next.getEventsCount() < durable.getEventsCount()) {
            throw new IllegalArgumentException("history cannot discard persisted events");
        }
        for (int index = 0; index < durable.getEventsCount(); index++) {
            if (!durable.getEvents(index).equals(next.getEvents(index))) {
                throw new LifecycleConflictException(
                        "history diverged from persisted sequence " + (index + 1));
            }
        }
        return next.getEventsList().subList(
                durable.getEventsCount(), next.getEventsCount());
    }

    private static DurableRunState durableState(RunState state) {
        return switch (state) {
            case RUN_STATE_RUNNING -> DurableRunState.DURABLE_RUN_STATE_RUNNING;
            case RUN_STATE_CANCELLATION_REQUESTED ->
                    DurableRunState.DURABLE_RUN_STATE_CANCELLATION_REQUESTED;
            case RUN_STATE_CANCELLED -> DurableRunState.DURABLE_RUN_STATE_CANCELLED;
            case RUN_STATE_COMPLETED -> DurableRunState.DURABLE_RUN_STATE_COMPLETED;
            case RUN_STATE_FAILED -> DurableRunState.DURABLE_RUN_STATE_FAILED;
            case RUN_STATE_UNSPECIFIED, UNRECOGNIZED -> throw new IllegalArgumentException(
                    "history requires a recognized run state");
        };
    }

    private static Instant deadline(FlowDefinition definition, Instant now) {
        Duration duration = definition.getDeadline();
        try {
            return now.plusSeconds(duration.getSeconds()).plusNanos(duration.getNanos());
        } catch (DateTimeException | ArithmeticException e) {
            throw new IllegalArgumentException("flow deadline overflows Instant", e);
        }
    }

    private static FlowValidationFinding finding(
            String code, String path, String message) {
        return FlowValidationFinding.newBuilder()
                .setCode(code).setPath(path).setMessage(message).build();
    }

    private static HistoryEvent baseEvent(
            long sequence, Instant occurredAt, HistoryEventKind kind) {
        return HistoryEvent.newBuilder()
                .setSequence(sequence)
                .setOccurredAt(timestamp(occurredAt))
                .setKind(kind)
                .build();
    }

    private static boolean validSlug(String value) {
        return value != null && SLUG.matcher(value).matches();
    }

    private static String bounded(String value, String fallback) {
        String result = value == null || value.isBlank() ? fallback : value;
        return result.length() <= 8_192 ? result : result.substring(0, 8_192);
    }

    private static Timestamp timestamp(Instant instant) {
        return RemoteValidation.timestamp(instant);
    }

    private final class StoreRunControl implements FlowRunControl {
        private final String runId;
        private long expectedRevision;

        private StoreRunControl(DurableFlowRun run) {
            runId = run.getRunId();
            expectedRevision = run.getStorageRevision();
        }

        @Override
        public void checkpoint(
                FlowHistory history, FlowExecutionCheckpoint checkpoint) {
            DurableFlowRun current = requireRun(history.getRunId());
            if (current.getStorageRevision() != expectedRevision) {
                throw new LifecycleConflictException("run " + history.getRunId()
                        + " advanced concurrently to storage_revision "
                        + current.getStorageRevision());
            }
            List<HistoryEvent> events = appended(current.getHistory(), history);
            DurableRunState state = durableState(history.getState());
            RunTransition.Builder transition = RunTransition.newBuilder()
                    .setRunId(history.getRunId())
                    .setExpectedStorageRevision(expectedRevision)
                    .setState(state)
                    .addAllEvents(events)
                    .setCheckpoint(checkpoint)
                    .setUpdatedAt(timestamp(clock.instant()));
            if (state == DurableRunState.DURABLE_RUN_STATE_CANCELLED) {
                transition.setCancellationReason(current.getCancellationReason());
            }
            try {
                DurableFlowRun updated = store.transition(transition.build());
                expectedRevision = updated.getStorageRevision();
            } catch (IOException e) {
                throw new UncheckedIOException("cannot checkpoint durable run "
                        + history.getRunId(), e);
            }
        }

        @Override
        public Cancellation cancellation() {
            DurableFlowRun current = store.run(runId).orElse(null);
            if (current == null || current.getState()
                    != DurableRunState.DURABLE_RUN_STATE_CANCELLATION_REQUESTED) {
                return Cancellation.none();
            }
            expectedRevision = current.getStorageRevision();
            return new Cancellation(true, current.getCancellationReason(),
                    current.getHistory());
        }
    }
}
