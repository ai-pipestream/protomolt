package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.MeshValidation;
import ai.protomolt.proto.mesh.runtime.v1.ActiveFlowInvocation;
import ai.protomolt.proto.mesh.runtime.v1.DeploymentPointer;
import ai.protomolt.proto.mesh.runtime.v1.DurableFlowRun;
import ai.protomolt.proto.mesh.runtime.v1.DurableRunState;
import ai.protomolt.proto.mesh.runtime.v1.FlowExecutionCheckpoint;
import ai.protomolt.proto.mesh.runtime.v1.FlowFailure;
import ai.protomolt.proto.mesh.runtime.v1.FlowHistory;
import ai.protomolt.proto.mesh.runtime.v1.HistoryEvent;
import ai.protomolt.proto.mesh.runtime.v1.HistoryEventKind;
import ai.protomolt.proto.mesh.runtime.v1.LifecycleRecord;
import ai.protomolt.proto.mesh.runtime.v1.PendingFlowMessage;
import ai.protomolt.proto.mesh.runtime.v1.PendingFlowSettlement;
import ai.protomolt.proto.mesh.runtime.v1.ProcessorNode;
import ai.protomolt.proto.mesh.runtime.v1.PublishedFlowVersion;
import ai.protomolt.proto.mesh.runtime.v1.RunState;
import ai.protomolt.proto.mesh.runtime.v1.RunTransition;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Append-only protobuf lifecycle store. Publication, deployment changes, run
 * creation, history deltas, and execution-frontier changes share one forced WAL.
 */
public final class FileFlowLifecycleStore implements FlowLifecycleStore {

    private static final byte[] MAGIC = "PMFL0001".getBytes(StandardCharsets.US_ASCII);
    private static final int MAX_RECORD_BYTES = 64 * 1024 * 1024;
    private static final int MAX_HISTORY_PAGE = 10_000;

    private final FramedProtobufWal<LifecycleRecord> wal;
    private final Map<VersionKey, PublishedFlowVersion> versions = new LinkedHashMap<>();
    private final Map<String, DeploymentPointer> deployments = new LinkedHashMap<>();
    private final Map<String, DurableFlowRun> runs = new LinkedHashMap<>();
    private long nextSequence = 1;
    private boolean closed;

    public FileFlowLifecycleStore(Path path) throws IOException {
        FramedProtobufWal<LifecycleRecord> opened =
                new FramedProtobufWal<>(path, MAGIC, MAX_RECORD_BYTES,
                        LifecycleRecord.parser());
        try {
            for (LifecycleRecord record : opened.records()) {
                validateRecord(record);
                apply(record);
                nextSequence++;
            }
            wal = opened;
        } catch (Throwable failure) {
            try {
                opened.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            if (failure instanceof IOException io) {
                throw io;
            }
            if (failure instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IOException("cannot recover flow lifecycle WAL", failure);
        }
    }

    @Override
    public synchronized Optional<PublishedFlowVersion> published(
            String workflowName, String version) {
        requireOpen();
        return Optional.ofNullable(versions.get(new VersionKey(workflowName, version)));
    }

    @Override
    public synchronized List<PublishedFlowVersion> versions(String workflowName) {
        requireOpen();
        return versions.values().stream()
                .filter(value -> value.getWorkflowName().equals(workflowName))
                .sorted(Comparator.comparing(PublishedFlowVersion::getVersion))
                .toList();
    }

    @Override
    public synchronized PublishedFlowVersion publish(PublishedFlowVersion published)
            throws IOException {
        requireOpen();
        validatePublished(published);
        VersionKey key = new VersionKey(
                published.getWorkflowName(), published.getVersion());
        PublishedFlowVersion existing = versions.get(key);
        if (existing != null) {
            if (!existing.equals(published)) {
                throw new LifecycleConflictException("published workflow version is immutable: "
                        + key);
            }
            return existing;
        }
        append(LifecycleRecord.newBuilder().setFlowPublished(published),
                RemoteValidation.instant(published.getPublishedAt()));
        return published;
    }

    @Override
    public synchronized Optional<DeploymentPointer> deployment(String workflowName) {
        requireOpen();
        return Optional.ofNullable(deployments.get(workflowName));
    }

    @Override
    public synchronized DeploymentPointer deploy(
            String workflowName,
            String version,
            OptionalLong expectedRevision,
            Instant updatedAt) throws IOException {
        requireOpen();
        Objects.requireNonNull(updatedAt, "updatedAt");
        PublishedFlowVersion published = versions.get(new VersionKey(workflowName, version));
        if (published == null) {
            throw new IllegalArgumentException("cannot deploy unpublished workflow "
                    + workflowName + " version " + version);
        }
        DeploymentPointer current = deployments.get(workflowName);
        if (current == null) {
            if (expectedRevision.isPresent() && expectedRevision.getAsLong() != 0) {
                throw new LifecycleConflictException("deployment " + workflowName
                        + " does not exist at expected revision "
                        + expectedRevision.getAsLong());
            }
        } else {
            if (expectedRevision.isEmpty()) {
                throw new LifecycleConflictException("updating deployment " + workflowName
                        + " requires expected_revision " + current.getRevision());
            }
            if (expectedRevision.getAsLong() != current.getRevision()) {
                throw new LifecycleConflictException("deployment " + workflowName
                        + " is at revision " + current.getRevision()
                        + ", not " + expectedRevision.getAsLong());
            }
            if (current.getVersion().equals(version)
                    && current.getPlanFingerprint().equals(
                    published.getPlan().getPlanFingerprint())) {
                return current;
            }
        }
        long revision = current == null ? 1 : Math.addExact(current.getRevision(), 1);
        DeploymentPointer pointer = DeploymentPointer.newBuilder()
                .setWorkflowName(workflowName)
                .setVersion(version)
                .setPlanFingerprint(published.getPlan().getPlanFingerprint())
                .setRevision(revision)
                .setUpdatedAt(RemoteValidation.timestamp(updatedAt))
                .build();
        validateDeployment(pointer);
        append(LifecycleRecord.newBuilder().setDeploymentChanged(pointer), updatedAt);
        return pointer;
    }

    @Override
    public synchronized Optional<DurableFlowRun> run(String runId) {
        requireOpen();
        return Optional.ofNullable(runs.get(runId));
    }

    @Override
    public synchronized List<DurableFlowRun> incompleteRuns() {
        requireOpen();
        return runs.values().stream()
                .filter(run -> !terminal(run.getState()))
                .sorted(Comparator.comparing(DurableFlowRun::getCreatedAt,
                        FileFlowLifecycleStore::compareTimestamp))
                .toList();
    }

    @Override
    public synchronized DurableFlowRun createRun(DurableFlowRun run) throws IOException {
        requireOpen();
        validateCreatedRun(run);
        DurableFlowRun existing = runs.get(run.getRunId());
        if (existing != null) {
            if (!sameRunIdentity(existing, run)) {
                throw new LifecycleConflictException("run_id already names different work: "
                        + run.getRunId());
            }
            return existing;
        }
        append(LifecycleRecord.newBuilder().setRunCreated(run),
                RemoteValidation.instant(run.getCreatedAt()));
        return run;
    }

    @Override
    public synchronized DurableFlowRun transition(RunTransition transition)
            throws IOException {
        requireOpen();
        validateTransition(transition);
        append(LifecycleRecord.newBuilder().setRunTransitioned(transition),
                RemoteValidation.instant(transition.getUpdatedAt()));
        return runs.get(transition.getRunId());
    }

    @Override
    public synchronized HistoryPage history(String runId, long afterSequence, int limit) {
        requireOpen();
        if (afterSequence < 0) {
            throw new IllegalArgumentException("afterSequence must not be negative");
        }
        int boundedLimit = limit == 0 ? 100 : limit;
        if (boundedLimit < 1 || boundedLimit > MAX_HISTORY_PAGE) {
            throw new IllegalArgumentException("history limit must be between 1 and "
                    + MAX_HISTORY_PAGE);
        }
        DurableFlowRun run = runs.get(runId);
        if (run == null) {
            throw new IllegalArgumentException("unknown run_id " + runId);
        }
        List<HistoryEvent> events = run.getHistory().getEventsList().stream()
                .filter(event -> event.getSequence() > afterSequence)
                .limit(boundedLimit)
                .toList();
        long next = events.isEmpty() ? afterSequence
                : events.getLast().getSequence();
        return new HistoryPage(events, next, terminal(run.getState()));
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        wal.close();
    }

    private void append(LifecycleRecord.Builder event, Instant recordedAt)
            throws IOException {
        LifecycleRecord record = event
                .setSequence(nextSequence)
                .setRecordedAt(RemoteValidation.timestamp(recordedAt))
                .build();
        validateRecord(record);
        wal.append(record);
        apply(record);
        nextSequence++;
    }

    private void validateRecord(LifecycleRecord record) {
        RemoteValidation.annotations(record);
        if (record.getSequence() != nextSequence) {
            throw new IllegalArgumentException("lifecycle sequence must be " + nextSequence
                    + " but was " + record.getSequence());
        }
        RemoteValidation.instant(record.getRecordedAt());
        switch (record.getEventCase()) {
            case FLOW_PUBLISHED -> validatePublished(record.getFlowPublished());
            case DEPLOYMENT_CHANGED -> validateDeployment(record.getDeploymentChanged());
            case RUN_CREATED -> validateCreatedRun(record.getRunCreated());
            case RUN_TRANSITIONED -> validateTransition(record.getRunTransitioned());
            case EVENT_NOT_SET -> throw new IllegalArgumentException(
                    "lifecycle record requires an event");
        }
    }

    private void validatePublished(PublishedFlowVersion published) {
        Objects.requireNonNull(published, "published");
        RemoteValidation.annotations(published);
        if (!published.hasPlan()) {
            throw new IllegalArgumentException("published flow requires a compiled plan");
        }
        if (!published.getWorkflowName().equals(
                published.getPlan().getDefinition().getName())) {
            throw new IllegalArgumentException("published workflow name does not match plan");
        }
        if (published.getPlan().getPlanFingerprint().isBlank()) {
            throw new IllegalArgumentException("published flow requires plan fingerprint");
        }
        RemoteValidation.instant(published.getPublishedAt());
        PublishedFlowVersion existing = versions.get(new VersionKey(
                published.getWorkflowName(), published.getVersion()));
        if (existing != null && !existing.equals(published)) {
            throw new LifecycleConflictException("published workflow version is immutable: "
                    + published.getWorkflowName() + "/" + published.getVersion());
        }
    }

    private void validateDeployment(DeploymentPointer pointer) {
        RemoteValidation.annotations(pointer);
        PublishedFlowVersion published = versions.get(new VersionKey(
                pointer.getWorkflowName(), pointer.getVersion()));
        if (published == null) {
            throw new IllegalArgumentException("deployment names unpublished workflow "
                    + pointer.getWorkflowName() + "/" + pointer.getVersion());
        }
        if (!pointer.getPlanFingerprint().equals(
                published.getPlan().getPlanFingerprint())) {
            throw new IllegalArgumentException("deployment plan fingerprint does not match "
                    + pointer.getWorkflowName() + "/" + pointer.getVersion());
        }
        DeploymentPointer current = deployments.get(pointer.getWorkflowName());
        long expected = current == null ? 1 : current.getRevision() + 1;
        if (pointer.getRevision() != expected) {
            throw new IllegalArgumentException("deployment revision must be " + expected
                    + " but was " + pointer.getRevision());
        }
        RemoteValidation.instant(pointer.getUpdatedAt());
    }

    private void validateCreatedRun(DurableFlowRun run) {
        Objects.requireNonNull(run, "run");
        RemoteValidation.annotations(run);
        RemoteValidation.uuid(run.getRunId(), "run_id");
        if (run.getState() != DurableRunState.DURABLE_RUN_STATE_RUNNING
                || run.getStorageRevision() != 1) {
            throw new IllegalArgumentException(
                    "created run must be RUNNING at storage_revision 1");
        }
        PublishedFlowVersion published = versions.get(new VersionKey(
                run.getWorkflowName(), run.getWorkflowVersion()));
        if (published == null) {
            throw new IllegalArgumentException("run names unpublished workflow "
                    + run.getWorkflowName() + "/" + run.getWorkflowVersion());
        }
        if (run.getReplayOfRunId().isBlank()) {
            DeploymentPointer deployment = deployments.get(run.getWorkflowName());
            if (deployment == null
                    || deployment.getRevision() != run.getDeploymentRevision()
                    || !deployment.getVersion().equals(run.getWorkflowVersion())) {
                throw new LifecycleConflictException(
                        "run does not pin the current deployment of "
                                + run.getWorkflowName());
            }
        } else {
            DurableFlowRun source = runs.get(run.getReplayOfRunId());
            if (source == null
                    || !source.getWorkflowName().equals(run.getWorkflowName())
                    || !source.getWorkflowVersion().equals(run.getWorkflowVersion())
                    || !source.getPlanFingerprint().equals(run.getPlanFingerprint())
                    || source.getDeploymentRevision() != run.getDeploymentRevision()) {
                throw new LifecycleConflictException(
                        "replay does not pin its source run identities");
            }
        }
        if (!run.getPlanFingerprint().equals(published.getPlan().getPlanFingerprint())) {
            throw new IllegalArgumentException("run plan fingerprint does not match publication");
        }
        if (!RuntimeSchemas.same(run.getInput().getSchema(),
                published.getPlan().getDefinition().getInputSchema())) {
            throw new IllegalArgumentException("run input exact schema does not match workflow");
        }
        MeshValidation.validateStructure(run.getInput());
        validateHistory(run);
        validateCheckpoint(run.getCheckpoint(), published);
        if (!run.getCreatedAt().equals(run.getUpdatedAt())) {
            throw new IllegalArgumentException("new run created_at and updated_at must match");
        }
    }

    private void validateHistory(DurableFlowRun run) {
        if (!run.hasHistory()) {
            throw new IllegalArgumentException("run requires history");
        }
        FlowHistory history = run.getHistory();
        if (!history.getRunId().equals(run.getRunId())
                || !history.getFlowName().equals(run.getWorkflowName())
                || !history.getPlanFingerprint().equals(run.getPlanFingerprint())) {
            throw new IllegalArgumentException("run history identity does not match run");
        }
        if (!history.getReplayOfRunId().equals(run.getReplayOfRunId())
                || !history.getReplayFrontierSequencesList()
                .equals(run.getReplayFrontierSequencesList())) {
            throw new IllegalArgumentException("run replay identity does not match history");
        }
        long sequence = 0;
        for (HistoryEvent event : history.getEventsList()) {
            if (event.getSequence() != ++sequence) {
                throw new IllegalArgumentException("run history sequence must be " + sequence
                        + " but was " + event.getSequence());
            }
        }
        if (history.getState() != RunState.RUN_STATE_RUNNING) {
            throw new IllegalArgumentException("created run history must be RUNNING");
        }
    }

    private void validateTransition(RunTransition transition) {
        Objects.requireNonNull(transition, "transition");
        RemoteValidation.annotations(transition);
        RemoteValidation.uuid(transition.getRunId(), "run_id");
        DurableFlowRun current = runs.get(transition.getRunId());
        if (current == null) {
            throw new IllegalArgumentException("unknown run_id " + transition.getRunId());
        }
        if (current.getStorageRevision() != transition.getExpectedStorageRevision()) {
            throw new LifecycleConflictException("run " + transition.getRunId()
                    + " is at storage_revision " + current.getStorageRevision()
                    + ", not " + transition.getExpectedStorageRevision());
        }
        requireStateTransition(current.getState(), transition.getState());
        long sequence = current.getHistory().getEventsCount();
        for (HistoryEvent event : transition.getEventsList()) {
            if (event.getSequence() != ++sequence) {
                throw new IllegalArgumentException("run history sequence must continue at "
                        + sequence + " but was " + event.getSequence());
            }
        }
        PublishedFlowVersion published = versions.get(new VersionKey(
                current.getWorkflowName(), current.getWorkflowVersion()));
        validateCheckpoint(transition.getCheckpoint(), published);
        validateTerminalTransition(transition);
        RemoteValidation.instant(transition.getUpdatedAt());
        if (compareTimestamp(transition.getUpdatedAt(), current.getUpdatedAt()) < 0) {
            throw new IllegalArgumentException("run updated_at cannot move backwards");
        }
    }

    private void validateCheckpoint(
            FlowExecutionCheckpoint checkpoint, PublishedFlowVersion published) {
        if (checkpoint.hasDeadline()) {
            RemoteValidation.instant(checkpoint.getDeadline());
        }
        Map<String, ProcessorNode> nodes = new LinkedHashMap<>();
        published.getPlan().getDefinition().getNodesList()
                .forEach(node -> nodes.put(node.getNodeId(), node));
        Set<String> edges = new LinkedHashSet<>();
        published.getPlan().getDefinition().getEdgesList()
                .forEach(edge -> edges.add(edge.getEdgeId()));
        checkpoint.getPendingList().forEach(pending ->
                validatePending(pending, nodes, edges));
        if (checkpoint.hasActive()) {
            ActiveFlowInvocation active = checkpoint.getActive();
            RemoteValidation.annotations(active);
            validatePending(active.getPending(), nodes, edges);
            RemoteValidation.uuid(active.getInvocationId(), "active invocation_id");
            if (active.getInvocationOrdinal() < 1
                    || active.getInvocationOrdinal()
                    > checkpoint.getNextInvocationOrdinal()) {
                throw new IllegalArgumentException(
                        "active invocation ordinal exceeds checkpoint frontier");
            }
        }
        for (PendingFlowSettlement settlement : checkpoint.getSettlementsList()) {
            RemoteValidation.annotations(settlement);
            ProcessorNode node = nodes.get(settlement.getNodeId());
            if (node == null || !node.getProcessorId().equals(settlement.getProcessorId())) {
                throw new IllegalArgumentException("settlement names unknown processor node "
                        + settlement.getNodeId());
            }
            RemoteValidation.uuid(settlement.getInvocationId(), "settlement invocation_id");
            if (!settlement.getDeliveryId().isBlank()) {
                RemoteValidation.uuid(settlement.getDeliveryId(), "settlement delivery_id");
            }
            MeshValidation.validateStructure(settlement.getInput());
        }
        if (checkpoint.getSettlementStarted()
                && (checkpoint.hasActive()
                || checkpoint.getPendingCount() != 0)) {
            throw new IllegalArgumentException(
                    "settlement_started cannot retain pending or active work");
        }
    }

    private static void validatePending(
            PendingFlowMessage pending,
            Map<String, ProcessorNode> nodes,
            Set<String> edges) {
        RemoteValidation.annotations(pending);
        ProcessorNode node = nodes.get(pending.getNodeId());
        if (node == null) {
            throw new IllegalArgumentException("pending message names unknown node "
                    + pending.getNodeId());
        }
        if (!edges.contains(pending.getEdgeId())) {
            throw new IllegalArgumentException("pending message names unknown edge "
                    + pending.getEdgeId());
        }
        MeshValidation.validateStructure(pending.getInput());
        if (!RuntimeSchemas.same(node.getInputSchema(), pending.getInput().getSchema())) {
            throw new IllegalArgumentException("pending message exact schema does not match node "
                    + pending.getNodeId());
        }
    }

    private static void validateTerminalTransition(RunTransition transition) {
        boolean terminal = terminal(transition.getState());
        if (terminal && (transition.getCheckpoint().getPendingCount() != 0
                || transition.getCheckpoint().hasActive()
                || transition.getCheckpoint().getSettlementsCount() != 0)) {
            throw new IllegalArgumentException("terminal run cannot retain execution frontier");
        }
        HistoryEventKind required = switch (transition.getState()) {
            case DURABLE_RUN_STATE_CANCELLATION_REQUESTED ->
                    HistoryEventKind.HISTORY_EVENT_KIND_CANCELLATION_REQUESTED;
            case DURABLE_RUN_STATE_CANCELLED ->
                    HistoryEventKind.HISTORY_EVENT_KIND_RUN_CANCELLED;
            case DURABLE_RUN_STATE_COMPLETED ->
                    HistoryEventKind.HISTORY_EVENT_KIND_RUN_COMPLETED;
            case DURABLE_RUN_STATE_FAILED ->
                    HistoryEventKind.HISTORY_EVENT_KIND_RUN_FAILED;
            default -> null;
        };
        if (required != null && (transition.getEventsCount() == 0
                || transition.getEvents(transition.getEventsCount() - 1).getKind()
                != required)) {
            throw new IllegalArgumentException("transition to " + transition.getState()
                    + " requires final history event " + required);
        }
    }

    private static void requireStateTransition(
            DurableRunState from, DurableRunState to) {
        boolean allowed = switch (from) {
            case DURABLE_RUN_STATE_RUNNING -> to == DurableRunState.DURABLE_RUN_STATE_RUNNING
                    || to == DurableRunState.DURABLE_RUN_STATE_CANCELLATION_REQUESTED
                    || to == DurableRunState.DURABLE_RUN_STATE_COMPLETED
                    || to == DurableRunState.DURABLE_RUN_STATE_FAILED;
            case DURABLE_RUN_STATE_CANCELLATION_REQUESTED ->
                    to == DurableRunState.DURABLE_RUN_STATE_CANCELLATION_REQUESTED
                            || to == DurableRunState.DURABLE_RUN_STATE_CANCELLED;
            default -> false;
        };
        if (!allowed) {
            throw new IllegalArgumentException("invalid durable run transition "
                    + from + " -> " + to);
        }
    }

    private void apply(LifecycleRecord record) {
        switch (record.getEventCase()) {
            case FLOW_PUBLISHED -> {
                PublishedFlowVersion published = record.getFlowPublished();
                versions.putIfAbsent(new VersionKey(
                        published.getWorkflowName(), published.getVersion()), published);
            }
            case DEPLOYMENT_CHANGED -> {
                DeploymentPointer pointer = record.getDeploymentChanged();
                deployments.put(pointer.getWorkflowName(), pointer);
            }
            case RUN_CREATED -> runs.put(
                    record.getRunCreated().getRunId(), record.getRunCreated());
            case RUN_TRANSITIONED -> applyTransition(record.getRunTransitioned());
            case EVENT_NOT_SET -> throw new AssertionError("validated record lost its event");
        }
    }

    private void applyTransition(RunTransition transition) {
        DurableFlowRun current = runs.get(transition.getRunId());
        FlowHistory.Builder history = current.getHistory().toBuilder()
                .addAllEvents(transition.getEventsList())
                .setState(historyState(transition.getState()));
        List<ai.protomolt.proto.mesh.v1.EntityEnvelope> outputs = history.getEventsList().stream()
                .filter(event -> event.getKind()
                        == HistoryEventKind.HISTORY_EVENT_KIND_OUTPUT_RETAINED)
                .map(HistoryEvent::getMessage)
                .toList();
        if (transition.getState() == DurableRunState.DURABLE_RUN_STATE_COMPLETED) {
            history.clearOutputs().addAllOutputs(outputs).clearFailure();
        } else if (transition.getState() == DurableRunState.DURABLE_RUN_STATE_FAILED
                || transition.getState() == DurableRunState.DURABLE_RUN_STATE_CANCELLED) {
            FlowFailure failure = transition.getEvents(
                    transition.getEventsCount() - 1).getFailure();
            history.setFailure(failure);
        }
        DurableFlowRun.Builder updated = current.toBuilder()
                .setState(transition.getState())
                .setStorageRevision(current.getStorageRevision() + 1)
                .setUpdatedAt(transition.getUpdatedAt())
                .setHistory(history)
                .setCheckpoint(transition.getCheckpoint());
        if (!transition.getCancellationReason().isBlank()) {
            updated.setCancellationReason(transition.getCancellationReason());
        }
        runs.put(transition.getRunId(), updated.build());
    }

    private static RunState historyState(DurableRunState state) {
        return switch (state) {
            case DURABLE_RUN_STATE_RUNNING -> RunState.RUN_STATE_RUNNING;
            case DURABLE_RUN_STATE_CANCELLATION_REQUESTED ->
                    RunState.RUN_STATE_CANCELLATION_REQUESTED;
            case DURABLE_RUN_STATE_CANCELLED -> RunState.RUN_STATE_CANCELLED;
            case DURABLE_RUN_STATE_COMPLETED -> RunState.RUN_STATE_COMPLETED;
            case DURABLE_RUN_STATE_FAILED -> RunState.RUN_STATE_FAILED;
            case DURABLE_RUN_STATE_UNSPECIFIED -> RunState.RUN_STATE_UNSPECIFIED;
            case UNRECOGNIZED -> throw new IllegalArgumentException(
                    "unrecognized durable run state");
        };
    }

    private static boolean sameRunIdentity(DurableFlowRun first, DurableFlowRun second) {
        return first.getRunId().equals(second.getRunId())
                && first.getWorkflowName().equals(second.getWorkflowName())
                && first.getWorkflowVersion().equals(second.getWorkflowVersion())
                && first.getPlanFingerprint().equals(second.getPlanFingerprint())
                && first.getDeploymentRevision() == second.getDeploymentRevision()
                && first.getInput().equals(second.getInput())
                && first.getReplayOfRunId().equals(second.getReplayOfRunId())
                && first.getReplayFrontierSequencesList()
                .equals(second.getReplayFrontierSequencesList());
    }

    static boolean terminal(DurableRunState state) {
        return state == DurableRunState.DURABLE_RUN_STATE_CANCELLED
                || state == DurableRunState.DURABLE_RUN_STATE_COMPLETED
                || state == DurableRunState.DURABLE_RUN_STATE_FAILED;
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("flow lifecycle store is closed");
        }
    }

    private static int compareTimestamp(
            com.google.protobuf.Timestamp first,
            com.google.protobuf.Timestamp second) {
        int seconds = Long.compare(first.getSeconds(), second.getSeconds());
        return seconds != 0 ? seconds : Integer.compare(first.getNanos(), second.getNanos());
    }

    private record VersionKey(String workflowName, String version) {
        private VersionKey {
            Objects.requireNonNull(workflowName, "workflowName");
            Objects.requireNonNull(version, "version");
        }

        @Override
        public String toString() {
            return workflowName + "/" + version;
        }
    }
}
