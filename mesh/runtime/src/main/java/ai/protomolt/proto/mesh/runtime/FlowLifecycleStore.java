package ai.protomolt.proto.mesh.runtime;

import ai.protomolt.proto.mesh.runtime.v1.DeploymentPointer;
import ai.protomolt.proto.mesh.runtime.v1.DurableFlowRun;
import ai.protomolt.proto.mesh.runtime.v1.HistoryEvent;
import ai.protomolt.proto.mesh.runtime.v1.PublishedFlowVersion;
import ai.protomolt.proto.mesh.runtime.v1.RunTransition;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/** Durable workflow versions, deployment pointers, run frontiers, and history. */
public interface FlowLifecycleStore extends AutoCloseable {

    Optional<PublishedFlowVersion> published(String workflowName, String version);

    List<PublishedFlowVersion> versions(String workflowName);

    /** Saves an immutable version or returns the byte-identical existing version. */
    PublishedFlowVersion publish(PublishedFlowVersion published) throws IOException;

    Optional<DeploymentPointer> deployment(String workflowName);

    /** Atomically changes one pointer, optionally fenced by its current revision. */
    DeploymentPointer deploy(
            String workflowName,
            String version,
            OptionalLong expectedRevision,
            Instant updatedAt) throws IOException;

    Optional<DurableFlowRun> run(String runId);

    List<DurableFlowRun> incompleteRuns();

    /** Creates a run before execution begins; run identity is an idempotency key. */
    DurableFlowRun createRun(DurableFlowRun run) throws IOException;

    /** Appends new history and replaces the execution frontier under a revision fence. */
    DurableFlowRun transition(RunTransition transition) throws IOException;

    HistoryPage history(String runId, long afterSequence, int limit);

    @Override
    void close() throws IOException;

    record HistoryPage(List<HistoryEvent> events, long nextSequence, boolean terminal) {
        public HistoryPage {
            events = List.copyOf(events);
        }
    }
}
