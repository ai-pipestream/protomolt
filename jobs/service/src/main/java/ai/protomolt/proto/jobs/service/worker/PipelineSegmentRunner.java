package ai.protomolt.proto.jobs.service.worker;

import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.jobs.service.PipelineJobCoordinator;
import ai.protomolt.proto.jobs.service.PipelineJobCoordinator.PipelineDefinitionException;
import ai.protomolt.proto.jobs.service.events.WorkflowRunEventFactory;
import ai.protomolt.proto.jobs.service.store.ClaimLostException;
import ai.protomolt.proto.jobs.service.store.WorkerClaim;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStore;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStoreException;
import ai.protomolt.proto.pipeline.PipelineExecutor;
import ai.protomolt.proto.pipeline.v1.EdgeCardinality;
import ai.protomolt.proto.pipeline.v1.Pipeline;
import ai.protomolt.proto.pipeline.v1.PipelineStep;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 * Package-private executor for one claimed durable {@link Pipeline} segment: reconstructs
 * the binary definition and checkpoint prefix, executes until park or completion, and
 * settles retry/dead-letter/failure outcomes.
 */
final class PipelineSegmentRunner {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineSegmentRunner.class);

    private final WorkflowRunStore store;
    private final PipelineExecutor pipelineExecutor;
    private final WorkflowRunWorker worker;

    PipelineSegmentRunner(WorkflowRunStore store, PipelineExecutor pipelineExecutor,
                          WorkflowRunWorker worker) {
        this.store = store;
        this.pipelineExecutor = pipelineExecutor;
        this.worker = worker;
    }

    void execute(WorkflowRunRecord job, WorkerClaim claim) {
        PipelineJobCoordinator.Definition definition;
        try {
            definition = PipelineJobCoordinator.readDefinition(job);
        } catch (PipelineDefinitionException e) {
            worker.failJob(job, claim, e.step(), "PIPELINE: " + e.getMessage());
            return;
        }
        List<PipelineExecutor.Checkpoint> prior;
        try {
            prior = PipelineJobCoordinator.readCheckpoints(job, definition);
        } catch (PipelineDefinitionException e) {
            worker.failJob(job, claim, e.step(), "PIPELINE: " + e.getMessage());
            return;
        }
        List<PipelineExecutor.Checkpoint> accumulated = new ArrayList<>(prior);
        Semaphore permit = acquirePipelinePermit(definition.pipeline(), prior.size());
        PipelineExecutor.ExecutionIdentity identity =
                new PipelineExecutor.ExecutionIdentity(job.jobId.toString(), job.attempt);
        try {
            PipelineExecutor.Segment segment = pipelineExecutor.runSegment(
                    definition.pipeline(),
                    definition.files(),
                    definition.input(),
                    prior,
                    identity,
                    checkpoint -> {
                        accumulated.add(checkpoint);
                        store.saveCheckpoint(claim,
                                PipelineJobCoordinator.writeCheckpoints(
                                        accumulated, definition.files()),
                                WorkflowRunEventFactory.stepCheckpoint(
                                        job, checkpoint.outcome().name()));
                    });
            switch (segment) {
                case PipelineExecutor.Segment.Completed completed -> {
                    String resultJson = PipelineJobCoordinator.writeResult(
                            completed.result(), definition.files());
                    String verdict = completed.result().cardinality()
                            == EdgeCardinality.EDGE_CARDINALITY_ONE
                            ? completed.result().steps().size() + " steps, output "
                                    + completed.result().message()
                                            .getDescriptorForType().getFullName()
                            : completed.result().steps().size() + " steps, stream ("
                                    + completed.result().messages().size() + " messages)";
                    store.markCompleted(claim, resultJson, verdict,
                            WorkflowRunEventFactory.completed(job, verdict));
                    LOG.info("pipeline run {} completed: {}", job.jobId, verdict);
                }
                case PipelineExecutor.Segment.Parked parked -> {
                    store.markWaiting(claim, parked.step(),
                            PipelineJobCoordinator.writeCheckpoints(
                                    accumulated, definition.files()),
                            WorkflowRunEventFactory.waiting(job, parked.step()));
                    LOG.info("pipeline run {} parked on external step '{}'",
                            job.jobId, parked.step());
                }
            }
        } catch (PipelineExecutor.PipelineExecutionException e) {
            if (e.getCause() instanceof ClaimLostException lost) {
                throw lost;
            }
            handlePipelineFailure(job, claim, e);
        } finally {
            if (permit != null) {
                permit.release();
            }
        }
    }

    private void handlePipelineFailure(WorkflowRunRecord job, WorkerClaim claim,
            PipelineExecutor.PipelineExecutionException e) {
        boolean retryable = isPipelineRetryable(e);
        String detail = e.kind() + ": " + e.getMessage();
        if (retryable && job.attempt < job.maxAttempts) {
            Duration backoff = worker.backoff(job.attempt);
            store.requeue(claim, backoff);
            LOG.info("pipeline run {} attempt {}/{} failed retryably ({}); requeued in {}s",
                    job.jobId, job.attempt, job.maxAttempts, detail, backoff.toSeconds());
        } else if (retryable) {
            store.markDead(claim, detail, WorkflowRunEventFactory.dead(job, detail));
            LOG.warn("pipeline run {} is DEAD after {} attempt(s): {}",
                    job.jobId, job.attempt, detail);
        } else {
            worker.failJob(job, claim, e.step(), detail);
        }
    }

    private static boolean isPipelineRetryable(PipelineExecutor.PipelineExecutionException e) {
        if (e.kind() == PipelineExecutor.FailureKind.DEADLINE) {
            return true;
        }
        if (e.kind() == PipelineExecutor.FailureKind.GRPC) {
            return e.grpcCode() == Status.Code.UNAVAILABLE
                    || e.grpcCode() == Status.Code.DEADLINE_EXCEEDED
                    || e.grpcCode() == Status.Code.RESOURCE_EXHAUSTED;
        }
        return e.kind() == PipelineExecutor.FailureKind.PIPELINE
                && e.getCause() instanceof WorkflowRunStoreException;
    }

    private Semaphore acquirePipelinePermit(Pipeline pipeline, int nextStepIndex) {
        if (nextStepIndex >= pipeline.getStepsCount()) {
            return null;
        }
        PipelineStep step = pipeline.getSteps(nextStepIndex);
        if (step.getDependency().isEmpty()) {
            return null;
        }
        String target = step.getDependency();
        for (ServiceDependency dependency : pipeline.getDependenciesList()) {
            if (dependency.getAlias().equals(step.getDependency())) {
                target = worker.resolvePipelineTarget(dependency);
                break;
            }
        }
        return worker.acquireTargetPermit(target);
    }
}
