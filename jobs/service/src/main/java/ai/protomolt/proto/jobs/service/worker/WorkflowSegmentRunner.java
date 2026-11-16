package ai.protomolt.proto.jobs.service.worker;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.http.json.MalformedProtobufJsonException;
import ai.protomolt.proto.jobs.service.events.WorkflowRunEventFactory;
import ai.protomolt.proto.jobs.service.store.ClaimLostException;
import ai.protomolt.proto.jobs.service.store.WorkerClaim;
import ai.protomolt.proto.jobs.service.store.WorkflowRunRecord;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStore;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStoreException;
import ai.protomolt.proto.workflow.CompiledWorkflow;
import ai.protomolt.proto.workflow.WorkflowJson;
import ai.protomolt.proto.workflow.WorkflowRunner;
import ai.protomolt.proto.workflow.WorkflowVerifier;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DynamicMessage;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 * Package-private executor for one claimed {@link CompiledWorkflow} segment: verifies the
 * stored definition, rebuilds checkpoints, executes until park or completion, and settles
 * retry/dead-letter/failure outcomes.
 */
final class WorkflowSegmentRunner {

    private static final Logger LOG = LoggerFactory.getLogger(WorkflowSegmentRunner.class);

    private final WorkflowRunStore store;
    private final ActionContext context;
    private final WorkflowRunner runner;
    private final WorkflowRunWorker worker;

    WorkflowSegmentRunner(WorkflowRunStore store, ActionContext context,
                          WorkflowRunner runner, WorkflowRunWorker worker) {
        this.store = store;
        this.context = context;
        this.runner = runner;
        this.worker = worker;
    }

    void execute(WorkflowRunRecord job, WorkerClaim claim) {
        CompiledWorkflow definition = parseAndVerify(job, claim);
        if (definition == null) {
            return;
        }
        DynamicMessage input;
        try {
            input = context.transcoder().fromJsonDynamic(job.input, definition.inputType());
        } catch (MalformedProtobufJsonException e) {
            worker.failJob(job, claim, "", "MAPPING: stored input is not valid proto3 JSON for "
                    + definition.inputType().getFullName() + ": " + e.getMessage());
            return;
        }
        List<WorkflowRunner.Checkpoint> prior;
        try {
            prior = rebuildCheckpoints(job, definition);
        } catch (CorruptJobException e) {
            worker.failJob(job, claim, "", "WORKFLOW: " + e.getMessage());
            return;
        }
        List<WorkflowRunner.Checkpoint> accumulated = new ArrayList<>(prior);
        Semaphore permit = acquirePermit(definition, prior.size());
        try {
            WorkflowRunner.Segment segment = runner.runSegment(definition, input, prior,
                    checkpoint -> {
                        accumulated.add(checkpoint);
                        store.saveCheckpoint(claim, checkpointsJson(accumulated),
                                WorkflowRunEventFactory.stepCheckpoint(job, checkpoint.name()));
                    });
            switch (segment) {
                case WorkflowRunner.Segment.Completed completed -> {
                    String resultJson = context.transcoder().toJson(completed.result().output());
                    String verdict = completed.result().steps().size() + " steps, output "
                            + completed.result().output().getDescriptorForType().getFullName();
                    store.markCompleted(claim, resultJson, verdict,
                            WorkflowRunEventFactory.completed(job, verdict));
                    LOG.info("workflow run {} completed: {}", job.jobId, verdict);
                }
                case WorkflowRunner.Segment.Parked parked -> {
                    store.markWaiting(claim, parked.step(), checkpointsJson(accumulated),
                            WorkflowRunEventFactory.waiting(job, parked.step()));
                    LOG.info("workflow run {} parked on external step '{}'",
                            job.jobId, parked.step());
                }
            }
        } catch (WorkflowRunner.WorkflowExecutionException e) {
            if (e.getCause() instanceof ClaimLostException lost) {
                throw lost;
            }
            handleFailure(job, claim, e);
        } finally {
            if (permit != null) {
                permit.release();
            }
        }
    }

    private CompiledWorkflow parseAndVerify(WorkflowRunRecord job, WorkerClaim claim) {
        JsonNode tree;
        try {
            tree = context.objectMapper().readTree(job.workflowDefinition);
        } catch (JsonProcessingException | IllegalArgumentException e) {
            worker.failJob(job, claim, "",
                    "WORKFLOW: stored workflow definition is not readable JSON: "
                            + e.getMessage());
            return null;
        }
        if (!(tree instanceof ObjectNode workflowNode)) {
            worker.failJob(job, claim, "",
                    "WORKFLOW: stored workflow definition is not a JSON object");
            return null;
        }
        CompiledWorkflow definition;
        try {
            definition = WorkflowJson.parse(workflowNode, context);
        } catch (WorkflowJson.WorkflowParseException e) {
            worker.failJob(job, claim, e.step, "WORKFLOW: stored workflow does not parse"
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
            worker.failJob(job, claim, findings.getFirst().step(), detail.toString());
            return null;
        }
        return definition;
    }

    private List<WorkflowRunner.Checkpoint> rebuildCheckpoints(WorkflowRunRecord job,
            CompiledWorkflow definition) throws CorruptJobException {
        JsonNode tree;
        try {
            tree = context.objectMapper().readTree(
                    job.checkpoints == null ? "[]" : job.checkpoints);
        } catch (JsonProcessingException e) {
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

    private void handleFailure(WorkflowRunRecord job, WorkerClaim claim,
            WorkflowRunner.WorkflowExecutionException e) {
        boolean retryable = isRetryable(e);
        String detail = e.kind() + ": " + e.getMessage();
        if (retryable && job.attempt < job.maxAttempts) {
            Duration backoff = worker.backoff(job.attempt);
            store.requeue(claim, backoff);
            LOG.info("workflow run {} attempt {}/{} failed retryably ({}); requeued in {}s",
                    job.jobId, job.attempt, job.maxAttempts, detail, backoff.toSeconds());
        } else if (retryable) {
            store.markDead(claim, detail, WorkflowRunEventFactory.dead(job, detail));
            LOG.warn("workflow run {} is DEAD after {} attempt(s): {}",
                    job.jobId, job.attempt, detail);
        } else {
            worker.failJob(job, claim, e.step(), detail);
        }
    }

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

    private Semaphore acquirePermit(CompiledWorkflow definition, int nextStepIndex) {
        if (nextStepIndex >= definition.steps().size()) {
            return null;
        }
        return worker.acquireTargetPermit(definition.steps().get(nextStepIndex).target());
    }

    private String checkpointsJson(List<WorkflowRunner.Checkpoint> checkpoints) {
        ArrayNode array = context.objectMapper().createArrayNode();
        for (WorkflowRunner.Checkpoint checkpoint : checkpoints) {
            ObjectNode entry = array.addObject();
            entry.put("name", checkpoint.name());
            entry.put("skipped", checkpoint.skipped());
            if (checkpoint.response() != null) {
                String preview = context.transcoder().toJson(checkpoint.response());
                try {
                    entry.set("response", context.objectMapper().readTree(preview));
                } catch (JsonProcessingException e) {
                    throw new IllegalStateException("could not render the checkpoint for '"
                            + checkpoint.name() + "' as JSON", e);
                }
            }
        }
        return array.toString();
    }

    private static final class CorruptJobException extends Exception {
        CorruptJobException(String message) {
            super(message);
        }
    }
}
