/**
 * Asynchronous workflow execution as durable jobs: the same workflow definition
 * {@code run-workflow} executes synchronously, run detached — Postgres is the
 * truth ({@code workflow_run} rows with per-step checkpoints), a Kafka request
 * topic in, lifecycle events out via the transactional outbox.
 *
 * <p>{@link ai.pipestream.proto.jobs.service.worker.WorkflowRunWorker} claims
 * QUEUED rows and executes one segment at a time through
 * {@link ai.pipestream.proto.workflow.WorkflowRunner#runSegment}, checkpointing
 * every step; {@link ai.pipestream.proto.jobs.service.events.WorkflowRunEventRelay}
 * drains the outbox to the workflow-run-events topic; the verbs
 * ({@code submit-workflow}, {@code get-job}, {@code list-jobs},
 * {@code complete-step}) live in
 * {@link ai.pipestream.proto.jobs.service.actions}.</p>
 */
package ai.pipestream.proto.jobs.service;
