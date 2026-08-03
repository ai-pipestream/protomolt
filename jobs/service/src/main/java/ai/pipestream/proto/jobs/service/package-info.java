/**
 * Asynchronous chain execution as durable jobs: the same chain definition
 * {@code run-chain} executes synchronously, run detached — Postgres is the
 * truth ({@code chain_job} rows with per-step checkpoints), a Kafka request
 * topic in, lifecycle events out via the transactional outbox.
 *
 * <p>{@link ai.pipestream.proto.jobs.service.worker.ChainJobWorker} claims
 * QUEUED rows and executes one segment at a time through
 * {@link ai.pipestream.proto.chain.ChainRunner#runSegment}, checkpointing
 * every step; {@link ai.pipestream.proto.jobs.service.events.ChainJobEventRelay}
 * drains the outbox to the chain-job-events topic; the verbs
 * ({@code submit-chain}, {@code get-job}, {@code list-jobs},
 * {@code complete-step}) live in
 * {@link ai.pipestream.proto.jobs.service.actions}.</p>
 */
package ai.pipestream.proto.jobs.service;
