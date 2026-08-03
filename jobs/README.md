# protomolt jobs — asynchronous chain execution

`run-chain` is synchronous: the caller holds a gRPC connection while steps
execute serially. That is right for sub-second work and wrong for LLM-scale
work (one item costs a minute of inference; a corpus run is millions of
items). This module runs the **same chain definition with the same serial
semantics**, detached, as a durable job: Kafka in, Kafka events out,
Postgres as the truth.

Two Gradle modules:

- **`jobs/proto`** (`protomolt-jobs-proto`) — the wire contract
  (`ai.pipestream.proto.jobs.v1`): `ChainJobEvent` (the lifecycle envelope on
  the events topic) and `ChainJobRequest` (the request-topic payload for
  broker-native submission). Messages only — the verbs are descriptor-native
  ProtoMoltService actions, so there is no gRPC service here.
- **`jobs/service`** (`protomolt-jobs-service`) — the store, the worker, the
  outbox relay, and the four verbs. Framework-free: plain JDBC over
  HikariCP + Flyway, plain kafka-clients, virtual threads.

## The contract

**Table: `chain_job`** (Flyway `V1__chain_jobs.sql`). One row per job: the
chain envelope and input snapshotted INTO the row at submit (a chain edited
later never shifts a live job), status, attempt counter, the ordered
`checkpoints` JSONB array, result/verdict/error, lease owner + expiry,
timestamps. Status machine:

```
QUEUED → RUNNING → COMPLETED
                 → WAITING   (parked on an external-completion step)
                 → QUEUED    (retryable failure, backoff via run_after;
                              or lease expiry swept back)
                 → FAILED    (a validation verdict or non-retryable error)
                 → DEAD      (attempts exhausted — the dead-letter state)
```

**Table: `chain_job_events_outbox`**. Every commit point above inserts one
outbox row in the SAME TRANSACTION as the job mutation, so an event can
never drift from the state change it describes. The relay drains PENDING
rows to the **`chain-job-events`** topic, keyed by `job_id` (one job's
events are partition-ordered). Delivery is at-least-once: publish first,
mark PUBLISHED after the ack; consumers dedupe on `ChainJobEvent.event_id`
(the outbox row id). A row whose relay attempts reach 10 lands FAILED — that
row IS the DLQ; the relay deliberately never re-enqueues it.

**Topic: `chain-job-requests`** (name from `ChainJobsConfig.requestTopic`).
Producing a `ChainJobRequest` keyed by `job_id` is exactly equivalent to
`submit-chain` with a stored chain name — broker-native clients never touch
gRPC. The worker consumes with group `protomolt-jobs-worker`, commits the
offset only after the row commits (at-least-once; the insert is idempotent
on `job_id`). An unknown chain name writes a FAILED row loudly — nothing is
silently dropped.

**The four verbs** (ProtoAction implementations; a server without
`--jobs-jdbc`/`--jobs-kafka` answers all four `unavailable`):

- `submit-chain` — `{chain | chainName, input, jobId?}` → `{ok, jobId,
  status}`. Verifies the chain before persisting; `jobId` is the idempotency
  key (a uuid is minted when absent; resubmitting returns the existing row).
- `get-job` — `{jobId}` → the full row: input, checkpoints, result.
- `list-jobs` — `{status?, chainName?, limit?, offset?}` → summary rows
  (no input/checkpoints/result), newest first; limit defaults 50, caps 500.
- `complete-step` — `{jobId, stepName, response}` → the human-in-the-loop
  lane; see below.

## Checkpointing, retries, verdicts, poison

**Step checkpointing is the whole game.** Each step's response is persisted
to the job row (same transaction as its STEP_CHECKPOINT event) before the
next step runs, so a worker crash — or a lost lease swept back to QUEUED —
resumes at the first missing checkpoint instead of at step zero. The
in-flight step that never checkpointed re-executes: **side-effecting steps
must be idempotent on `job_id + step_name`**. Likewise, a store failure
inside the checkpoint observer requeues the job (the response may not have
persisted); it never settles as failed work.

The failure model rides `ChainRunner.ChainExecutionException.kind()`:

- **Retryable** — kind GRPC with UNAVAILABLE / DEADLINE_EXCEEDED /
  RESOURCE_EXHAUSTED, or kind DEADLINE. Requeues with exponential backoff
  (`base * 2^(attempt-1)` seconds) until `attempt == max_attempts`, then
  lands DEAD with the last error verbatim. Operator territory: nothing
  re-enqueues a DEAD job.
- **Verdict** — kind VALIDATION. The job FAILS with the violations in the
  record. This is not an error; it is the chain's answer, and the review
  queue consumes these events directly.
- **Non-retryable** — GATE / MAPPING / CHAIN. Deterministic corruption;
  the job FAILS loud.

## Park / resume (external steps)

A step declaring `completion: "external"` parks the job: the worker persists
the checkpoint prefix, writes the WAITING event, and stops (the chain
deadline bounds a segment, not the hours a human may take). `complete-step`
supplies the response: the job's state is gated first (only a WAITING job
parked on exactly that step accepts it), the response is parsed against the
step's output type and — when the step declares `validate` — checked against
its declared rules (a rejection fails the job as a verdict). Accepted, the
checkpoint appends and the job requeues in one transaction; the worker fleet
runs the next segment. Redelivering a completed step is idempotent.

## Per-target concurrency

A worker acquires a permit (bounded at `maxConcurrentPerTarget`) for the
target of the job's NEXT unexecuted step before running a segment, and
releases it when the segment ends. This is a documented approximation — a
segment may call several targets, so the cap throttles the step each job is
about to run rather than every call in flight. It exists because the
inference tier's bottleneck is one box per model, and an uncapped worker
fleet will DDoS it.

## Wiring

The pieces are plain constructors (no framework, no DI):

```java
ChainJobDatabase database = new ChainJobDatabase(new ChainJobStoreConfig(jdbcUrl, user, password));
JdbcChainJobStore store = new JdbcChainJobStore(database);
ChainJobWorker worker = new ChainJobWorker(store, actionContext, chainRepositoryOrNull,
        new ChainRunner(), config);          // config: ChainJobsConfig
worker.start();                              // workerCount claim loops + request-topic consumer
ChainJobEventRelay relay = new ChainJobEventRelay(store,
        ChainJobEventRelay.newProducer(bootstrap, schemaRegistryUrlOrNull),
        config.eventsTopic(), Duration.ofMillis(500), 100);
relay.start();
```

The verbs take the same store (`new SubmitChainAction(store, repository,
maxAttempts)` etc.); passing a null store makes them answer `unavailable`,
which is how a server without jobs configured mounts them anyway.

## v1 deferrals (explicit, not forgotten)

- **Payloads ride inline in the row** (`input`, `checkpoints`, `result` as
  JSONB). The `input_ref`/`result_ref` columns for the repo-service
  claim-check (rustfs, versioned) exist but are unwired; step responses are
  small protos, so rows stay small until decoration-scale outputs arrive.
- **No `WatchJob` streaming RPC** — the events topic is the watch lane;
  `get-job`/`list-jobs` cover the CLI and 2am debugging.
- **`run-chain` is unchanged** — async is an execution mode, not a chain
  dialect. The worker replays the same definition through the same
  `ChainRunner` (`runSegment`), so a chain means exactly the same thing in
  both modes.
- **Poison request records kill the consumer thread** (logged loud; the
  claim loops keep serving verb-submitted jobs and the broker retains the
  records). A request-tombstone/DLQ lane is a later refinement.

## Tests

- Unit (no Docker): `ChainJobWorkerTest` drives the complete / park-resume /
  retry-dead / verifier-rejection / gate-skip / checkpoint-hiccup cycles
  against in-process gRPC services and an in-memory store;
  `ChainJobActionsTest` covers the four verbs' envelopes and the null-store
  `unavailable` answer.
- Integration (`*IT`, `@Testcontainers(disabledWithoutDocker = true)`):
  `JdbcChainJobStoreIT` against PostgreSQL 17 (claim under concurrency,
  lease sweep, checkpoint transactions, outbox DLQ) and `ChainJobKafkaIT`
  against Redpanda (request-topic submit → chain execution on a real
  localhost gRPC server → relayed lifecycle events, plus the
  unknown-chain-name loud failure).
