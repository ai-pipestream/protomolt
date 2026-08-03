-- Chain jobs: durable asynchronous chain execution. Postgres is the truth;
-- the Kafka topics are propagation.
--
-- One chain_job row per job: the chain envelope and input are snapshotted
-- INTO the row at submit (a chain definition edited later never shifts a
-- live job), and each step's response is checkpointed into the row as it
-- lands, so a worker crash resumes at the first missing checkpoint instead
-- of at step zero. The row answers "what is the state of job X right now"
-- without replaying a log; the events topic tells interested parties what
-- happened.
--
-- status: QUEUED (claimable once run_after passes) → RUNNING (a worker holds
-- the lease) → WAITING (parked on an external-completion step, awaiting
-- complete-step; outstanding_step names it) → COMPLETED / FAILED / DEAD
-- (terminal). FAILED is a verdict or a non-retryable error; DEAD is retries
-- exhausted — operator territory, nothing re-enqueues it. A RUNNING job
-- whose lease expires (worker died) is swept back to QUEUED; the
-- checkpoints make that resume safe.

CREATE TABLE chain_job (
    -- Client-generated uuid (the submit idempotency key).
    job_id           UUID PRIMARY KEY,
    -- The stored chain name, or the inline chain's declared name.
    chain_name       TEXT NOT NULL,
    -- The raw chain JSON envelope (the run-chain shape), snapshotted at
    -- submit. Workers execute exactly this, never a re-resolved definition.
    chain_definition JSONB NOT NULL,
    -- The chain input, proto3 JSON of the chain's inputType.
    input            JSONB NOT NULL,
    -- Future repo-service claim-check for the input; null until that lands.
    input_ref        TEXT,
    status           TEXT NOT NULL,
    -- 1-based execution counter; claim increments it.
    attempt          INTEGER NOT NULL DEFAULT 0,
    -- Retry ceiling; at attempt == max_attempts a retryable failure lands
    -- DEAD instead of requeueing.
    max_attempts     INTEGER NOT NULL DEFAULT 3,
    -- Claim eligibility: backoff sets it into the future, the claim scan
    -- requires run_after <= now().
    run_after        TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- The external-completion step the job is parked on; set only while
    -- WAITING.
    outstanding_step TEXT,
    -- Ordered checkpoint array: one entry per executed step,
    -- {"name": string, "skipped": bool, "response": object|null}, response
    -- being proto3 JSON of that step's output type. The resume prefix.
    checkpoints      JSONB NOT NULL DEFAULT '[]',
    -- The chain's composed output (proto3 JSON); set on COMPLETED only.
    result           JSONB,
    -- Future repo-service claim-check for the result; null until that lands.
    result_ref       TEXT,
    -- The one-line completion summary ("3 steps, output acme.v1.Ticket").
    verdict          TEXT,
    -- The verbatim last error (validation violations included); set on
    -- FAILED and DEAD.
    error            TEXT,
    -- The worker holding the lease; null unless RUNNING.
    lease_owner      TEXT,
    lease_until      TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- When the job reached a terminal state (COMPLETED, FAILED or DEAD).
    completed_at     TIMESTAMPTZ,

    CONSTRAINT chk_chain_job_status CHECK (status IN
        ('QUEUED', 'RUNNING', 'WAITING', 'COMPLETED', 'FAILED', 'DEAD'))
);

-- The claim scan: claimable jobs, oldest eligible first.
CREATE INDEX idx_chain_job_status_run_after ON chain_job (status, run_after);

-- Transactional outbox for the chain-job-events topic.
--
-- Every job commit point (accept, step checkpoint, park, terminal state)
-- inserts one row here IN THE SAME TRANSACTION as the chain_job mutation, so
-- an event can never drift from the state change it describes. The payload
-- column carries the serialized ChainJobEvent protobuf (see jobs/proto
-- jobs.proto); event_type names the ChainJobEvent.Type. kafka_key is the
-- job_id: the relay publishes with it as the record key, so one job's events
-- are partition-ordered on the single chain-job-events topic.
--
-- status: PENDING (awaiting/between relay attempts) → PUBLISHED (acked by
-- the broker; retained, not deleted) or FAILED (attempts exhausted; the
-- FAILED record IS the dead-letter queue for now — operator territory, the
-- relay deliberately does not re-enqueue it).
--
-- Delivery is at-least-once: the relay publishes first and marks PUBLISHED
-- after, so a crash mid-flight republishes on restart. Consumers dedupe on
-- the event id (ChainJobEvent.event_id carries this row's event_id).

CREATE TABLE chain_job_events_outbox (
    -- Surrogate event id, minted by the app (java.util.UUID) per event.
    event_id     UUID PRIMARY KEY,
    -- The ChainJobEvent.Type name (no CHECK: the enum may grow).
    event_type   TEXT NOT NULL,
    -- The serialized ChainJobEvent protobuf.
    payload      BYTEA NOT NULL,
    -- The Kafka record key: the job's job_id.
    kafka_key    TEXT NOT NULL,
    -- Relay attempts so far; attempts >= 10 flips status to FAILED.
    attempts     INTEGER NOT NULL DEFAULT 0,
    status       TEXT NOT NULL DEFAULT 'PENDING',
    -- When the event's transaction committed (the relay's drain order).
    created_at   TIMESTAMPTZ NOT NULL,
    -- When the broker acked the record; null until PUBLISHED.
    published_at TIMESTAMPTZ,
    -- The last relay failure's detail (truncated), for the DLQ record.
    last_error   TEXT,

    CONSTRAINT chk_chain_job_events_outbox_status CHECK (status IN
        ('PENDING', 'PUBLISHED', 'FAILED'))
);

-- The claim scan: oldest PENDING first.
CREATE INDEX idx_chain_job_events_outbox_status_created
    ON chain_job_events_outbox (status, created_at);
