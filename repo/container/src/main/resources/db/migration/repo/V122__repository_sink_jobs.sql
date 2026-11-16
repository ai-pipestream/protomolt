-- Durable repository sink jobs. One row per (account, operation); job_pb,
-- request_pb and pending_provenance_pb hold the protobuf records. owner_id,
-- lease_epoch and lease_expires_at fence each owner write, and a peer may
-- claim the row only after the lease expires, bumping lease_epoch.
-- cancel_requested lets a peer ask the live owner to stop without
-- overwriting its state.

CREATE SCHEMA repo_sink;

CREATE TABLE repo_sink.sink_jobs (
    account_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    state TEXT NOT NULL,
    job_pb BYTEA NOT NULL,
    request_pb BYTEA NOT NULL,
    pending_provenance_pb BYTEA,
    owner_id TEXT NOT NULL DEFAULT 'unowned',
    lease_epoch BIGINT NOT NULL DEFAULT 1,
    lease_expires_at TIMESTAMPTZ NOT NULL DEFAULT TIMESTAMP WITH TIME ZONE '1970-01-01 00:00:00+00',
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    cancel_reason TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, operation_id)
);
