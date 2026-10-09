-- Trusted Java verifies evidence before entering SQL. SQL enforces identity,
-- atomic recording and replay; a stored digest alone cannot prove process exit.
ALTER TABLE repository_reader_host_executions DROP CONSTRAINT repository_reader_host_executions_state_check;
ALTER TABLE repository_reader_host_executions ADD CONSTRAINT repository_reader_host_executions_state_check
    CHECK(state IN ('ACTIVE','FENCED','TERMINATED'));

CREATE TABLE repository_reader_host_terminations (
    execution UUID PRIMARY KEY REFERENCES repository_reader_host_executions(execution),
    receipt_id UUID NOT NULL UNIQUE,
    host_identity TEXT NOT NULL,
    boot_identity TEXT NOT NULL,
    verifier TEXT NOT NULL CHECK(length(verifier) BETWEEN 1 AND 128),
    proof_format INTEGER NOT NULL CHECK(proof_format > 0),
    attestation_id UUID NOT NULL,
    evidence_sha256 TEXT NOT NULL CHECK(evidence_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(verifier,attestation_id)
);

CREATE FUNCTION guard_repository_reader_host_termination() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE host repository_reader_host_executions%ROWTYPE;
BEGIN
    PERFORM require_repository_read_committed();
    IF TG_OP <> 'INSERT' THEN RAISE EXCEPTION 'Host termination receipts are immutable'; END IF;
    SELECT * INTO STRICT host FROM repository_reader_host_executions WHERE execution=NEW.execution FOR UPDATE;
    IF host.state <> 'FENCED' OR ROW(host.host_identity,host.boot_identity)
        IS DISTINCT FROM ROW(NEW.host_identity,NEW.boot_identity) THEN
        RAISE EXCEPTION 'Host termination requires exact fenced execution';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER repository_reader_host_termination_guard BEFORE INSERT OR UPDATE OR DELETE
    ON repository_reader_host_terminations FOR EACH ROW EXECUTE FUNCTION guard_repository_reader_host_termination();

CREATE OR REPLACE FUNCTION guard_repository_reader_host_execution() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM require_repository_read_committed();
    IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Host execution tombstones are permanent'; END IF;
    IF TG_OP='INSERT' THEN
        IF NEW.state <> 'ACTIVE' THEN RAISE EXCEPTION 'New host execution must be ACTIVE'; END IF;
        RETURN NEW;
    END IF;
    IF NEW.execution IS DISTINCT FROM OLD.execution
        OR NEW.host_identity IS DISTINCT FROM OLD.host_identity
        OR NEW.boot_identity IS DISTINCT FROM OLD.boot_identity
        OR NOT (NEW.state=OLD.state OR (OLD.state='ACTIVE' AND NEW.state='FENCED')
            OR (OLD.state='FENCED' AND NEW.state='TERMINATED')) THEN
        RAISE EXCEPTION 'Host execution identity and fence are permanent';
    END IF;
    IF NEW.state='TERMINATED' AND NOT EXISTS(SELECT 1 FROM repository_reader_host_terminations
        WHERE execution=NEW.execution AND host_identity=NEW.host_identity AND boot_identity=NEW.boot_identity) THEN
        RAISE EXCEPTION 'Host termination requires a matching receipt';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION require_committed_reader_host_termination() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS(SELECT 1 FROM repository_reader_host_executions WHERE execution=NEW.execution AND state='TERMINATED') THEN
        RAISE EXCEPTION 'Host termination receipt must commit with TERMINATED state';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER repository_reader_host_termination_complete AFTER INSERT
    ON repository_reader_host_terminations DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION require_committed_reader_host_termination();
