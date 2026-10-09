-- Quiesce one exact reader under a permanent verified host receipt. This operation
-- does not hold a host write lock, scan other readers or release provider pins.
CREATE TABLE repository_reader_external_quiescence (
    incarnation UUID PRIMARY KEY REFERENCES repository_reader_incarnations(incarnation),
    receipt_id UUID NOT NULL UNIQUE,
    registration_nonce UUID NOT NULL,
    host_execution UUID NOT NULL REFERENCES repository_reader_host_executions(execution),
    termination_receipt UUID NOT NULL REFERENCES repository_reader_host_terminations(receipt_id),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE FUNCTION guard_repository_reader_external_quiescence() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE reader repository_reader_incarnations%ROWTYPE;
BEGIN
    PERFORM require_repository_read_committed();
    IF TG_OP <> 'INSERT' THEN RAISE EXCEPTION 'External reader quiescence receipts are immutable'; END IF;
    SELECT * INTO STRICT reader FROM repository_reader_incarnations WHERE incarnation=NEW.incarnation FOR UPDATE;
    IF reader.state <> 'FENCED' OR ROW(reader.registration_nonce,reader.host_execution)
        IS DISTINCT FROM ROW(NEW.registration_nonce,NEW.host_execution) THEN
        RAISE EXCEPTION 'External quiescence requires exact fenced reader registration';
    END IF;
    IF NOT EXISTS(SELECT 1 FROM repository_reader_host_terminations t
        JOIN repository_reader_host_executions h ON h.execution=t.execution AND h.state='TERMINATED'
        WHERE t.execution=NEW.host_execution AND t.receipt_id=NEW.termination_receipt) THEN
        RAISE EXCEPTION 'External quiescence requires verified host termination';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER repository_reader_external_quiescence_guard BEFORE INSERT OR UPDATE OR DELETE
    ON repository_reader_external_quiescence FOR EACH ROW EXECUTE FUNCTION guard_repository_reader_external_quiescence();

ALTER TABLE repository_reader_incarnations DROP CONSTRAINT reader_quiescence_evidence;
ALTER TABLE repository_reader_incarnations ADD CONSTRAINT reader_quiescence_evidence
    CHECK ((state='QUIESCED' AND quiesced_at IS NOT NULL AND quiescence_source IS NOT NULL
            AND quiescence_source IN ('LOCAL_DRAIN','HOST_TERMINATION'))
        OR (state<>'QUIESCED' AND quiesced_at IS NULL AND quiescence_source IS NULL));

CREATE OR REPLACE FUNCTION guard_repository_reader_incarnation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM require_repository_read_committed();
    IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Reader incarnation tombstones are permanent'; END IF;
    IF TG_OP='INSERT' THEN
        IF NEW.state <> 'ACTIVE' AND NOT (NEW.state='QUIESCED'
            AND NEW.quiescence_source='LOCAL_DRAIN' AND NEW.quiesced_at IS NOT NULL) THEN
            RAISE EXCEPTION 'New reader must be ACTIVE or a locally drained registration tombstone';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.incarnation IS DISTINCT FROM OLD.incarnation OR
        NOT (NEW.state=OLD.state OR (OLD.state='ACTIVE' AND NEW.state='FENCED')
            OR (OLD.state='FENCED' AND NEW.state='QUIESCED')) THEN
        RAISE EXCEPTION 'Reader incarnation identity and fence are permanent';
    END IF;
    IF OLD.state='QUIESCED' AND (NEW.quiesced_at IS DISTINCT FROM OLD.quiesced_at
        OR NEW.quiescence_source IS DISTINCT FROM OLD.quiescence_source) THEN
        RAISE EXCEPTION 'Reader quiescence evidence is immutable';
    END IF;
    IF NEW.state='QUIESCED' AND NEW.quiescence_source='HOST_TERMINATION' AND NOT EXISTS(
        SELECT 1 FROM repository_reader_external_quiescence q WHERE q.incarnation=NEW.incarnation
            AND q.registration_nonce=NEW.registration_nonce AND q.host_execution=NEW.host_execution) THEN
        RAISE EXCEPTION 'External quiescence requires a matching reader receipt';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION require_committed_reader_external_quiescence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS(SELECT 1 FROM repository_reader_incarnations WHERE incarnation=NEW.incarnation
        AND state='QUIESCED' AND quiescence_source='HOST_TERMINATION') THEN
        RAISE EXCEPTION 'External reader receipt must commit with HOST_TERMINATION quiescence';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER repository_reader_external_quiescence_complete AFTER INSERT
    ON repository_reader_external_quiescence DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION require_committed_reader_external_quiescence();

CREATE OR REPLACE FUNCTION attest_local_reader_quiescence(id UUID) RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE AS $$
DECLARE reader repository_reader_incarnations%ROWTYPE;
BEGIN
    PERFORM require_repository_read_committed();
    SELECT * INTO reader FROM repository_reader_incarnations WHERE incarnation=id FOR UPDATE;
    IF NOT FOUND OR reader.state NOT IN ('FENCED','QUIESCED') THEN
        RAISE EXCEPTION 'Reader quiescence requires a fenced incarnation';
    END IF;
    IF reader.state='QUIESCED' AND reader.quiescence_source <> 'LOCAL_DRAIN' THEN
        RAISE EXCEPTION 'External quiescence cannot be claimed as local drain';
    END IF;
    IF reader.state='FENCED' THEN
        UPDATE repository_reader_incarnations SET state='QUIESCED',quiesced_at=clock_timestamp(),
            quiescence_source='LOCAL_DRAIN' WHERE incarnation=id;
    END IF;
    RETURN true;
END;
$$;

CREATE FUNCTION quiesce_repository_reader_from_host(p_reader UUID,p_nonce UUID,p_host UUID,p_termination UUID)
RETURNS UUID LANGUAGE plpgsql VOLATILE AS $$
DECLARE reader repository_reader_incarnations%ROWTYPE; prior repository_reader_external_quiescence%ROWTYPE;
    receipt UUID;
BEGIN
    PERFORM require_repository_read_committed();
    IF p_reader IS NULL OR p_nonce IS NULL OR p_host IS NULL OR p_termination IS NULL THEN
        RAISE EXCEPTION 'External quiescence requires complete identity';
    END IF;
    -- Host termination is permanent. Do not retain a host lock while waiting for a reader.
    IF NOT EXISTS(SELECT 1 FROM repository_reader_host_terminations t
        JOIN repository_reader_host_executions h ON h.execution=t.execution AND h.state='TERMINATED'
        WHERE t.execution=p_host AND t.receipt_id=p_termination) THEN
        RAISE EXCEPTION 'External quiescence requires verified host termination';
    END IF;
    SELECT * INTO reader FROM repository_reader_incarnations WHERE incarnation=p_reader FOR UPDATE;
    IF NOT FOUND OR ROW(reader.registration_nonce,reader.host_execution) IS DISTINCT FROM ROW(p_nonce,p_host) THEN
        RAISE EXCEPTION 'External quiescence reader registration mismatch';
    END IF;
    SELECT * INTO prior FROM repository_reader_external_quiescence WHERE incarnation=p_reader;
    IF FOUND THEN
        IF ROW(prior.registration_nonce,prior.host_execution,prior.termination_receipt)
            IS DISTINCT FROM ROW(p_nonce,p_host,p_termination)
            OR reader.state <> 'QUIESCED' OR reader.quiescence_source <> 'HOST_TERMINATION' THEN
            RAISE EXCEPTION 'Conflicting external reader quiescence';
        END IF;
        RETURN prior.receipt_id;
    END IF;
    IF reader.state NOT IN ('ACTIVE','FENCED') THEN
        RAISE EXCEPTION 'External quiescence cannot replace existing reader provenance';
    END IF;
    PERFORM fence_repository_reader(p_reader);
    receipt := gen_random_uuid();
    INSERT INTO repository_reader_external_quiescence(incarnation,receipt_id,registration_nonce,host_execution,termination_receipt)
        VALUES(p_reader,receipt,p_nonce,p_host,p_termination);
    UPDATE repository_reader_incarnations SET state='QUIESCED',quiesced_at=clock_timestamp(),quiescence_source='HOST_TERMINATION'
        WHERE incarnation=p_reader;
    RETURN receipt;
END;
$$;
