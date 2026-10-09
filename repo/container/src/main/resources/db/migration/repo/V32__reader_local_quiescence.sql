-- Only trusted owning lifecycle code may attest LOCAL_DRAIN. SQL cannot prove
-- provider termination; UNKNOWN and unproven crashed owners remain protected.
ALTER TABLE repository_reader_incarnations DROP CONSTRAINT repository_reader_incarnations_state_check;
ALTER TABLE repository_reader_incarnations ADD CONSTRAINT repository_reader_incarnations_state_check
    CHECK(state IN ('ACTIVE','FENCED','UNKNOWN','QUIESCED'));
ALTER TABLE repository_reader_incarnations ADD COLUMN quiesced_at TIMESTAMPTZ;
ALTER TABLE repository_reader_incarnations ADD COLUMN quiescence_source TEXT;
ALTER TABLE repository_reader_incarnations ADD CONSTRAINT reader_quiescence_evidence
    CHECK ((state='QUIESCED' AND quiesced_at IS NOT NULL AND quiescence_source IS NOT NULL
            AND quiescence_source='LOCAL_DRAIN')
        OR (state<>'QUIESCED' AND quiesced_at IS NULL AND quiescence_source IS NULL));
CREATE INDEX repository_quiesced_readers ON repository_reader_incarnations(incarnation) WHERE state='QUIESCED';

CREATE OR REPLACE FUNCTION guard_repository_reader_incarnation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM require_repository_read_committed();
    IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Reader incarnation tombstones are permanent'; END IF;
    IF TG_OP='INSERT' THEN
        IF NEW.state <> 'ACTIVE' THEN RAISE EXCEPTION 'New reader incarnation must be ACTIVE'; END IF;
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
    RETURN NEW;
END;
$$;

CREATE FUNCTION attest_local_reader_quiescence(id UUID) RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE AS $$
DECLARE current_state TEXT;
BEGIN
    PERFORM require_repository_read_committed();
    SELECT state INTO current_state FROM repository_reader_incarnations WHERE incarnation=id FOR UPDATE;
    IF NOT FOUND OR current_state NOT IN ('FENCED','QUIESCED') THEN
        RAISE EXCEPTION 'Reader quiescence requires a fenced incarnation';
    END IF;
    IF current_state='FENCED' THEN
        UPDATE repository_reader_incarnations SET state='QUIESCED',quiesced_at=clock_timestamp(),
            quiescence_source='LOCAL_DRAIN' WHERE incarnation=id;
    END IF;
    RETURN true;
END;
$$;

CREATE FUNCTION recover_quiesced_archive_read_pin(p_pin UUID,p_reader UUID,p_object UUID)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    PERFORM require_repository_read_committed();
    -- QUIESCED is permanent. No incarnation lock is needed across object work.
    IF NOT EXISTS(SELECT 1 FROM repository_reader_incarnations WHERE incarnation=p_reader AND state='QUIESCED') THEN
        RAISE EXCEPTION 'Reader recovery requires proven quiescence';
    END IF;
    RETURN release_archive_read_pin(p_pin,p_reader,p_object);
END;
$$;
