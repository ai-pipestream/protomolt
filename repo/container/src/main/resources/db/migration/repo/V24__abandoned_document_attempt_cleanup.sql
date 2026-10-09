-- Abandoned attempts only. Publication history remains a permanent retention pin.
-- ABSENT records survive so a later sweep can observe bytes from a late writer.
CREATE TABLE document_part_attempt_cleanup (
    attempt_id UUID PRIMARY KEY REFERENCES document_part_attempts(attempt_id),
    cleanup_token UUID NOT NULL,
    claim_until TIMESTAMPTZ NOT NULL,
    state VARCHAR(16) NOT NULL CHECK (state IN ('DELETING', 'ABSENT')),
    last_checked_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    last_error VARCHAR(2048),
    absence_observed_at TIMESTAMPTZ,
    CHECK (state <> 'ABSENT' OR (absence_observed_at IS NOT NULL AND last_error IS NULL))
);
CREATE INDEX document_part_cleanup_due ON document_part_attempt_cleanup(last_checked_at, attempt_id);

CREATE FUNCTION protect_document_attempt_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE owner document_part_attempts%ROWTYPE;
BEGIN
    IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Document cleanup tombstone cannot be deleted'; END IF;
    IF TG_OP='UPDATE' AND NEW.attempt_id IS DISTINCT FROM OLD.attempt_id THEN
        RAISE EXCEPTION 'Document cleanup identity is immutable';
    END IF;
    SELECT * INTO STRICT owner FROM document_part_attempts WHERE attempt_id=NEW.attempt_id FOR UPDATE;
    IF owner.lease_until > clock_timestamp() OR owner.state='PLANNING' THEN
        RAISE EXCEPTION 'Document cleanup requires an expired sealed attempt';
    END IF;
    IF EXISTS (SELECT 1 FROM document_part_publication_history WHERE attempt_id=NEW.attempt_id) THEN
        RAISE EXCEPTION 'Published document attempts are retained';
    END IF;
    IF TG_OP='INSERT' OR NEW.cleanup_token IS DISTINCT FROM OLD.cleanup_token THEN
        IF TG_OP='UPDATE' AND OLD.claim_until > clock_timestamp() THEN
            RAISE EXCEPTION 'Document cleanup claim is still active';
        END IF;
        IF NEW.state <> 'DELETING' OR NEW.claim_until <= clock_timestamp()
           OR NEW.absence_observed_at IS NOT NULL OR NEW.last_error IS NOT NULL THEN
            RAISE EXCEPTION 'Document cleanup requires a fresh deletion claim';
        END IF;
    ELSE
        IF OLD.claim_until <= clock_timestamp() THEN RAISE EXCEPTION 'Document cleanup claim expired'; END IF;
        IF NEW.claim_until > OLD.claim_until AND (NEW.state <> OLD.state
           OR NEW.absence_observed_at IS DISTINCT FROM OLD.absence_observed_at
           OR NEW.last_error IS DISTINCT FROM OLD.last_error) THEN
            RAISE EXCEPTION 'Document cleanup renewal cannot record a result';
        END IF;
        IF NEW.state='ABSENT' AND NEW.claim_until > clock_timestamp() THEN
            RAISE EXCEPTION 'Document cleanup absence must release its claim';
        END IF;
    END IF;
    NEW.last_checked_at := clock_timestamp();
    RETURN NEW;
END;
$$;
CREATE TRIGGER document_attempt_cleanup_guard BEFORE INSERT OR UPDATE OR DELETE ON document_part_attempt_cleanup
FOR EACH ROW EXECUTE FUNCTION protect_document_attempt_cleanup();

CREATE FUNCTION reject_cleaned_document_publication() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM document_part_attempts WHERE attempt_id=NEW.attempt_id FOR UPDATE;
    IF EXISTS (SELECT 1 FROM document_part_attempt_cleanup WHERE attempt_id=NEW.attempt_id) THEN
        RAISE EXCEPTION 'Document cleanup has permanently fenced this attempt from publication';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER document_publication_cleanup_guard BEFORE INSERT ON document_part_publication_history
FOR EACH ROW EXECUTE FUNCTION reject_cleaned_document_publication();
CREATE TRIGGER document_publication_cleanup_switch BEFORE INSERT OR UPDATE ON document_part_publications
FOR EACH ROW EXECUTE FUNCTION reject_cleaned_document_publication();
