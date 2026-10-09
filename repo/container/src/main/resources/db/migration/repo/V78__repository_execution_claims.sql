-- Private coordinator authority primitive. Existing publication paths are not
-- registered or protected by this table until their mutation fences integrate it.
-- No FK to repository_operations: preparation must precede uncertain admission.
CREATE TABLE repository_execution_claims (
    account_id VARCHAR(200) NOT NULL CHECK (btrim(account_id) <> ''),
    principal VARCHAR(200) NOT NULL CHECK (btrim(principal) <> ''),
    operation_id UUID NOT NULL,
    command_sha256 BYTEA NOT NULL CHECK (octet_length(command_sha256)=32),
    claim_epoch BIGINT NOT NULL CHECK (claim_epoch > 0),
    claim_token UUID NOT NULL,
    lease_until TIMESTAMPTZ NOT NULL,
    PRIMARY KEY(account_id,principal,operation_id)
);

CREATE FUNCTION guard_repository_execution_claim() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM require_repository_read_committed();
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'Execution claim deletion requires a future retention protocol';
    END IF;
    IF NOT isfinite(NEW.lease_until) OR NEW.lease_until <= clock_timestamp()
        OR NEW.lease_until > clock_timestamp() + interval '1 day' THEN
        RAISE EXCEPTION 'Execution claim requires a bounded live lease';
    END IF;
    IF TG_OP='INSERT' THEN
        IF NEW.claim_epoch <> 1 THEN RAISE EXCEPTION 'Execution claim must start at epoch one'; END IF;
    ELSE
        IF (NEW.account_id,NEW.principal,NEW.operation_id,NEW.command_sha256)
            IS DISTINCT FROM (OLD.account_id,OLD.principal,OLD.operation_id,OLD.command_sha256) THEN
            RAISE EXCEPTION 'Execution claim identity is immutable';
        END IF;
        IF NEW.claim_epoch=OLD.claim_epoch THEN
            IF NEW.claim_token <> OLD.claim_token OR OLD.lease_until <= clock_timestamp()
                OR NEW.lease_until < OLD.lease_until THEN
                RAISE EXCEPTION 'Execution claim renewal requires the current live identity';
            END IF;
        ELSIF OLD.claim_epoch < 9223372036854775807 AND NEW.claim_epoch=OLD.claim_epoch+1 THEN
            IF NEW.claim_token=OLD.claim_token OR OLD.lease_until > clock_timestamp() THEN
                RAISE EXCEPTION 'Execution claim transfer requires expired predecessor and fresh token';
            END IF;
        ELSE
            RAISE EXCEPTION 'Execution claim epoch must advance exactly once';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER repository_execution_claim_guard BEFORE INSERT OR UPDATE OR DELETE
    ON repository_execution_claims FOR EACH ROW EXECUTE FUNCTION guard_repository_execution_claim();
