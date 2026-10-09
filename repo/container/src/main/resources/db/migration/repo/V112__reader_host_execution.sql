-- Internal lifecycle foundation. Fencing stops admission, not in-flight provider work.
-- External termination evidence and reclamation are deliberately separate operations.
CREATE TABLE repository_reader_host_executions (
    execution UUID PRIMARY KEY,
    host_identity TEXT NOT NULL CHECK(length(host_identity) BETWEEN 1 AND 512),
    boot_identity TEXT NOT NULL CHECK(length(boot_identity) BETWEEN 1 AND 512),
    state TEXT NOT NULL CHECK(state IN ('ACTIVE','FENCED'))
);

CREATE FUNCTION guard_repository_reader_host_execution() RETURNS trigger LANGUAGE plpgsql AS $$
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
        OR NOT (NEW.state=OLD.state OR (OLD.state='ACTIVE' AND NEW.state='FENCED')) THEN
        RAISE EXCEPTION 'Host execution identity and fence are permanent';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER repository_reader_host_execution_guard BEFORE INSERT OR UPDATE OR DELETE
    ON repository_reader_host_executions FOR EACH ROW EXECUTE FUNCTION guard_repository_reader_host_execution();

CREATE FUNCTION require_active_repository_reader_host(id UUID) RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
DECLARE current_state TEXT;
BEGIN
    PERFORM require_repository_read_committed();
    SELECT state INTO current_state FROM repository_reader_host_executions WHERE execution=id FOR SHARE;
    IF NOT FOUND OR current_state <> 'ACTIVE' THEN RAISE EXCEPTION 'Reader host execution is not ACTIVE'; END IF;
END;
$$;

CREATE FUNCTION fence_repository_reader_host(id UUID) RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE AS $$
DECLARE current_state TEXT;
BEGIN
    PERFORM require_repository_read_committed();
    SELECT state INTO current_state FROM repository_reader_host_executions WHERE execution=id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'Reader host execution is unknown'; END IF;
    IF current_state='ACTIVE' THEN
        UPDATE repository_reader_host_executions SET state='FENCED' WHERE execution=id;
    END IF;
    RETURN true;
END;
$$;

ALTER TABLE repository_reader_incarnations ADD COLUMN host_execution UUID
    REFERENCES repository_reader_host_executions(execution);
CREATE INDEX repository_readers_by_host ON repository_reader_incarnations(host_execution,incarnation)
    WHERE host_execution IS NOT NULL;

CREATE FUNCTION guard_repository_reader_host_binding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        IF NEW.host_execution IS NOT NULL AND NEW.state <> 'QUIESCED' THEN
            PERFORM require_active_repository_reader_host(NEW.host_execution);
        END IF;
    ELSIF NEW.host_execution IS DISTINCT FROM OLD.host_execution THEN
        RAISE EXCEPTION 'Reader host binding is immutable';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER repository_reader_host_binding BEFORE INSERT OR UPDATE ON repository_reader_incarnations
    FOR EACH ROW EXECUTE FUNCTION guard_repository_reader_host_binding();

CREATE OR REPLACE FUNCTION require_active_repository_reader(id UUID) RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
DECLARE bound_host UUID; locked_host UUID; current_state TEXT;
BEGIN
    PERFORM require_repository_read_committed();
    -- Binding is immutable from INSERT. Missing/uncommitted rows fail; they cannot
    -- be mistaken for local-only registrations. Lock host before reader/object.
    SELECT host_execution INTO bound_host FROM repository_reader_incarnations WHERE incarnation=id;
    IF NOT FOUND THEN RAISE EXCEPTION 'Reader incarnation is not ACTIVE'; END IF;
    IF bound_host IS NOT NULL THEN PERFORM require_active_repository_reader_host(bound_host); END IF;
    SELECT state,host_execution INTO current_state,locked_host
        FROM repository_reader_incarnations WHERE incarnation=id FOR SHARE;
    IF NOT FOUND OR current_state <> 'ACTIVE' OR locked_host IS DISTINCT FROM bound_host THEN
        RAISE EXCEPTION 'Reader incarnation is not ACTIVE';
    END IF;
END;
$$;

-- A failed constructor never exposes a reader or starts provider work. Reserve
-- its identity directly as a closed tombstone, including after a host fence.
-- These internal SQL operations have the same trusted LOCAL_DRAIN boundary as V32.
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
    RETURN NEW;
END;
$$;

