-- A transaction-local proof that the owner row was written (and therefore
-- locked) before dependent mutations. No attempt is bound by this migration.
ALTER TABLE repository_operation_owners ADD COLUMN write_fence_xid xid8;

CREATE FUNCTION stamp_repository_operation_write_fence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- Never accept a caller-provided transaction identifier. This follows the
    -- existing owner guard; same-generation writes still require a live owner.
    NEW.write_fence_xid := pg_current_xact_id();
    RETURN NEW;
END;
$$;
CREATE TRIGGER repository_operation_write_stamp
    BEFORE INSERT OR UPDATE ON repository_operation_owners
    FOR EACH ROW EXECUTE FUNCTION stamp_repository_operation_write_fence();

CREATE FUNCTION require_repository_operation_write_fence(
    scoped_account text, scoped_principal text, scoped_operation uuid, scoped_generation bigint)
RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
    PERFORM require_repository_read_committed();
    -- Deliberately no locking SELECT here: dependent row triggers can run after
    -- their target row is locked. A missing proof must fail, never wait on owner.
    IF NOT EXISTS (
        SELECT 1 FROM repository_operation_owners
        WHERE account_id=scoped_account AND principal=scoped_principal AND operation_id=scoped_operation
          AND owner_generation=scoped_generation
          AND write_fence_xid=pg_current_xact_id_if_assigned()
          AND lease_until > clock_timestamp()
    ) THEN
        RAISE EXCEPTION 'Repository mutation requires a live owner write fence in this transaction';
    END IF;
    RETURN true;
END;
$$;
