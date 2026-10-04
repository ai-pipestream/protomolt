-- Recovery lock proof only; no claims, artifacts or outcomes are deleted.
ALTER TABLE repository_operation_owners ADD COLUMN recovery_fence_xid xid8;

CREATE FUNCTION repository_operation_recovery_only(
 old_owner repository_operation_owners, new_owner repository_operation_owners)
RETURNS BOOLEAN LANGUAGE sql IMMUTABLE AS $$
 SELECT new_owner.recovery_fence_xid IS DISTINCT FROM old_owner.recovery_fence_xid
 AND ROW(new_owner.account_id,new_owner.principal,new_owner.operation_id,new_owner.owner_token,
         new_owner.owner_generation,new_owner.lease_until,new_owner.write_fence_xid)
 IS NOT DISTINCT FROM
     ROW(old_owner.account_id,old_owner.principal,old_owner.operation_id,old_owner.owner_token,
         old_owner.owner_generation,old_owner.lease_until,old_owner.write_fence_xid);
$$;

CREATE OR REPLACE FUNCTION protect_repository_operation_owner() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM require_repository_read_committed();
    IF TG_OP = 'UPDATE' AND repository_operation_recovery_only(OLD, NEW) THEN
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Repository operation ownership cannot be deleted';
    END IF;
    IF NEW.lease_until > clock_timestamp() + interval '1 day' THEN
        RAISE EXCEPTION 'Repository operation lease exceeds one day';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.owner_generation <> 1 OR NEW.lease_until <= clock_timestamp() THEN
            RAISE EXCEPTION 'Repository admission requires first live owner';
        END IF;
    ELSE
        IF ROW(NEW.account_id, NEW.principal, NEW.operation_id)
           IS DISTINCT FROM ROW(OLD.account_id, OLD.principal, OLD.operation_id) THEN
            RAISE EXCEPTION 'Repository operation ownership scope is immutable';
        END IF;
        IF NEW.owner_generation = OLD.owner_generation THEN
            IF NEW.owner_token <> OLD.owner_token OR OLD.lease_until <= clock_timestamp()
               OR NEW.lease_until < OLD.lease_until THEN
                RAISE EXCEPTION 'Repository operation renewal requires its live owner';
            END IF;
        ELSE
            IF OLD.owner_generation = 9223372036854775807
               OR NEW.owner_generation <> OLD.owner_generation + 1
               OR NEW.owner_token = OLD.owner_token OR OLD.lease_until > clock_timestamp()
               OR NEW.lease_until <= clock_timestamp() THEN
                RAISE EXCEPTION 'Repository operation takeover requires an expired owner and next generation';
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE OR REPLACE FUNCTION stamp_repository_operation_write_fence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='UPDATE' AND repository_operation_recovery_only(OLD,NEW) THEN
  NEW.recovery_fence_xid := pg_current_xact_id();
  NEW.write_fence_xid := OLD.write_fence_xid;
 ELSE
  NEW.recovery_fence_xid := NULL;
  NEW.write_fence_xid := pg_current_xact_id();
 END IF;
 RETURN NEW;
END;
$$;

CREATE FUNCTION fence_repository_operation_recovery(p_account TEXT,p_principal TEXT,p_operation UUID)
RETURNS BIGINT LANGUAGE plpgsql VOLATILE AS $$
DECLARE generation BIGINT;
BEGIN
 PERFORM require_repository_read_committed();
 -- The sentinel requests recovery proof even when this transaction already stamped
 -- the owner. The BEFORE trigger replaces it with the real transaction identity.
 UPDATE repository_operation_owners SET recovery_fence_xid='0'::xid8
 WHERE account_id=p_account AND principal=p_principal AND operation_id=p_operation
 RETURNING owner_generation INTO generation;
 IF NOT FOUND THEN RAISE EXCEPTION 'Repository recovery requires an existing operation owner'; END IF;
 RETURN generation;
END;
$$;

CREATE FUNCTION require_repository_operation_recovery_fence(p_account TEXT,p_principal TEXT,p_operation UUID)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 PERFORM require_repository_read_committed();
 -- No owner lock acquisition from a dependent-row trigger.
 IF NOT EXISTS(SELECT 1 FROM repository_operation_owners
  WHERE account_id=p_account AND principal=p_principal AND operation_id=p_operation
   AND recovery_fence_xid=pg_current_xact_id_if_assigned()) THEN
  RAISE EXCEPTION 'Repository recovery requires an owner recovery fence in this transaction';
 END IF;
 RETURN TRUE;
END;
$$;
