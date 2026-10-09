-- Pruning prerequisite only: no prune state, reference release or provider deletion.
-- Java prelocks complete ordered document sets before physical origins. Direct
-- SQL insertions must not bypass that boundary. Never wait for an earlier lock
-- from a row trigger: callers may already hold claims, origins or retention rows.
CREATE FUNCTION require_document_source_acquisition(p_node uuid)
RETURNS boolean LANGUAGE plpgsql VOLATILE AS $$
DECLARE bits text; lock_key bigint;
BEGIN
 PERFORM require_repository_read_committed();
 IF p_node IS NULL THEN RAISE EXCEPTION 'Document source identity is required'; END IF;
 bits:=replace(p_node::text,'-','');
 -- Exact signed 64-bit key used by DocumentRevisionLocks: UUID MSB XOR LSB.
 -- Use the one-bigint namespace, not the separate two-integer advisory namespace.
 lock_key:=('x'||substr(bits,1,16))::bit(64)::bigint
          # ('x'||substr(bits,17,16))::bit(64)::bigint;
 IF NOT pg_try_advisory_xact_lock_shared(lock_key) THEN
  RAISE EXCEPTION 'Historical source acquisition conflicts with document mutation'
   USING ERRCODE='40001';
 END IF;
 RETURN true;
END;
$$;

CREATE FUNCTION fence_document_read_pin_acquisition()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 -- Both scopes: a CURRENT capture can remain live after the pointer changes.
 PERFORM require_document_source_acquisition(NEW.source_node);
 RETURN NEW;
END;
$$;
CREATE TRIGGER a0_document_read_pin_acquisition BEFORE INSERT ON document_read_pins
 FOR EACH ROW EXECUTE FUNCTION fence_document_read_pin_acquisition();

CREATE FUNCTION fence_preparation_history_root_acquisition()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM require_document_source_acquisition(NEW.node_id);
 RETURN NEW;
END;
$$;
CREATE TRIGGER a0_preparation_history_root_acquisition BEFORE INSERT ON repository_preparation_history_roots
 FOR EACH ROW EXECUTE FUNCTION fence_preparation_history_root_acquisition();
