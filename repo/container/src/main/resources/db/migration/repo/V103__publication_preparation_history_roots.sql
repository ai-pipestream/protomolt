-- A missing projection is UNKNOWN, never an empty historical source set.
-- Existing preparations are deliberately not backfilled. This does not enable
-- historical execution or pruning; release needs a separately fenced protocol.
CREATE TABLE repository_preparation_history_sets (
 account_id varchar(200) NOT NULL,
 principal varchar(200) NOT NULL,
 operation_id uuid NOT NULL,
 predecessor_generation bigint NOT NULL,
 preparation_sha256 bytea NOT NULL CHECK(octet_length(preparation_sha256)=32),
 command_sha256 bytea NOT NULL CHECK(octet_length(command_sha256)=32),
 expected_count integer NOT NULL CHECK(expected_count BETWEEN 0 AND 10000),
 roots_sha256 bytea NOT NULL CHECK(octet_length(roots_sha256)=32),
 creation_xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
 sealed boolean NOT NULL DEFAULT false,
 PRIMARY KEY(account_id,principal,operation_id,predecessor_generation),
 FOREIGN KEY(account_id,principal,operation_id,predecessor_generation)
  REFERENCES repository_publication_preparations
);

CREATE TABLE repository_preparation_history_roots (
 account_id varchar(200) NOT NULL,
 principal varchar(200) NOT NULL,
 operation_id uuid NOT NULL,
 predecessor_generation bigint NOT NULL,
 node_id uuid NOT NULL,
 revision_id uuid NOT NULL,
 PRIMARY KEY(account_id,principal,operation_id,predecessor_generation,node_id,revision_id),
 FOREIGN KEY(account_id,principal,operation_id,predecessor_generation)
  REFERENCES repository_preparation_history_sets,
 FOREIGN KEY(node_id,revision_id) REFERENCES document_revision_publications(node_id,revision_id) ON DELETE RESTRICT
);
CREATE INDEX repository_preparation_history_revision ON repository_preparation_history_roots(node_id,revision_id);

CREATE FUNCTION guard_repository_preparation_history_set() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE actual_count bigint; actual_sha bytea;
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Preparation history retention requires qualified release'; END IF;
 PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id);
 IF TG_OP='INSERT' THEN
  IF NEW.sealed OR NEW.creation_xid<>pg_current_xact_id() THEN
   RAISE EXCEPTION 'Preparation history set must start unsealed in its creation transaction';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM repository_publication_preparations p
   WHERE p.account_id=NEW.account_id AND p.principal=NEW.principal AND p.operation_id=NEW.operation_id
    AND p.predecessor_generation=NEW.predecessor_generation
    AND p.preparation_sha256=NEW.preparation_sha256 AND p.command_sha256=NEW.command_sha256) THEN
   RAISE EXCEPTION 'Preparation history set differs from sealed command identity';
  END IF;
  RETURN NEW;
 END IF;
 IF OLD.sealed OR NOT NEW.sealed OR OLD.creation_xid<>pg_current_xact_id()
  OR (to_jsonb(OLD)-'sealed') IS DISTINCT FROM (to_jsonb(NEW)-'sealed') THEN
  RAISE EXCEPTION 'Preparation history set is immutable after sealing';
 END IF;
 SELECT count(*),sha256(convert_to('protomolt/preparation-history/v1' || E'\n' ||
  coalesce(string_agg(node_id::text || '/' || revision_id::text || E'\n','' ORDER BY node_id,revision_id),''),'UTF8'))
 INTO actual_count,actual_sha FROM repository_preparation_history_roots
 WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
  AND predecessor_generation=NEW.predecessor_generation;
 IF actual_count<>NEW.expected_count OR actual_sha<>NEW.roots_sha256 THEN
  RAISE EXCEPTION 'Preparation history source count or digest differs';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_preparation_history_set_guard BEFORE INSERT OR UPDATE OR DELETE
 ON repository_preparation_history_sets FOR EACH ROW EXECUTE FUNCTION guard_repository_preparation_history_set();

CREATE FUNCTION guard_repository_preparation_history_root() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE owner repository_preparation_history_sets%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Preparation history roots require qualified release'; END IF;
 PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id);
 SELECT * INTO STRICT owner FROM repository_preparation_history_sets
 WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
  AND predecessor_generation=NEW.predecessor_generation FOR UPDATE;
 IF owner.sealed OR owner.creation_xid<>pg_current_xact_id() THEN
  RAISE EXCEPTION 'Preparation history roots require the unsealed creation transaction';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM document_revision_publications r WHERE r.node_id=NEW.node_id
  AND r.revision_id=NEW.revision_id AND r.projection_sealed AND r.body->>'account_id'=NEW.account_id) THEN
  RAISE EXCEPTION 'Preparation history root requires an exact sealed source in its account';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_preparation_history_root_guard BEFORE INSERT OR UPDATE OR DELETE
 ON repository_preparation_history_roots FOR EACH ROW EXECUTE FUNCTION guard_repository_preparation_history_root();

CREATE FUNCTION require_sealed_preparation_history_set() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM repository_preparation_history_sets
  WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
   AND predecessor_generation=NEW.predecessor_generation AND sealed) THEN
  RAISE EXCEPTION 'Preparation history source set must seal atomically';
 END IF;
 RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER repository_preparation_history_set_complete AFTER INSERT
 ON repository_preparation_history_sets DEFERRABLE INITIALLY DEFERRED
 FOR EACH ROW EXECUTE FUNCTION require_sealed_preparation_history_set();
