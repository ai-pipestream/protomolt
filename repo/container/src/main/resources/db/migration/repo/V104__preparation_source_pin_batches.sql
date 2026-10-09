-- Append-only captured pin evidence. No release permission and no legacy backfill.
CREATE TABLE repository_preparation_pin_batches (
 account_id varchar(200) NOT NULL,
 principal varchar(200) NOT NULL,
 operation_id uuid NOT NULL,
 predecessor_generation bigint NOT NULL,
 pins_sha256 bytea NOT NULL CHECK(octet_length(pins_sha256)=32),
 expected_count integer NOT NULL CHECK(expected_count BETWEEN 1 AND 10000),
 initial_capture boolean NOT NULL,
 creation_xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
 sealed boolean NOT NULL DEFAULT false,
 PRIMARY KEY(account_id,principal,operation_id,predecessor_generation,pins_sha256),
 FOREIGN KEY(account_id,principal,operation_id,predecessor_generation)
  REFERENCES repository_preparation_history_sets
);

CREATE TABLE repository_preparation_source_pins (
 account_id varchar(200) NOT NULL,
 principal varchar(200) NOT NULL,
 operation_id uuid NOT NULL,
 predecessor_generation bigint NOT NULL,
 pins_sha256 bytea NOT NULL,
 pin_id uuid NOT NULL,
 reader_incarnation uuid NOT NULL REFERENCES repository_reader_incarnations(incarnation),
 object_id uuid NOT NULL,
 node_id uuid NOT NULL,
 revision_id uuid NOT NULL,
 publication_revision bigint NOT NULL CHECK(publication_revision>0),
 PRIMARY KEY(account_id,principal,operation_id,predecessor_generation,pins_sha256,pin_id),
 FOREIGN KEY(account_id,principal,operation_id,predecessor_generation,pins_sha256)
  REFERENCES repository_preparation_pin_batches
);
-- Deliberately no live pin/object/revision FK: this evidence survives their release.
CREATE INDEX repository_preparation_source_pin_identity ON repository_preparation_source_pins(pin_id);

CREATE FUNCTION guard_repository_preparation_pin_batch() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE source_set repository_preparation_history_sets%ROWTYPE; actual_count bigint; actual_sha bytea;
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Preparation pin batches are permanent evidence'; END IF;
 PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id);
 IF TG_OP='INSERT' THEN
  SELECT * INTO STRICT source_set FROM repository_preparation_history_sets
   WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
    AND predecessor_generation=NEW.predecessor_generation;
  IF NOT source_set.sealed OR source_set.expected_count=0 OR NEW.sealed
   OR NEW.creation_xid<>pg_current_xact_id()
   OR NEW.initial_capture IS DISTINCT FROM (source_set.creation_xid=pg_current_xact_id()) THEN
   RAISE EXCEPTION 'Preparation pin batch requires exact retained source creation state';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM repository_preparation_pin_batches
    WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
     AND predecessor_generation=NEW.predecessor_generation AND pins_sha256=NEW.pins_sha256)
   AND (SELECT count(*) FROM repository_preparation_pin_batches
    WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
     AND predecessor_generation=NEW.predecessor_generation)>=16 THEN
   RAISE EXCEPTION 'Preparation source capture batch limit exceeded';
  END IF;
  RETURN NEW;
 END IF;
 IF OLD.sealed OR NOT NEW.sealed OR OLD.creation_xid<>pg_current_xact_id()
  OR (to_jsonb(OLD)-'sealed') IS DISTINCT FROM (to_jsonb(NEW)-'sealed') THEN
  RAISE EXCEPTION 'Preparation pin batch is immutable after sealing';
 END IF;
 SELECT count(*),sha256(convert_to('protomolt/preparation-pins/v1' || E'\n' ||
  coalesce(string_agg(reader_incarnation::text || '/' || pin_id::text || '/' || object_id::text || '/' ||
   node_id::text || '/' || revision_id::text || '/' || publication_revision::text || E'\n','' ORDER BY pin_id),''),'UTF8'))
 INTO actual_count,actual_sha FROM repository_preparation_source_pins
 WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
  AND predecessor_generation=NEW.predecessor_generation AND pins_sha256=NEW.pins_sha256;
 IF actual_count<>NEW.expected_count OR actual_sha<>NEW.pins_sha256 THEN
  RAISE EXCEPTION 'Preparation pin batch count or digest differs';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_preparation_pin_batch_guard BEFORE INSERT OR UPDATE OR DELETE
 ON repository_preparation_pin_batches FOR EACH ROW EXECUTE FUNCTION guard_repository_preparation_pin_batch();

CREATE FUNCTION guard_repository_preparation_source_pin() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE batch repository_preparation_pin_batches%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Preparation source pins are permanent evidence'; END IF;
 PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id);
 SELECT * INTO STRICT batch FROM repository_preparation_pin_batches
 WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
  AND predecessor_generation=NEW.predecessor_generation AND pins_sha256=NEW.pins_sha256 FOR UPDATE;
 IF batch.sealed OR batch.creation_xid<>pg_current_xact_id() THEN
  RAISE EXCEPTION 'Preparation source pins require the unsealed creation transaction';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM repository_preparation_history_roots
  WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
   AND predecessor_generation=NEW.predecessor_generation AND node_id=NEW.node_id AND revision_id=NEW.revision_id) THEN
  RAISE EXCEPTION 'Preparation source pin has no retained source revision';
 END IF;
 -- Host acquires the complete sorted origin and retention sets before all pin locks.
 PERFORM 1 FROM document_read_pins WHERE pin_id=NEW.pin_id AND reader_incarnation=NEW.reader_incarnation
  AND object_id=NEW.object_id AND source_node=NEW.node_id AND source_revision=NEW.revision_id
  AND publication_revision=NEW.publication_revision AND read_scope='HISTORICAL' FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Preparation source pin differs from live historical capture'; END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_preparation_source_pin_guard BEFORE INSERT OR UPDATE OR DELETE
 ON repository_preparation_source_pins FOR EACH ROW EXECUTE FUNCTION guard_repository_preparation_source_pin();

CREATE FUNCTION require_sealed_preparation_pin_batch() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM repository_preparation_pin_batches
  WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
   AND predecessor_generation=NEW.predecessor_generation AND pins_sha256=NEW.pins_sha256 AND sealed) THEN
  RAISE EXCEPTION 'Preparation source pin batch must seal atomically';
 END IF;
 RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER repository_preparation_pin_batch_complete AFTER INSERT
 ON repository_preparation_pin_batches DEFERRABLE INITIALLY DEFERRED
 FOR EACH ROW EXECUTE FUNCTION require_sealed_preparation_pin_batch();

CREATE FUNCTION require_initial_preparation_pin_batch() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.expected_count>0 AND NOT EXISTS(SELECT 1 FROM repository_preparation_pin_batches
  WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
   AND predecessor_generation=NEW.predecessor_generation AND initial_capture AND sealed
   AND creation_xid=NEW.creation_xid) THEN
  RAISE EXCEPTION 'New historical preparation requires its initial source pin batch';
 END IF;
 RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER repository_preparation_initial_pins AFTER INSERT
 ON repository_preparation_history_sets DEFERRABLE INITIALLY DEFERRED
 FOR EACH ROW EXECUTE FUNCTION require_initial_preparation_pin_batch();
