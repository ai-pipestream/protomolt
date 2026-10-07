-- Private retention completion. Java must additionally validate the canonical
-- preparation, receipts and selector coverage before inserting this evidence.
-- No live object/revision FK: confirmation must survive independent pruning.
CREATE TABLE repository_preparation_root_releases (
 account_id varchar(200) NOT NULL,
 principal varchar(200) NOT NULL,
 operation_id uuid NOT NULL,
 predecessor_generation bigint NOT NULL,
 preparation_sha256 bytea NOT NULL CHECK(octet_length(preparation_sha256)=32),
 command_sha256 bytea NOT NULL CHECK(octet_length(command_sha256)=32),
 root_count integer NOT NULL CHECK(root_count BETWEEN 1 AND 10000),
 roots_sha256 bytea NOT NULL CHECK(octet_length(roots_sha256)=32),
 capture_count integer NOT NULL CHECK(capture_count BETWEEN 1 AND 16),
 captures_sha256 bytea NOT NULL CHECK(octet_length(captures_sha256)=32),
 terminal_kind text NOT NULL CHECK(terminal_kind IN ('SUCCESS','REJECTION','ABANDONMENT')),
 terminal_generation bigint,
 terminal_sha256 bytea,
 terminal_xid xid8,
 abandonment_token uuid,
 abandonment_nonce uuid,
 creation_xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
 PRIMARY KEY(account_id,principal,operation_id,predecessor_generation),
 FOREIGN KEY(account_id,principal,operation_id,predecessor_generation)
  REFERENCES repository_preparation_history_sets,
 CHECK ((terminal_kind='ABANDONMENT' AND terminal_generation IS NULL AND terminal_sha256 IS NULL
   AND terminal_xid IS NULL AND abandonment_token IS NOT NULL AND abandonment_nonce IS NOT NULL)
  OR (terminal_kind IN ('SUCCESS','REJECTION') AND terminal_generation IS NOT NULL
   AND terminal_generation>predecessor_generation AND terminal_sha256 IS NOT NULL
   AND octet_length(terminal_sha256)=32 AND terminal_xid IS NOT NULL
   AND abandonment_token IS NULL AND abandonment_nonce IS NULL))
);

-- Call only while holding the operation claim and target header locks.
-- Includes every immutable capture owner and completion identity in digest order.
CREATE FUNCTION repository_preparation_capture_fingerprint(a varchar,p varchar,o uuid,g bigint)
RETURNS bytea LANGUAGE sql STABLE AS $$
 SELECT sha256(convert_to('protomolt/preparation-drains/v1' || E'\n' || coalesce(string_agg(
  encode(b.pins_sha256,'hex') || '/' || b.expected_count::text || '/' || b.initial_capture::text || '/' ||
  b.creation_xid::text || '/' || w.claim_epoch::text || '/' || w.claim_token::text || '/' || w.incarnation::text || '/' ||
  d.claim_epoch::text || '/' || d.claim_token::text || '/' || d.incarnation::text || '/' ||
  encode(d.command_sha256,'hex') || '/' || d.drain_kind || E'\n','' ORDER BY b.pins_sha256),''),'UTF8'))
 FROM repository_preparation_pin_batches b
 JOIN repository_preparation_pin_owners w USING(account_id,principal,operation_id,predecessor_generation,pins_sha256)
 JOIN repository_preparation_capture_drains d USING(account_id,principal,operation_id,predecessor_generation,pins_sha256)
 WHERE b.account_id=a AND b.principal=p AND b.operation_id=o AND b.predecessor_generation=g
$$;

CREATE FUNCTION guard_repository_preparation_root_release() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
DECLARE c repository_execution_claims%ROWTYPE; prep repository_publication_preparations%ROWTYPE;
 h repository_preparation_history_sets%ROWTYPE; b repository_preparation_pin_batches%ROWTYPE;
 w repository_preparation_pin_owners%ROWTYPE; d repository_preparation_capture_drains%ROWTYPE;
 actual_count bigint; actual_sha bytea; batches integer:=0; initials integer:=0; outcomes integer;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Preparation release is permanent'; END IF;
 PERFORM require_repository_read_committed();
 IF NEW.creation_xid<>pg_current_xact_id() THEN RAISE EXCEPTION 'Release requires its creation transaction'; END IF;
 SELECT * INTO c FROM repository_execution_claims
  WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id FOR UPDATE;
 IF NOT FOUND OR c.command_sha256<>NEW.command_sha256 THEN RAISE EXCEPTION 'Release claim differs'; END IF;
 SELECT * INTO prep FROM repository_publication_preparations
  WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
   AND predecessor_generation=NEW.predecessor_generation FOR UPDATE;
 IF NOT FOUND OR prep.preparation_sha256<>NEW.preparation_sha256 OR prep.command_sha256<>NEW.command_sha256 THEN
  RAISE EXCEPTION 'Release preparation differs';
 END IF;
 SELECT * INTO h FROM repository_preparation_history_sets
  WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
   AND predecessor_generation=NEW.predecessor_generation FOR UPDATE;
 IF NOT FOUND OR NOT h.sealed OR h.preparation_sha256<>NEW.preparation_sha256 OR h.command_sha256<>NEW.command_sha256
  OR h.expected_count<>NEW.root_count OR h.roots_sha256<>NEW.roots_sha256 THEN RAISE EXCEPTION 'Release root header differs'; END IF;
 SELECT count(*),sha256(convert_to('protomolt/preparation-history/v1' || E'\n' ||
  coalesce(string_agg(node_id::text || '/' || revision_id::text || E'\n','' ORDER BY node_id,revision_id),''),'UTF8'))
 INTO actual_count,actual_sha FROM repository_preparation_history_roots
 WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
  AND predecessor_generation=NEW.predecessor_generation;
 IF actual_count<>NEW.root_count OR actual_sha<>NEW.roots_sha256 THEN RAISE EXCEPTION 'Release live roots differ'; END IF;

 SELECT (SELECT count(*) FROM repository_operation_success WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id)
  +(SELECT count(*) FROM repository_operation_rejection WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id)
  +(SELECT count(*) FROM repository_publication_abandonments WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id)
 INTO outcomes;
 IF outcomes<>1 THEN RAISE EXCEPTION 'Release requires one exact terminal alternative'; END IF;
 IF NEW.terminal_kind='ABANDONMENT' THEN
  IF NEW.predecessor_generation<>0 OR c.claim_epoch<>1 OR c.claim_token<>NEW.abandonment_token
   OR prep.owner_nonce<>NEW.abandonment_nonce
   OR NOT EXISTS(SELECT 1 FROM repository_publication_abandonments x WHERE x.account_id=NEW.account_id
    AND x.principal=NEW.principal AND x.operation_id=NEW.operation_id AND x.predecessor_generation=0
    AND x.preparation_sha256=NEW.preparation_sha256 AND x.owner_nonce=NEW.abandonment_nonce
    AND x.claim_epoch=1 AND x.claim_token=NEW.abandonment_token)
   OR NOT EXISTS(SELECT 1 FROM repository_coordinator_bindings x WHERE x.account_id=NEW.account_id
    AND x.principal=NEW.principal AND x.operation_id=NEW.operation_id AND x.claim_epoch=1 AND x.claim_token=NEW.abandonment_token)
   OR EXISTS(SELECT 1 FROM repository_operations WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id)
   OR EXISTS(SELECT 1 FROM repository_operation_owners WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id)
   OR EXISTS(SELECT 1 FROM repository_publication_assessment_starts WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id) THEN
   RAISE EXCEPTION 'Release abandonment differs';
  END IF;
 ELSE
  IF NOT EXISTS(SELECT 1 FROM repository_operation_owners x JOIN repository_publication_preparations y
   ON y.account_id=x.account_id AND y.principal=x.principal AND y.operation_id=x.operation_id
    AND y.predecessor_generation=x.owner_generation-1 AND y.owner_nonce=x.owner_token
   JOIN repository_operations op ON op.account_id=x.account_id AND op.principal=x.principal AND op.operation_id=x.operation_id
   WHERE x.account_id=NEW.account_id AND x.principal=NEW.principal AND x.operation_id=NEW.operation_id
    AND x.owner_generation=NEW.terminal_generation AND y.command_sha256=NEW.command_sha256
    AND op.command_codec='document-publication' AND op.command_version=1 AND op.command_sha256=NEW.command_sha256) THEN
   RAISE EXCEPTION 'Release terminal owner differs';
  END IF;
  IF NEW.terminal_kind='SUCCESS' THEN
   IF NOT EXISTS(SELECT 1 FROM repository_operation_success x WHERE x.account_id=NEW.account_id AND x.principal=NEW.principal
    AND x.operation_id=NEW.operation_id AND x.owner_generation=NEW.terminal_generation AND x.result_sha256=NEW.terminal_sha256
    AND x.creation_xid=NEW.terminal_xid AND x.command_codec='document-publication' AND x.command_version=1
    AND x.command_sha256=NEW.command_sha256 AND sha256(x.result_bytes)=x.result_sha256) THEN RAISE EXCEPTION 'Release success differs'; END IF;
  ELSE
   IF NOT EXISTS(SELECT 1 FROM repository_operation_rejection x WHERE x.account_id=NEW.account_id AND x.principal=NEW.principal
    AND x.operation_id=NEW.operation_id AND x.owner_generation=NEW.terminal_generation AND x.result_sha256=NEW.terminal_sha256
    AND x.creation_xid=NEW.terminal_xid AND x.command_codec='document-publication' AND x.command_version=1
    AND x.command_sha256=NEW.command_sha256 AND sha256(x.result_bytes)=x.result_sha256
    AND (x.reason<>4 OR EXISTS(SELECT 1 FROM repository_recovery_limit_decisions z WHERE z.account_id=x.account_id
     AND z.principal=x.principal AND z.operation_id=x.operation_id AND z.owner_generation=x.owner_generation
     AND z.command_sha256=x.command_sha256 AND z.creation_xid=x.creation_xid))) THEN RAISE EXCEPTION 'Release rejection differs'; END IF;
  END IF;
 END IF;

 FOR b IN SELECT * FROM repository_preparation_pin_batches
  WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
   AND predecessor_generation=NEW.predecessor_generation ORDER BY pins_sha256 FOR UPDATE LOOP
  batches:=batches+1;
  IF batches>16 OR NOT b.sealed OR b.initial_capture IS DISTINCT FROM (b.creation_xid=h.creation_xid) THEN
   RAISE EXCEPTION 'Release capture set differs';
  END IF;
  IF b.initial_capture THEN initials:=initials+1; END IF;
  SELECT * INTO w FROM repository_preparation_pin_owners WHERE account_id=b.account_id AND principal=b.principal
   AND operation_id=b.operation_id AND predecessor_generation=b.predecessor_generation AND pins_sha256=b.pins_sha256;
  IF NOT FOUND THEN RAISE EXCEPTION 'Release capture ownership unknown'; END IF;
  IF NOT EXISTS(SELECT 1 FROM repository_coordinator_bindings x WHERE x.account_id=w.account_id AND x.principal=w.principal
   AND x.operation_id=w.operation_id AND x.claim_epoch=w.claim_epoch AND x.claim_token=w.claim_token AND x.incarnation=w.incarnation) THEN
   RAISE EXCEPTION 'Release capture binding differs';
  END IF;
  SELECT * INTO d FROM repository_preparation_capture_drains WHERE account_id=b.account_id AND principal=b.principal
   AND operation_id=b.operation_id AND predecessor_generation=b.predecessor_generation AND pins_sha256=b.pins_sha256;
  IF NOT FOUND OR d.claim_epoch<>w.claim_epoch OR d.claim_token<>w.claim_token OR d.incarnation<>w.incarnation
   OR d.command_sha256<>NEW.command_sha256 THEN RAISE EXCEPTION 'Release capture has not drained exactly'; END IF;
  SELECT count(*),sha256(convert_to('protomolt/preparation-pins/v1' || E'\n' ||
   coalesce(string_agg(reader_incarnation::text || '/' || pin_id::text || '/' || object_id::text || '/' ||
    node_id::text || '/' || revision_id::text || '/' || publication_revision::text || E'\n','' ORDER BY pin_id),''),'UTF8'))
  INTO actual_count,actual_sha FROM repository_preparation_source_pins WHERE account_id=b.account_id AND principal=b.principal
   AND operation_id=b.operation_id AND predecessor_generation=b.predecessor_generation AND pins_sha256=b.pins_sha256;
  IF actual_count<>b.expected_count OR actual_sha<>b.pins_sha256 THEN RAISE EXCEPTION 'Release capture pins differ'; END IF;
  IF EXISTS(SELECT 1 FROM repository_preparation_source_pins s WHERE s.account_id=b.account_id AND s.principal=b.principal
   AND s.operation_id=b.operation_id AND s.predecessor_generation=b.predecessor_generation AND s.pins_sha256=b.pins_sha256
   AND (EXISTS(SELECT 1 FROM document_read_pins x WHERE x.pin_id=s.pin_id)
    OR EXISTS(SELECT 1 FROM repository_object_references x WHERE x.owner_kind='DOCUMENT_READER' AND x.owner_id=s.pin_id))) THEN
   RAISE EXCEPTION 'Release capture still has live pins or mirrors';
  END IF;
 END LOOP;
 IF batches<>NEW.capture_count OR initials=0 OR NEW.captures_sha256<>
  repository_preparation_capture_fingerprint(NEW.account_id,NEW.principal,NEW.operation_id,NEW.predecessor_generation) THEN
  RAISE EXCEPTION 'Release capture fingerprint differs';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_preparation_root_release_guard BEFORE INSERT OR UPDATE OR DELETE
 ON repository_preparation_root_releases FOR EACH ROW EXECUTE FUNCTION guard_repository_preparation_root_release();

CREATE OR REPLACE FUNCTION guard_repository_preparation_history_root() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE owner repository_preparation_history_sets%ROWTYPE;
BEGIN
 IF TG_OP='UPDATE' THEN RAISE EXCEPTION 'Preparation history roots are immutable'; END IF;
 IF TG_OP='DELETE' THEN
  IF NOT EXISTS(SELECT 1 FROM repository_preparation_root_releases r JOIN repository_preparation_history_sets h
   USING(account_id,principal,operation_id,predecessor_generation)
   WHERE r.account_id=OLD.account_id AND r.principal=OLD.principal AND r.operation_id=OLD.operation_id
    AND r.predecessor_generation=OLD.predecessor_generation AND r.creation_xid=pg_current_xact_id()
    AND h.sealed AND h.preparation_sha256=r.preparation_sha256 AND h.command_sha256=r.command_sha256
    AND h.expected_count=r.root_count AND h.roots_sha256=r.roots_sha256) THEN
   RAISE EXCEPTION 'Preparation history roots require this transaction release';
  END IF;
  RETURN OLD;
 END IF;
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

CREATE FUNCTION require_complete_repository_root_release() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM repository_preparation_history_roots WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id AND predecessor_generation=NEW.predecessor_generation) THEN
  RAISE EXCEPTION 'Preparation release must remove every target root atomically';
 END IF;
 RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER repository_preparation_root_release_complete AFTER INSERT ON repository_preparation_root_releases
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_complete_repository_root_release();

CREATE OR REPLACE FUNCTION require_open_repository_capture() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id);
 IF EXISTS(SELECT 1 FROM repository_publication_abandonments WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id)
  OR EXISTS(SELECT 1 FROM repository_operation_success WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id)
  OR EXISTS(SELECT 1 FROM repository_operation_rejection WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id)
  OR EXISTS(SELECT 1 FROM repository_preparation_root_releases WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id) THEN
  RAISE EXCEPTION 'Publication capture admission is closed';
 END IF;
 RETURN NEW;
END;
$$;
