-- Terminal evidence only: this does not open any execution or provider fence.
CREATE TABLE repository_recovery_limit_decisions (
 account_id varchar(200) NOT NULL, principal varchar(200) NOT NULL, operation_id uuid NOT NULL,
 claim_epoch bigint NOT NULL, claim_token uuid NOT NULL, incarnation uuid NOT NULL,
 owner_generation bigint NOT NULL, owner_nonce uuid NOT NULL,
 command_sha256 bytea NOT NULL CHECK(octet_length(command_sha256)=32),
 preparation_sha256 bytea NOT NULL CHECK(octet_length(preparation_sha256)=32),
 modes_sha256 bytea NOT NULL CHECK(octet_length(modes_sha256)=32),
 retention_generation bigint NOT NULL CHECK(retention_generation>=0),
 retention_sha256 bytea NOT NULL CHECK(octet_length(retention_sha256)=32),
 limit_kind text NOT NULL CHECK(limit_kind IN ('CAPTURES','ANCESTRY')),
 observed_count integer NOT NULL,
 inspected_depth integer NOT NULL CHECK(inspected_depth BETWEEN 1 AND 65),
 endpoint_generation bigint NOT NULL CHECK(endpoint_generation>=0),
 endpoint_sha256 bytea NOT NULL CHECK(octet_length(endpoint_sha256)=32),
 creation_xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
 PRIMARY KEY(account_id,principal,operation_id),
 FOREIGN KEY(account_id,principal,operation_id,claim_epoch)
  REFERENCES repository_successor_installs(account_id,principal,operation_id,successor_epoch),
 FOREIGN KEY(account_id,principal,operation_id,retention_generation)
  REFERENCES repository_preparation_history_sets(account_id,principal,operation_id,predecessor_generation)
);

CREATE FUNCTION require_live_repository_limit_identity(d repository_recovery_limit_decisions)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE c repository_execution_claims%ROWTYPE; w repository_operation_owners%ROWTYPE;
BEGIN
 PERFORM require_repository_read_committed();
 SELECT * INTO STRICT c FROM repository_execution_claims WHERE account_id=d.account_id
  AND principal=d.principal AND operation_id=d.operation_id FOR UPDATE;
 SELECT * INTO STRICT w FROM repository_operation_owners WHERE account_id=d.account_id
  AND principal=d.principal AND operation_id=d.operation_id FOR UPDATE;
 IF (c.claim_epoch,c.claim_token,c.command_sha256) IS DISTINCT FROM (d.claim_epoch,d.claim_token,d.command_sha256)
  OR (w.owner_generation,w.owner_token) IS DISTINCT FROM (d.owner_generation,d.owner_nonce)
  OR c.lease_until<=clock_timestamp() OR w.lease_until<=clock_timestamp() THEN
  RAISE EXCEPTION 'Recovery limit decision requires exact live installed identity';
 END IF;
 IF EXISTS(SELECT 1 FROM repository_successor_executions WHERE account_id=d.account_id
  AND principal=d.principal AND operation_id=d.operation_id AND claim_epoch=d.claim_epoch) THEN
  RAISE EXCEPTION 'Recovery limit decision cannot accompany activation';
 END IF;
 RETURN true;
END; $$;

CREATE FUNCTION guard_repository_recovery_limit_decision() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE c repository_execution_claims%ROWTYPE; w repository_operation_owners%ROWTYPE;
 h repository_preparation_history_sets%ROWTYPE; edge repository_successor_installs%ROWTYPE;
 g bigint; sha bytea; depth integer:=0; batches integer; owned integer;
 actual_count bigint; actual_sha bytea; batch repository_preparation_pin_batches%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Recovery limit decision is immutable'; END IF;
 PERFORM require_repository_read_committed();
 SELECT * INTO STRICT c FROM repository_execution_claims WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id FOR UPDATE;
 SELECT * INTO STRICT w FROM repository_operation_owners WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id FOR UPDATE;
 IF (c.claim_epoch,c.claim_token,c.command_sha256) IS DISTINCT FROM
    (NEW.claim_epoch,NEW.claim_token,NEW.command_sha256)
  OR (w.owner_generation,w.owner_token) IS DISTINCT FROM (NEW.owner_generation,NEW.owner_nonce)
  OR c.lease_until<=clock_timestamp() OR w.lease_until<=clock_timestamp()
  OR NEW.creation_xid<>pg_current_xact_id() THEN
  RAISE EXCEPTION 'Recovery limit decision requires exact live installed identity';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM repository_successor_installs i
  JOIN repository_coordinator_reservations r ON (r.account_id,r.principal,r.operation_id,r.successor_epoch)=
   (i.account_id,i.principal,i.operation_id,i.successor_epoch)
  WHERE i.account_id=NEW.account_id AND i.principal=NEW.principal AND i.operation_id=NEW.operation_id
   AND i.successor_epoch=NEW.claim_epoch AND i.successor_token=NEW.claim_token
   AND i.successor_incarnation=NEW.incarnation AND i.owner_nonce=NEW.owner_nonce
   AND i.predecessor_generation=NEW.owner_generation-1 AND i.command_sha256=NEW.command_sha256
   AND i.preparation_sha256=NEW.preparation_sha256 AND i.modes_sha256=NEW.modes_sha256
   AND i.install_xid<>pg_current_xact_id()
   AND r.successor_token=i.successor_token AND r.successor_incarnation=i.successor_incarnation
   AND r.command_sha256=i.command_sha256) THEN
  RAISE EXCEPTION 'Recovery limit decision differs from committed installation';
 END IF;
 IF EXISTS(SELECT 1 FROM repository_successor_executions WHERE account_id=NEW.account_id
   AND principal=NEW.principal AND operation_id=NEW.operation_id AND claim_epoch=NEW.claim_epoch)
  OR EXISTS(SELECT 1 FROM repository_operation_success WHERE account_id=NEW.account_id
   AND principal=NEW.principal AND operation_id=NEW.operation_id)
  OR EXISTS(SELECT 1 FROM repository_operation_rejection WHERE account_id=NEW.account_id
   AND principal=NEW.principal AND operation_id=NEW.operation_id) THEN
  RAISE EXCEPTION 'Activated or terminal operation cannot decide a recovery limit';
 END IF;
 PERFORM 1 FROM repository_publication_preparations WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id
  AND predecessor_generation=NEW.retention_generation AND preparation_sha256=NEW.retention_sha256
  AND command_sha256=NEW.command_sha256 FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Recovery limit retained preparation differs'; END IF;
 SELECT * INTO STRICT h FROM repository_preparation_history_sets WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id
  AND predecessor_generation=NEW.retention_generation FOR UPDATE;
 IF NOT h.sealed OR h.expected_count=0 OR h.preparation_sha256<>NEW.retention_sha256
  OR h.command_sha256<>NEW.command_sha256 OR NEW.retention_generation>=NEW.owner_generation-1 THEN
  RAISE EXCEPTION 'Recovery limit requires exact historical retention';
 END IF;
 SELECT count(*),sha256(convert_to('protomolt/preparation-history/v1' || E'\n' ||
  coalesce(string_agg(node_id::text || '/' || revision_id::text || E'\n','' ORDER BY node_id,revision_id),''),'UTF8'))
 INTO actual_count,actual_sha FROM repository_preparation_history_roots
 WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
  AND predecessor_generation=NEW.retention_generation;
 IF actual_count<>h.expected_count OR actual_sha<>h.roots_sha256 THEN
  RAISE EXCEPTION 'Recovery limit retained roots differ';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM repository_preparation_pin_batches b
  JOIN repository_preparation_pin_owners o USING(account_id,principal,operation_id,predecessor_generation,pins_sha256)
  JOIN repository_coordinator_bindings i USING(account_id,principal,operation_id,claim_epoch,claim_token,incarnation)
  WHERE b.account_id=NEW.account_id AND b.principal=NEW.principal AND b.operation_id=NEW.operation_id
   AND b.predecessor_generation=NEW.retention_generation AND b.initial_capture AND b.sealed
   AND b.creation_xid=h.creation_xid) THEN
  RAISE EXCEPTION 'Recovery limit requires original capture ownership';
 END IF;
 g:=NEW.owner_generation-1; sha:=NEW.preparation_sha256;
 WHILE g>NEW.retention_generation AND depth<65 LOOP
  SELECT * INTO STRICT edge FROM repository_successor_installs WHERE account_id=NEW.account_id
   AND principal=NEW.principal AND operation_id=NEW.operation_id
   AND predecessor_generation=g AND preparation_sha256=sha;
  IF edge.command_sha256<>NEW.command_sha256 THEN RAISE EXCEPTION 'Recovery limit ancestry command differs'; END IF;
  depth:=depth+1; g:=g-1; sha:=edge.predecessor_preparation_sha256;
 END LOOP;
 IF g=NEW.retention_generation AND sha<>NEW.retention_sha256 THEN
  RAISE EXCEPTION 'Recovery limit ancestry anchor differs';
 END IF;
 IF (NEW.inspected_depth,NEW.endpoint_generation,NEW.endpoint_sha256) IS DISTINCT FROM (depth,g,sha) THEN
  RAISE EXCEPTION 'Recovery limit ancestry evidence differs';
 END IF;
 IF NEW.limit_kind='ANCESTRY' THEN
  IF depth<>65 OR NEW.observed_count<>65 THEN RAISE EXCEPTION 'Recovery ancestry limit is not exhausted'; END IF;
 ELSE
  IF g<>NEW.retention_generation OR sha<>NEW.retention_sha256 THEN
   RAISE EXCEPTION 'Recovery capture limit requires complete retained ancestry';
  END IF;
  PERFORM 1 FROM repository_preparation_pin_batches WHERE account_id=NEW.account_id
   AND principal=NEW.principal AND operation_id=NEW.operation_id
   AND predecessor_generation=NEW.retention_generation ORDER BY pins_sha256 FOR UPDATE;
  SELECT count(*),count(o.pins_sha256) INTO batches,owned FROM repository_preparation_pin_batches b
   LEFT JOIN repository_preparation_pin_owners o USING(account_id,principal,operation_id,predecessor_generation,pins_sha256)
   WHERE b.account_id=NEW.account_id AND b.principal=NEW.principal AND b.operation_id=NEW.operation_id
    AND b.predecessor_generation=NEW.retention_generation AND b.sealed;
  IF batches<>16 OR owned<>16 OR NEW.observed_count<>16 THEN
   RAISE EXCEPTION 'Recovery capture limit is not exhausted';
  END IF;
  IF NOT EXISTS(SELECT 1 FROM repository_preparation_pin_batches WHERE account_id=NEW.account_id
   AND principal=NEW.principal AND operation_id=NEW.operation_id AND predecessor_generation=NEW.retention_generation
   AND initial_capture AND sealed AND creation_xid=h.creation_xid) THEN
   RAISE EXCEPTION 'Recovery capture limit requires original capture evidence';
  END IF;
  FOR batch IN SELECT * FROM repository_preparation_pin_batches WHERE account_id=NEW.account_id
   AND principal=NEW.principal AND operation_id=NEW.operation_id AND predecessor_generation=NEW.retention_generation LOOP
   IF NOT batch.sealed OR batch.initial_capture IS DISTINCT FROM (batch.creation_xid=h.creation_xid)
    OR NOT EXISTS(SELECT 1 FROM repository_preparation_pin_owners o JOIN repository_coordinator_bindings b
     USING(account_id,principal,operation_id,claim_epoch,claim_token,incarnation)
     WHERE o.account_id=NEW.account_id AND o.principal=NEW.principal AND o.operation_id=NEW.operation_id
      AND o.predecessor_generation=NEW.retention_generation AND o.pins_sha256=batch.pins_sha256) THEN
    RAISE EXCEPTION 'Recovery capture ownership differs';
   END IF;
   SELECT count(*),sha256(convert_to('protomolt/preparation-pins/v1' || E'\n' ||
    coalesce(string_agg(reader_incarnation::text || '/' || pin_id::text || '/' || object_id::text || '/' ||
     node_id::text || '/' || revision_id::text || '/' || publication_revision::text || E'\n','' ORDER BY pin_id),''),'UTF8'))
   INTO actual_count,actual_sha FROM repository_preparation_source_pins
   WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
    AND predecessor_generation=NEW.retention_generation AND pins_sha256=batch.pins_sha256;
   IF actual_count<>batch.expected_count OR actual_sha<>batch.pins_sha256 THEN
    RAISE EXCEPTION 'Recovery capture contents differ';
   END IF;
  END LOOP;
 END IF;
 RETURN NEW;
END; $$;
CREATE TRIGGER repository_recovery_limit_decision_guard BEFORE INSERT OR UPDATE OR DELETE
 ON repository_recovery_limit_decisions FOR EACH ROW EXECUTE FUNCTION guard_repository_recovery_limit_decision();

ALTER TABLE repository_operation_rejection DROP CONSTRAINT repository_operation_rejection_reason_check;
ALTER TABLE repository_operation_rejection ADD CONSTRAINT repository_operation_rejection_reason_check CHECK(reason IN (1,2,3,4));

CREATE OR REPLACE FUNCTION guard_repository_operation_rejection() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE command repository_operations%ROWTYPE; decision repository_recovery_limit_decisions%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Repository operation rejection is immutable'; END IF;
 IF NEW.reason=4 THEN
  IF NOT EXISTS(SELECT 1 FROM repository_recovery_limit_decisions d
   WHERE d.account_id=NEW.account_id AND d.principal=NEW.principal AND d.operation_id=NEW.operation_id
    AND d.owner_generation=NEW.owner_generation AND d.command_sha256=NEW.command_sha256
    AND d.creation_xid=pg_current_xact_id()) THEN
   RAISE EXCEPTION 'Recovery rejection requires its atomic limit decision';
  END IF;
  SELECT * INTO STRICT decision FROM repository_recovery_limit_decisions WHERE account_id=NEW.account_id
   AND principal=NEW.principal AND operation_id=NEW.operation_id;
  PERFORM require_live_repository_limit_identity(decision);
 ELSE
  PERFORM require_repository_operation_write_fence(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation);
 END IF;
 SELECT * INTO STRICT command FROM repository_operations WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id;
 IF ROW(NEW.command_codec,NEW.command_version,NEW.command_sha256)
  IS DISTINCT FROM ROW(command.command_codec,command.command_version,command.command_sha256)
  OR command.command_codec<>'document-publication' OR command.command_version<>1 THEN
  RAISE EXCEPTION 'Repository rejection differs from admitted command identity';
 END IF;
 IF EXISTS(SELECT 1 FROM document_revision_commits WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id) THEN
  RAISE EXCEPTION 'Repository rejection cannot accompany publication';
 END IF;
 IF NEW.recorded_at_epoch_micros < floor(extract(epoch FROM transaction_timestamp())*1000000)
  OR NEW.recorded_at_epoch_micros > floor(extract(epoch FROM clock_timestamp())*1000000) THEN
  RAISE EXCEPTION 'Repository rejection time must belong to its decision transaction';
 END IF;
 NEW.creation_xid:=pg_current_xact_id(); RETURN NEW;
END; $$;

CREATE FUNCTION require_repository_recovery_limit_pair() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM require_live_repository_limit_identity(NEW);
 IF NOT EXISTS(SELECT 1 FROM repository_recovery_limit_decisions d JOIN repository_operation_rejection r
  USING(account_id,principal,operation_id) WHERE d.account_id=NEW.account_id
   AND d.principal=NEW.principal AND d.operation_id=NEW.operation_id AND r.reason=4 AND r.disposition=1
   AND d.creation_xid=r.creation_xid AND d.creation_xid=pg_current_xact_id()
   AND d.owner_generation=r.owner_generation AND d.command_sha256=r.command_sha256) THEN
  RAISE EXCEPTION 'Recovery limit decision requires atomic rejection';
 END IF;
 RETURN NULL;
END; $$;
CREATE CONSTRAINT TRIGGER repository_recovery_limit_complete AFTER INSERT ON repository_recovery_limit_decisions
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_repository_recovery_limit_pair();
