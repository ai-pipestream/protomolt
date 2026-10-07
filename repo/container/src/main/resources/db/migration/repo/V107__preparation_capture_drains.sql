-- Private local-lifecycle attestation or consumption of permanent reader quiescence.
-- Neither mode permits preparation-root release by itself.
CREATE TABLE repository_preparation_capture_drains (
 account_id varchar(200) NOT NULL,
 principal varchar(200) NOT NULL,
 operation_id uuid NOT NULL,
 predecessor_generation bigint NOT NULL,
 pins_sha256 bytea NOT NULL,
 claim_epoch bigint NOT NULL CHECK(claim_epoch>0),
 claim_token uuid NOT NULL,
 incarnation uuid NOT NULL,
 command_sha256 bytea NOT NULL CHECK(octet_length(command_sha256)=32),
 drain_kind text NOT NULL CHECK(drain_kind IN ('LOCAL','QUIESCED')),
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(account_id,principal,operation_id,predecessor_generation,pins_sha256),
 FOREIGN KEY(account_id,principal,operation_id,predecessor_generation,pins_sha256)
  REFERENCES repository_preparation_pin_owners
);

CREATE FUNCTION guard_repository_preparation_capture_drain() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
DECLARE current_command bytea; source_set repository_preparation_history_sets%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Preparation capture drain is immutable'; END IF;
 PERFORM require_repository_read_committed();
 -- Serialize against execution/retention transitions, but never renew or stamp
 -- a claim. The original capture can finish after expiry or a successor transfer.
 SELECT command_sha256 INTO current_command FROM repository_execution_claims
  WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id FOR UPDATE;
 IF NOT FOUND OR current_command IS DISTINCT FROM NEW.command_sha256 THEN
  RAISE EXCEPTION 'Capture drain requires its original command scope';
 END IF;
 SELECT * INTO source_set FROM repository_preparation_history_sets
  WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
   AND predecessor_generation=NEW.predecessor_generation FOR UPDATE;
 IF NOT FOUND OR NOT source_set.sealed OR source_set.expected_count=0
  OR source_set.command_sha256 IS DISTINCT FROM NEW.command_sha256 THEN
  RAISE EXCEPTION 'Capture drain requires its sealed historical preparation';
 END IF;
 PERFORM 1 FROM repository_preparation_pin_batches b JOIN repository_preparation_pin_owners o
  USING(account_id,principal,operation_id,predecessor_generation,pins_sha256)
  WHERE b.account_id=NEW.account_id AND b.principal=NEW.principal AND b.operation_id=NEW.operation_id
   AND b.predecessor_generation=NEW.predecessor_generation AND b.pins_sha256=NEW.pins_sha256
   AND b.sealed AND o.claim_epoch=NEW.claim_epoch AND o.claim_token=NEW.claim_token AND o.incarnation=NEW.incarnation
  FOR UPDATE OF b;
 IF NOT FOUND THEN RAISE EXCEPTION 'Capture drain requires its exact immutable owner'; END IF;
 IF EXISTS(SELECT 1 FROM repository_preparation_source_pins s
  WHERE s.account_id=NEW.account_id AND s.principal=NEW.principal AND s.operation_id=NEW.operation_id
   AND s.predecessor_generation=NEW.predecessor_generation AND s.pins_sha256=NEW.pins_sha256
   AND (EXISTS(SELECT 1 FROM document_read_pins p WHERE p.pin_id=s.pin_id)
    OR EXISTS(SELECT 1 FROM repository_object_references r WHERE r.owner_kind='DOCUMENT_READER' AND r.owner_id=s.pin_id))) THEN
  RAISE EXCEPTION 'Capture drain requires absent native pins and mirrors';
 END IF;
 -- LOCAL is a trusted host attestation, minted only through the owning capture
 -- lifecycle in Java. SQL cannot observe a JVM worker. Recovery has durable proof.
 IF NEW.drain_kind='QUIESCED' AND EXISTS(SELECT 1 FROM repository_preparation_source_pins s
  JOIN repository_reader_incarnations r ON r.incarnation=s.reader_incarnation
  WHERE s.account_id=NEW.account_id AND s.principal=NEW.principal AND s.operation_id=NEW.operation_id
   AND s.predecessor_generation=NEW.predecessor_generation AND s.pins_sha256=NEW.pins_sha256 AND r.state<>'QUIESCED') THEN
  RAISE EXCEPTION 'Capture recovery requires every original reader quiesced';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_preparation_capture_drain_guard BEFORE INSERT OR UPDATE OR DELETE
 ON repository_preparation_capture_drains FOR EACH ROW EXECUTE FUNCTION guard_repository_preparation_capture_drain();
