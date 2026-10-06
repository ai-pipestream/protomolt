-- Per-claim identity, not an attestation of local or provider quiescence.
CREATE TABLE repository_coordinator_bindings (
 account_id varchar(200) NOT NULL,
 principal varchar(200) NOT NULL,
 operation_id uuid NOT NULL,
 claim_epoch bigint NOT NULL CHECK(claim_epoch=1),
 claim_token uuid NOT NULL,
 incarnation uuid NOT NULL,
 PRIMARY KEY(account_id,principal,operation_id,claim_epoch),
 FOREIGN KEY(account_id,principal,operation_id)
  REFERENCES repository_execution_claims(account_id,principal,operation_id)
);
CREATE INDEX repository_coordinator_incarnation ON repository_coordinator_bindings(incarnation);

CREATE FUNCTION protect_repository_coordinator_binding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Coordinator binding is immutable'; END IF;
 PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id);
 IF NOT EXISTS(SELECT 1 FROM repository_execution_claims WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id
  AND claim_epoch=NEW.claim_epoch AND claim_token=NEW.claim_token) THEN
  RAISE EXCEPTION 'Coordinator binding differs from current claim';
 END IF;
 IF EXISTS(SELECT 1 FROM repository_publication_preparations WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id)
  OR EXISTS(SELECT 1 FROM repository_operations WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id) THEN
  RAISE EXCEPTION 'Coordinator binding must precede preparation and operation admission';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_coordinator_binding_guard BEFORE INSERT OR UPDATE OR DELETE
 ON repository_coordinator_bindings FOR EACH ROW EXECUTE FUNCTION protect_repository_coordinator_binding();
