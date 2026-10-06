-- Private, monotonic cancellation before any owner can execute provider work.
CREATE TABLE repository_publication_abandonments (
 account_id varchar(200) NOT NULL,
 principal varchar(200) NOT NULL,
 operation_id uuid NOT NULL,
 predecessor_generation bigint NOT NULL CHECK(predecessor_generation=0),
 owner_nonce uuid NOT NULL,
 preparation_sha256 bytea NOT NULL CHECK(octet_length(preparation_sha256)=32),
 claim_epoch bigint NOT NULL CHECK(claim_epoch=1),
 claim_token uuid NOT NULL,
 PRIMARY KEY(account_id,principal,operation_id),
 FOREIGN KEY(account_id,principal,operation_id,predecessor_generation)
  REFERENCES repository_publication_preparations(account_id,principal,operation_id,predecessor_generation)
);
CREATE FUNCTION protect_repository_publication_abandonment() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE preparation repository_publication_preparations%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Publication abandonment is immutable'; END IF;
 PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id);
 IF NOT EXISTS(SELECT 1 FROM repository_execution_claims WHERE account_id=NEW.account_id AND principal=NEW.principal
   AND operation_id=NEW.operation_id AND claim_epoch=NEW.claim_epoch AND claim_token=NEW.claim_token) THEN
  RAISE EXCEPTION 'Abandonment requires original execution claim';
 END IF;
 SELECT * INTO preparation FROM repository_publication_preparations WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id AND predecessor_generation=0;
 IF NOT FOUND OR preparation.owner_nonce<>NEW.owner_nonce OR preparation.preparation_sha256<>NEW.preparation_sha256 THEN
  RAISE EXCEPTION 'Abandonment differs from initial preparation';
 END IF;
 IF EXISTS(SELECT 1 FROM repository_operations WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id)
  OR EXISTS(SELECT 1 FROM repository_operation_owners WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id)
  OR EXISTS(SELECT 1 FROM repository_publication_assessment_starts WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id) THEN
  RAISE EXCEPTION 'Admitted publication cannot be abandoned';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_publication_abandonment_guard BEFORE INSERT OR UPDATE OR DELETE ON repository_publication_abandonments
 FOR EACH ROW EXECUTE FUNCTION protect_repository_publication_abandonment();

CREATE FUNCTION refuse_abandoned_repository_publication() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 -- Existing claim guards establish the same transaction fence before this check.
 IF EXISTS(SELECT 1 FROM repository_publication_abandonments WHERE account_id=NEW.account_id
   AND principal=NEW.principal AND operation_id=NEW.operation_id) THEN
  RAISE EXCEPTION 'Publication registration was abandoned';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER zz_repository_abandonment_guard BEFORE INSERT ON repository_publication_preparations
 FOR EACH ROW EXECUTE FUNCTION refuse_abandoned_repository_publication();
CREATE TRIGGER zz_repository_abandonment_guard BEFORE INSERT ON repository_publication_modes
 FOR EACH ROW EXECUTE FUNCTION refuse_abandoned_repository_publication();
CREATE TRIGGER zz_repository_abandonment_guard BEFORE INSERT ON repository_operations
 FOR EACH ROW EXECUTE FUNCTION refuse_abandoned_repository_publication();
CREATE TRIGGER zz_repository_abandonment_guard BEFORE INSERT OR UPDATE ON repository_operation_owners
 FOR EACH ROW EXECUTE FUNCTION refuse_abandoned_repository_publication();
