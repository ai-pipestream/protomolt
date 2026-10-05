-- Sticky intent to create, never evidence that assessment CREATE committed.
CREATE TABLE repository_publication_assessment_starts (
 account_id varchar(200) NOT NULL,
 principal varchar(200) NOT NULL,
 operation_id uuid NOT NULL,
 predecessor_generation bigint NOT NULL,
 owner_nonce uuid NOT NULL,
 command_sha256 bytea NOT NULL CHECK(octet_length(command_sha256)=32),
 assessment_id uuid NOT NULL UNIQUE,
 retention_micros bigint NOT NULL CHECK(retention_micros BETWEEN 1 AND 86400000000),
 retain_until timestamptz NOT NULL,
 started_xid xid8 NOT NULL,
 PRIMARY KEY(account_id,principal,operation_id,predecessor_generation),
 FOREIGN KEY(account_id,principal,operation_id,predecessor_generation)
  REFERENCES repository_publication_modes(account_id,principal,operation_id,predecessor_generation)
);
CREATE FUNCTION protect_repository_publication_assessment_start() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE preparation repository_publication_preparations%ROWTYPE;
 existing repository_publication_assessment_starts%ROWTYPE; owner repository_operation_owners%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Assessment start is immutable'; END IF;
 PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id);
 PERFORM 1 FROM repository_publication_modes WHERE account_id=NEW.account_id AND principal=NEW.principal
  AND operation_id=NEW.operation_id AND predecessor_generation=NEW.predecessor_generation FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Assessment start requires fixed modes'; END IF;
 SELECT * INTO STRICT preparation FROM repository_publication_preparations WHERE account_id=NEW.account_id AND principal=NEW.principal
  AND operation_id=NEW.operation_id AND predecessor_generation=NEW.predecessor_generation;
 IF preparation.owner_nonce<>NEW.owner_nonce OR preparation.command_sha256<>NEW.command_sha256 THEN
  RAISE EXCEPTION 'Assessment start differs from preparation';
 END IF;
 PERFORM require_repository_operation_write_fence(NEW.account_id,NEW.principal,NEW.operation_id,NEW.predecessor_generation+1);
 SELECT * INTO STRICT owner FROM repository_operation_owners WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id;
 IF owner.owner_token<>NEW.owner_nonce THEN RAISE EXCEPTION 'Assessment start differs from owner'; END IF;
 SELECT * INTO existing FROM repository_publication_assessment_starts WHERE account_id=NEW.account_id AND principal=NEW.principal
  AND operation_id=NEW.operation_id AND predecessor_generation=NEW.predecessor_generation;
 IF FOUND THEN
  IF ROW(existing.owner_nonce,existing.command_sha256,existing.assessment_id,existing.retention_micros)
   IS DISTINCT FROM ROW(NEW.owner_nonce,NEW.command_sha256,NEW.assessment_id,NEW.retention_micros) THEN
   RAISE EXCEPTION 'Assessment start changed';
  END IF;
  NEW.retain_until:=existing.retain_until;
  NEW.started_xid:=existing.started_xid;
 ELSE
  IF EXISTS(SELECT 1 FROM document_assessment_owners WHERE account_id=NEW.account_id AND principal=NEW.principal
    AND operation_id=NEW.operation_id AND owner_generation=NEW.predecessor_generation+1) THEN
   RAISE EXCEPTION 'Assessment start must precede assessment creation';
  END IF;
  NEW.retain_until:=clock_timestamp() + NEW.retention_micros * interval '1 microsecond';
  NEW.started_xid:=pg_current_xact_id();
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_publication_assessment_start_guard BEFORE INSERT OR UPDATE OR DELETE ON repository_publication_assessment_starts
 FOR EACH ROW EXECUTE FUNCTION protect_repository_publication_assessment_start();

-- Existing unjournaled operations remain explicit legacy paths. Once fixed modes
-- exist, absence of a start record is uncertainty, never permission to CREATE.
CREATE FUNCTION require_repository_assessment_start(a text,p text,o uuid,g bigint,id uuid,digest bytea,deadline timestamptz)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE start repository_publication_assessment_starts%ROWTYPE; nonce uuid;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM repository_publication_modes WHERE account_id=a AND principal=p AND operation_id=o
   AND predecessor_generation=g-1) THEN RETURN; END IF;
 PERFORM require_repository_execution_claim(a,p,o);
 SELECT * INTO start FROM repository_publication_assessment_starts WHERE account_id=a AND principal=p AND operation_id=o
  AND predecessor_generation=g-1 FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Assessment requires durable start'; END IF;
 IF start.started_xid=pg_current_xact_id_if_assigned() THEN
  RAISE EXCEPTION 'Assessment start must be committed before creation';
 END IF;
 SELECT owner_token INTO STRICT nonce FROM repository_operation_owners WHERE account_id=a AND principal=p AND operation_id=o;
 IF ROW(start.owner_nonce,start.command_sha256,start.assessment_id,start.retain_until)
  IS DISTINCT FROM ROW(nonce,digest,id,deadline) THEN RAISE EXCEPTION 'Assessment differs from durable start'; END IF;
END;
$$;
CREATE FUNCTION guard_document_assessment_start_binding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM require_repository_assessment_start(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation,
  NEW.assessment_id,NEW.command_sha256,NEW.retain_until);
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_assessment_start_binding BEFORE INSERT ON document_assessment_owners
 FOR EACH ROW EXECUTE FUNCTION guard_document_assessment_start_binding();
