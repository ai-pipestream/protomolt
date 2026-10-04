-- Preserve the initial placement anchor and every explicit attempt choice.
-- Hold admission inserts until both the backfill and initializer are installed.
LOCK TABLE document_operation_selections IN SHARE ROW EXCLUSIVE MODE;
CREATE TABLE document_operation_selection_attempts (
 account_id VARCHAR(200) NOT NULL,
 principal VARCHAR(200) NOT NULL,
 operation_id UUID NOT NULL,
 owner_generation BIGINT NOT NULL,
 member_id VARCHAR(128) NOT NULL,
 selection_revision BIGINT NOT NULL CHECK(selection_revision>0),
 attempt_id UUID UNIQUE REFERENCES document_part_attempts(attempt_id),
 previous_attempt_id UUID REFERENCES document_part_attempts(attempt_id),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(account_id,principal,operation_id,owner_generation,member_id,selection_revision),
 FOREIGN KEY(account_id,principal,operation_id,owner_generation,member_id)
  REFERENCES document_operation_selections(account_id,principal,operation_id,owner_generation,member_id),
 CHECK(selection_revision<>1 OR previous_attempt_id IS NULL),
 CHECK(selection_revision=1 OR (attempt_id IS NOT NULL AND previous_attempt_id IS NOT NULL AND attempt_id<>previous_attempt_id))
);
CREATE TABLE document_operation_selection_current (
 account_id VARCHAR(200) NOT NULL,
 principal VARCHAR(200) NOT NULL,
 operation_id UUID NOT NULL,
 owner_generation BIGINT NOT NULL,
 member_id VARCHAR(128) NOT NULL,
 selection_revision BIGINT NOT NULL,
 PRIMARY KEY(account_id,principal,operation_id,owner_generation,member_id),
 FOREIGN KEY(account_id,principal,operation_id,owner_generation,member_id,selection_revision)
  REFERENCES document_operation_selection_attempts(account_id,principal,operation_id,owner_generation,member_id,selection_revision)
);
INSERT INTO document_operation_selection_attempts(account_id,principal,operation_id,owner_generation,member_id,selection_revision,attempt_id,created_at)
 SELECT account_id,principal,operation_id,owner_generation,member_id,1,attempt_id,created_at FROM document_operation_selections;
INSERT INTO document_operation_selection_current
 SELECT account_id,principal,operation_id,owner_generation,member_id,1 FROM document_operation_selections;

CREATE FUNCTION guard_document_operation_selection_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE s document_operation_selections%ROWTYPE; a document_part_attempts%ROWTYPE;
 current_revision BIGINT; current_attempt UUID; d drives%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Document selection history is immutable'; END IF;
 PERFORM require_repository_operation_write_fence(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation);
 SELECT * INTO STRICT s FROM document_operation_selections
 WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
  AND owner_generation=NEW.owner_generation AND member_id=NEW.member_id;
 IF NEW.selection_revision=1 THEN
  IF NEW.attempt_id IS DISTINCT FROM s.attempt_id THEN RAISE EXCEPTION 'Initial attempt must match selection anchor'; END IF;
  RETURN NEW;
 END IF;
 SELECT * INTO STRICT d FROM drives WHERE drive_id=s.drive_id FOR SHARE;
 IF d.status<>'ACTIVE' OR s.drive_sha256 IS DISTINCT FROM document_operation_drive_digest_v1(d) THEN
  RAISE EXCEPTION 'Retry placement differs from selection anchor';
 END IF;
 SELECT * INTO STRICT a FROM document_part_attempts WHERE attempt_id=NEW.attempt_id FOR SHARE;
 IF a.plan_kind<>'NEW_CONTENT' OR a.state<>'STAGING' OR a.lease_until<=clock_timestamp()
  OR ROW(a.account_id,a.operation_principal,a.operation_id,a.operation_generation,a.member_id,a.node_id,
         a.sampled_revision,a.drive_id,a.backend_generation,a.storage_realm,a.storage_namespace,a.planned_count)
   IS DISTINCT FROM ROW(s.account_id,s.principal,s.operation_id,s.owner_generation,s.member_id,s.node_id,
         s.sampled_revision,s.drive_id,s.backend_generation,s.storage_realm,s.storage_namespace,s.upload_count)
  OR EXISTS(SELECT 1 FROM document_part_attempt_cleanup WHERE attempt_id=NEW.attempt_id) THEN
  RAISE EXCEPTION 'Retry requires its exact live new-content attempt';
 END IF;
 SELECT c.selection_revision,h.attempt_id INTO current_revision,current_attempt
 FROM document_operation_selection_current c JOIN document_operation_selection_attempts h
 USING(account_id,principal,operation_id,owner_generation,member_id,selection_revision)
 WHERE c.account_id=NEW.account_id AND c.principal=NEW.principal AND c.operation_id=NEW.operation_id
  AND c.owner_generation=NEW.owner_generation AND c.member_id=NEW.member_id FOR UPDATE OF c;
 IF current_revision IS NULL OR current_revision=9223372036854775807 OR NEW.selection_revision<>current_revision+1
  OR NEW.previous_attempt_id IS DISTINCT FROM current_attempt OR s.upload_count=0 THEN
  RAISE EXCEPTION 'Document selection compare-and-set conflict';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_operation_selection_attempt_guard BEFORE INSERT OR UPDATE OR DELETE ON document_operation_selection_attempts
 FOR EACH ROW EXECUTE FUNCTION guard_document_operation_selection_attempt();

CREATE FUNCTION guard_document_operation_selection_current() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE previous_attempt UUID; next_previous UUID;
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Document selection pointer cannot be deleted'; END IF;
 PERFORM require_repository_operation_write_fence(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation);
 IF TG_OP='INSERT' THEN
  IF NEW.selection_revision<>1 THEN RAISE EXCEPTION 'Document selection must start at revision one'; END IF;
 ELSE
  IF ROW(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation,NEW.member_id)
   IS DISTINCT FROM ROW(OLD.account_id,OLD.principal,OLD.operation_id,OLD.owner_generation,OLD.member_id)
   OR NEW.selection_revision<>OLD.selection_revision+1 THEN
   RAISE EXCEPTION 'Document selection pointer requires the next revision';
  END IF;
  SELECT attempt_id INTO STRICT previous_attempt FROM document_operation_selection_attempts
  WHERE account_id=OLD.account_id AND principal=OLD.principal AND operation_id=OLD.operation_id
   AND owner_generation=OLD.owner_generation AND member_id=OLD.member_id AND selection_revision=OLD.selection_revision;
  SELECT previous_attempt_id INTO STRICT next_previous FROM document_operation_selection_attempts
  WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
   AND owner_generation=NEW.owner_generation AND member_id=NEW.member_id AND selection_revision=NEW.selection_revision;
  IF previous_attempt IS DISTINCT FROM next_previous THEN RAISE EXCEPTION 'Document selection predecessor differs'; END IF;
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_operation_selection_current_guard BEFORE INSERT OR UPDATE OR DELETE ON document_operation_selection_current
 FOR EACH ROW EXECUTE FUNCTION guard_document_operation_selection_current();

CREATE FUNCTION require_document_operation_selection_current() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM document_operation_selection_current
  WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
   AND owner_generation=NEW.owner_generation AND member_id=NEW.member_id AND selection_revision>=NEW.selection_revision) THEN
  RAISE EXCEPTION 'Document attempt selection must advance its current pointer in the same transaction';
 END IF;
 RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER document_operation_selection_current_required AFTER INSERT ON document_operation_selection_attempts
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_document_operation_selection_current();

CREATE FUNCTION initialize_document_operation_selection_history() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO document_operation_selection_attempts(account_id,principal,operation_id,owner_generation,member_id,selection_revision,attempt_id,created_at)
 VALUES(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation,NEW.member_id,1,NEW.attempt_id,NEW.created_at);
 INSERT INTO document_operation_selection_current VALUES(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation,NEW.member_id,1);
 RETURN NULL;
END;
$$;
CREATE TRIGGER document_operation_selection_history_initialize AFTER INSERT ON document_operation_selections
 FOR EACH ROW EXECUTE FUNCTION initialize_document_operation_selection_history();
CREATE OR REPLACE FUNCTION guard_document_operation_selection() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE a document_part_attempts%ROWTYPE; d drives%ROWTYPE; profile managed_backend_profiles%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Initial document operation selection is immutable'; END IF;
 PERFORM require_repository_operation_write_fence(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation);
 SELECT * INTO STRICT d FROM drives WHERE drive_id=NEW.drive_id FOR SHARE;
 SELECT * INTO STRICT profile FROM managed_backend_profiles WHERE generation=NEW.backend_generation;
 IF d.account_id<>NEW.account_id OR d.status<>'ACTIVE' OR d.provider<>profile.provider
  OR NEW.storage_realm<>profile.storage_realm OR NEW.storage_namespace<>d.bucket
  OR NEW.drive_snapshot IS DISTINCT FROM document_operation_drive_snapshot(d)
  OR NEW.drive_sha256 IS DISTINCT FROM document_operation_drive_digest_v1(d) THEN
  RAISE EXCEPTION 'Document operation selection differs from sampled placement';
 END IF;
 IF NEW.attempt_id IS NOT NULL THEN
  SELECT * INTO STRICT a FROM document_part_attempts WHERE attempt_id=NEW.attempt_id FOR SHARE;
  IF a.plan_kind<>'NEW_CONTENT' OR a.state<>'STAGING' OR a.lease_until<=clock_timestamp()
   OR ROW(a.account_id,a.operation_principal,a.operation_id,a.operation_generation,a.member_id,a.node_id,
          a.sampled_revision,a.drive_id,a.backend_generation,a.storage_realm,a.storage_namespace,a.planned_count)
    IS DISTINCT FROM ROW(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation,NEW.member_id,NEW.node_id,
          NEW.sampled_revision,NEW.drive_id,NEW.backend_generation,NEW.storage_realm,NEW.storage_namespace,NEW.upload_count)
   OR EXISTS(SELECT 1 FROM document_part_attempt_cleanup WHERE attempt_id=NEW.attempt_id) THEN
   RAISE EXCEPTION 'Document operation selection requires its exact live new-content attempt';
  END IF;
 END IF;
 RETURN NEW;
END;
$$;
