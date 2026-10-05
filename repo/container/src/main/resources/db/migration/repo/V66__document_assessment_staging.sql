-- Internal staging only. The host must prove canonical observed-manifest,
-- command/slot equality, policy and authorization before using these tables.
-- No terminal rejection, replay reader or provider verification is enabled.
LOCK TABLE repository_object_references, document_part_attempt_cleanup IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE document_assessment_owners (
 assessment_id UUID PRIMARY KEY,
 account_id VARCHAR(200) NOT NULL,
 principal VARCHAR(200) NOT NULL,
 operation_id UUID NOT NULL,
 owner_generation BIGINT NOT NULL CHECK(owner_generation>0),
 command_codec VARCHAR(128) NOT NULL,
 command_version INTEGER NOT NULL,
 command_sha256 BYTEA NOT NULL CHECK(octet_length(command_sha256)=32),
 manifest_bytes BYTEA NOT NULL CHECK(octet_length(manifest_bytes) BETWEEN 1 AND 4194304),
 manifest_sha256 BYTEA NOT NULL CHECK(manifest_sha256=sha256(manifest_bytes)),
 expected_slots INTEGER NOT NULL CHECK(expected_slots BETWEEN 1 AND 10000),
 retain_until TIMESTAMPTZ NOT NULL,
 creation_xid xid8 NOT NULL,
 sealed BOOLEAN NOT NULL DEFAULT false,
 release_xid xid8,
 FOREIGN KEY(account_id,principal,operation_id) REFERENCES repository_operations,
 UNIQUE(account_id,principal,operation_id,owner_generation)
);
CREATE INDEX document_assessment_expiry ON document_assessment_owners(retain_until,assessment_id);
CREATE TABLE document_assessment_slots (
 assessment_id UUID NOT NULL REFERENCES document_assessment_owners,
 member_id VARCHAR(128) NOT NULL CHECK(member_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'),
 revision_ordinal INTEGER NOT NULL CHECK(revision_ordinal BETWEEN 0 AND 9999),
 selection_revision BIGINT NOT NULL CHECK(selection_revision>0),
 object_id UUID NOT NULL REFERENCES repository_physical_locations,
 declaration TEXT NOT NULL CHECK(declaration IN ('NEW_CONTENT','REUSE')),
 source_revision UUID,
 source_ordinal INTEGER,
 CHECK((declaration='NEW_CONTENT' AND source_revision IS NULL AND source_ordinal IS NULL)
  OR (declaration='REUSE' AND source_revision IS NOT NULL AND source_ordinal IS NOT NULL AND source_ordinal>=0)),
 PRIMARY KEY(assessment_id,member_id,revision_ordinal)
);
CREATE TABLE document_assessment_objects (
 assessment_id UUID NOT NULL REFERENCES document_assessment_owners,
 object_id UUID NOT NULL REFERENCES repository_physical_locations,
 PRIMARY KEY(assessment_id,object_id)
);
-- Object guards probe this once per distinct object; avoid repeatedly scanning
-- every candidate slot when one assessment contains thousands of objects.
CREATE INDEX document_assessment_slot_object ON document_assessment_slots(assessment_id,object_id);
CREATE INDEX document_assessment_object_owner ON document_assessment_objects(object_id,assessment_id);

ALTER TABLE repository_object_references DROP CONSTRAINT repository_object_references_owner_kind_check;
ALTER TABLE repository_object_references ADD CONSTRAINT repository_object_references_owner_kind_check
 CHECK(owner_kind IN ('ARCHIVE_VERSION','DOCUMENT_HISTORY','DOCUMENT_CURRENT','ARCHIVE_READER','DOCUMENT_READER','ASSESSMENT'));

CREATE OR REPLACE FUNCTION repository_native_reference_exists(id UUID, kind TEXT, owner UUID, revision BIGINT)
RETURNS BOOLEAN LANGUAGE sql VOLATILE AS $$
 SELECT CASE kind
 WHEN 'ARCHIVE_VERSION' THEN EXISTS(
  SELECT 1 FROM archive_version_object_refs r WHERE r.object_id=id AND r.entry_uuid=owner AND r.version=revision)
 WHEN 'DOCUMENT_HISTORY' THEN EXISTS(
  SELECT 1 FROM document_revision_publications r JOIN document_revision_parts p USING(revision_id)
  WHERE p.object_id=id AND r.revision_id=owner AND r.publication_revision=revision AND r.projection_sealed)
 WHEN 'DOCUMENT_CURRENT' THEN EXISTS(
  SELECT 1 FROM document_revision_current c JOIN document_revision_publications r USING(revision_id)
  JOIN document_revision_parts p USING(revision_id)
  WHERE p.object_id=id AND c.node_id=owner AND r.publication_revision=revision AND r.projection_sealed)
 WHEN 'ARCHIVE_READER' THEN EXISTS(
  SELECT 1 FROM archive_read_pins p WHERE p.object_id=id AND p.pin_id=owner AND p.version=revision)
 WHEN 'DOCUMENT_READER' THEN EXISTS(
  SELECT 1 FROM document_read_pins p WHERE p.object_id=id AND p.pin_id=owner AND p.publication_revision=revision)
 WHEN 'ASSESSMENT' THEN revision=1 AND EXISTS(
  SELECT 1 FROM document_assessment_objects p WHERE p.object_id=id AND p.assessment_id=owner)
 ELSE false END
$$;

CREATE FUNCTION guard_document_assessment_child() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE owner RECORD; id UUID;
BEGIN
 PERFORM require_repository_read_committed();
 IF TG_OP='UPDATE' THEN RAISE EXCEPTION 'Assessment associations are immutable'; END IF;
 id:=CASE WHEN TG_OP='DELETE' THEN OLD.assessment_id ELSE NEW.assessment_id END;
 SELECT account_id,principal,operation_id,owner_generation,creation_xid,sealed,release_xid
 INTO STRICT owner FROM document_assessment_owners WHERE assessment_id=id FOR UPDATE;
 IF TG_OP='DELETE' THEN
  PERFORM require_repository_operation_recovery_fence(owner.account_id,owner.principal,owner.operation_id);
  IF owner.release_xid IS DISTINCT FROM pg_current_xact_id_if_assigned() THEN
   RAISE EXCEPTION 'Assessment association release requires fenced owner recovery';
  END IF;
  RETURN OLD;
 END IF;
 PERFORM require_repository_operation_write_fence(owner.account_id,owner.principal,owner.operation_id,owner.owner_generation);
 IF owner.creation_xid IS DISTINCT FROM pg_current_xact_id_if_assigned() OR owner.release_xid IS NOT NULL
  OR (TG_TABLE_NAME='document_assessment_slots' AND owner.sealed)
  OR (TG_TABLE_NAME='document_assessment_objects' AND NOT owner.sealed) THEN
  RAISE EXCEPTION 'Assessment associations require the owner creation transaction and correct seal phase';
 END IF;
 IF TG_TABLE_NAME='document_assessment_objects' AND NOT EXISTS(
  SELECT 1 FROM document_assessment_slots WHERE assessment_id=id AND object_id=NEW.object_id) THEN
  RAISE EXCEPTION 'Assessment object requires an exact candidate association';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_assessment_slot_guard BEFORE INSERT OR UPDATE OR DELETE ON document_assessment_slots
 FOR EACH ROW EXECUTE FUNCTION guard_document_assessment_child();
CREATE TRIGGER document_assessment_object_guard BEFORE INSERT OR UPDATE OR DELETE ON document_assessment_objects
 FOR EACH ROW EXECUTE FUNCTION guard_document_assessment_child();

CREATE FUNCTION mirror_document_assessment_object() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='INSERT' THEN
  INSERT INTO repository_object_references VALUES(NEW.object_id,'ASSESSMENT',NEW.assessment_id,1);
  RETURN NEW;
 END IF;
 DELETE FROM repository_object_references WHERE object_id=OLD.object_id AND owner_kind='ASSESSMENT'
  AND owner_id=OLD.assessment_id AND owner_revision=1;
 RETURN OLD;
END;
$$;
CREATE TRIGGER document_assessment_object_mirror AFTER INSERT OR DELETE ON document_assessment_objects
 FOR EACH ROW EXECUTE FUNCTION mirror_document_assessment_object();

CREATE FUNCTION check_document_assessment_slots(p_owner document_assessment_owners)
RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
DECLARE objects UUID[]; matched INTEGER;
BEGIN
 IF (SELECT count(*) FROM document_assessment_slots WHERE assessment_id=p_owner.assessment_id)<>p_owner.expected_slots THEN
  RAISE EXCEPTION 'Assessment requires its complete declared candidate slot set';
 END IF;
 SELECT array_agg(DISTINCT object_id ORDER BY object_id) INTO objects
 FROM document_assessment_slots WHERE assessment_id=p_owner.assessment_id;
 PERFORM lock_repository_retention_set(objects);
 -- Every source lock is now held. Recheck time, current selection and physical
 -- eligibility after waits. Callers already hold the complete logical lock set.
 PERFORM require_repository_operation_write_fence(p_owner.account_id,p_owner.principal,p_owner.operation_id,p_owner.owner_generation);
 IF p_owner.retain_until<=clock_timestamp() THEN RAISE EXCEPTION 'Assessment staging deadline expired'; END IF;
 SELECT count(*) INTO matched FROM document_assessment_slots s
 JOIN document_operation_selection_current c
  ON c.account_id=p_owner.account_id AND c.principal=p_owner.principal AND c.operation_id=p_owner.operation_id
   AND c.owner_generation=p_owner.owner_generation AND c.member_id=s.member_id AND c.selection_revision=s.selection_revision
 JOIN document_operation_selection_attempts selected
  ON selected.account_id=c.account_id AND selected.principal=c.principal AND selected.operation_id=c.operation_id
   AND selected.owner_generation=c.owner_generation AND selected.member_id=c.member_id
   AND selected.selection_revision=c.selection_revision
 JOIN repository_physical_locations l ON l.object_id=s.object_id AND l.source_kind='DOCUMENT_PART'
 JOIN document_part_attempt_objects o ON o.attempt_id=l.source_id AND o.ordinal=l.source_ordinal
  AND o.physical_object_id=l.object_id AND o.verified
 JOIN document_part_attempts a ON a.attempt_id=o.attempt_id AND a.state='VERIFIED' AND a.account_id=p_owner.account_id
 JOIN repository_object_retention r ON r.object_id=l.object_id AND NOT r.retiring AND NOT r.reclaiming
 WHERE s.assessment_id=p_owner.assessment_id
  AND NOT EXISTS(SELECT 1 FROM document_part_attempt_cleanup cleanup WHERE cleanup.attempt_id=a.attempt_id)
  AND CASE s.declaration WHEN 'NEW_CONTENT' THEN
   a.plan_kind='NEW_CONTENT' AND a.lease_until>clock_timestamp() AND a.attempt_id=selected.attempt_id
   AND a.account_id=p_owner.account_id AND a.operation_principal=p_owner.principal
   AND a.operation_id=p_owner.operation_id AND a.operation_generation=p_owner.owner_generation
   AND a.member_id=s.member_id AND o.revision_ordinal=s.revision_ordinal
  ELSE EXISTS(
   SELECT 1 FROM document_revision_publications h
   JOIN document_revision_current current_ref ON current_ref.revision_id=h.revision_id AND current_ref.node_id=h.node_id
   JOIN documents d ON d.node_id=h.node_id AND d.account_id=p_owner.account_id
   JOIN document_revision_parts part ON part.revision_id=h.revision_id AND part.revision_ordinal=s.source_ordinal
    AND part.object_id=s.object_id
   WHERE h.revision_id=s.source_revision AND h.projection_sealed AND d.status='AVAILABLE' AND d.pending_purge_id IS NULL
    AND EXISTS(SELECT 1 FROM repository_object_references ref WHERE ref.object_id=s.object_id
     AND ref.owner_kind='DOCUMENT_HISTORY' AND ref.owner_id=h.revision_id AND ref.owner_revision=h.publication_revision)
    AND EXISTS(SELECT 1 FROM repository_object_references ref WHERE ref.object_id=s.object_id
     AND ref.owner_kind='DOCUMENT_CURRENT' AND ref.owner_id=h.node_id AND ref.owner_revision=h.publication_revision)
  ) END;
 IF matched<>p_owner.expected_slots THEN
  RAISE EXCEPTION 'Assessment candidate differs from selected verified uploads or retained current sources';
 END IF;
 -- The forward join is insufficient: a caller could omit a selected upload and
 -- lower expected_slots. Every selected member and every selected upload must
 -- appear, while zero-upload reuse members need no synthetic attempt.
 IF EXISTS(
  SELECT 1 FROM document_operation_selection_current c
  JOIN document_operation_selection_attempts selected
   USING(account_id,principal,operation_id,owner_generation,member_id,selection_revision)
  WHERE c.account_id=p_owner.account_id AND c.principal=p_owner.principal AND c.operation_id=p_owner.operation_id
   AND c.owner_generation=p_owner.owner_generation
   AND (NOT EXISTS(SELECT 1 FROM document_assessment_slots s WHERE s.assessment_id=p_owner.assessment_id AND s.member_id=c.member_id)
    OR EXISTS(SELECT 1 FROM document_part_attempt_objects o WHERE o.attempt_id=selected.attempt_id
     AND NOT EXISTS(SELECT 1 FROM document_assessment_slots s WHERE s.assessment_id=p_owner.assessment_id
      AND s.member_id=c.member_id AND s.declaration='NEW_CONTENT' AND s.selection_revision=c.selection_revision
      AND s.revision_ordinal=o.revision_ordinal AND s.object_id=o.physical_object_id)))
 ) THEN RAISE EXCEPTION 'Assessment omits a selected member or uploaded part'; END IF;
END;
$$;

CREATE FUNCTION guard_document_assessment_owner() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE command repository_operations%ROWTYPE; objects UUID[];
BEGIN
 PERFORM require_repository_read_committed();
 IF TG_OP='DELETE' THEN
  PERFORM require_repository_operation_recovery_fence(OLD.account_id,OLD.principal,OLD.operation_id);
  IF OLD.release_xid IS DISTINCT FROM pg_current_xact_id_if_assigned() THEN
   RAISE EXCEPTION 'Assessment owner deletion requires explicit recovery';
  END IF;
  RETURN OLD;
 END IF;
 IF TG_OP='INSERT' THEN
  PERFORM require_repository_operation_write_fence(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation);
  SELECT * INTO STRICT command FROM repository_operations WHERE account_id=NEW.account_id
   AND principal=NEW.principal AND operation_id=NEW.operation_id;
  IF ROW(NEW.command_codec,NEW.command_version,NEW.command_sha256)
   IS DISTINCT FROM ROW(command.command_codec,command.command_version,command.command_sha256)
   OR command.command_codec<>'document-publication' OR command.command_version<>1 THEN
   RAISE EXCEPTION 'Assessment differs from admitted document command';
  END IF;
  IF NEW.sealed OR NEW.release_xid IS NOT NULL OR NEW.retain_until<=clock_timestamp()
   OR NEW.retain_until>clock_timestamp()+interval '1 day' THEN
   RAISE EXCEPTION 'Assessment requires unsealed staging with an explicit deadline within one day';
  END IF;
  NEW.creation_xid:=pg_current_xact_id();
  RETURN NEW;
 END IF;
 IF (to_jsonb(NEW)-'sealed'-'release_xid') IS DISTINCT FROM (to_jsonb(OLD)-'sealed'-'release_xid') THEN
  RAISE EXCEPTION 'Assessment owner identity and evidence are immutable';
 END IF;
 IF NOT OLD.sealed AND NEW.sealed AND OLD.creation_xid=pg_current_xact_id_if_assigned()
  AND OLD.release_xid IS NULL AND NEW.release_xid IS NULL THEN
  PERFORM check_document_assessment_slots(NEW);
  RETURN NEW;
 END IF;
 IF OLD.sealed AND NEW.sealed AND OLD.release_xid IS NULL AND NEW.release_xid=pg_current_xact_id_if_assigned() THEN
  PERFORM require_repository_operation_recovery_fence(OLD.account_id,OLD.principal,OLD.operation_id);
  IF OLD.retain_until>clock_timestamp() THEN RAISE EXCEPTION 'Assessment staging has not expired'; END IF;
  SELECT array_agg(object_id ORDER BY object_id) INTO objects FROM document_assessment_objects WHERE assessment_id=OLD.assessment_id;
  PERFORM lock_repository_retention_set(COALESCE(objects,'{}'::UUID[]));
  RETURN NEW;
 END IF;
 RAISE EXCEPTION 'Unsupported assessment owner transition';
END;
$$;
CREATE TRIGGER document_assessment_owner_guard BEFORE INSERT OR UPDATE OR DELETE ON document_assessment_owners
 FOR EACH ROW EXECUTE FUNCTION guard_document_assessment_owner();

CREATE FUNCTION retain_document_assessment_objects() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT OLD.sealed AND NEW.sealed THEN
  INSERT INTO document_assessment_objects(assessment_id,object_id)
   SELECT NEW.assessment_id,object_id FROM document_assessment_slots WHERE assessment_id=NEW.assessment_id
   GROUP BY object_id ORDER BY object_id;
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_assessment_seal AFTER UPDATE ON document_assessment_owners
 FOR EACH ROW EXECUTE FUNCTION retain_document_assessment_objects();

CREATE FUNCTION require_complete_document_assessment() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE owner document_assessment_owners%ROWTYPE;
BEGIN
 SELECT * INTO owner FROM document_assessment_owners WHERE assessment_id=NEW.assessment_id;
 IF NOT FOUND THEN RETURN NULL; END IF;
 IF NOT owner.sealed OR owner.release_xid IS NOT NULL THEN
  RAISE EXCEPTION 'Assessment transaction must finish sealed or fully released';
 END IF;
 IF (SELECT count(*) FROM document_assessment_slots WHERE assessment_id=owner.assessment_id)<>owner.expected_slots
  OR EXISTS(SELECT 1 FROM document_assessment_slots s WHERE s.assessment_id=owner.assessment_id
    AND NOT EXISTS(SELECT 1 FROM document_assessment_objects o WHERE o.assessment_id=s.assessment_id AND o.object_id=s.object_id)) THEN
  RAISE EXCEPTION 'Assessment retained associations are incomplete';
 END IF;
 RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER document_assessment_complete AFTER INSERT OR UPDATE ON document_assessment_owners
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_complete_document_assessment();

-- Explicit recovery removes ownership; expiry alone never changes the native
-- reference predicate. No assessment reader pins exist yet. Future reader or
-- terminal ownership must extend this gate before those paths are enabled.
CREATE FUNCTION release_expired_document_assessment(p_account TEXT,p_principal TEXT,p_operation UUID,p_assessment UUID)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE AS $$
DECLARE owner document_assessment_owners%ROWTYPE;
BEGIN
 PERFORM fence_repository_operation_recovery(p_account,p_principal,p_operation);
 SELECT * INTO owner FROM document_assessment_owners WHERE assessment_id=p_assessment FOR UPDATE;
 IF NOT FOUND THEN RETURN false; END IF;
 IF ROW(owner.account_id,owner.principal,owner.operation_id) IS DISTINCT FROM ROW(p_account,p_principal,p_operation) THEN
  RAISE EXCEPTION 'Assessment recovery scope differs from owner';
 END IF;
 UPDATE document_assessment_owners SET release_xid=pg_current_xact_id() WHERE assessment_id=p_assessment;
 DELETE FROM document_assessment_slots WHERE assessment_id=p_assessment;
 DELETE FROM document_assessment_objects WHERE assessment_id=p_assessment;
 DELETE FROM document_assessment_owners WHERE assessment_id=p_assessment;
 RETURN true;
END;
$$;

CREATE FUNCTION protect_assessed_document_attempt_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM require_repository_read_committed();
 PERFORM 1 FROM document_part_attempts WHERE attempt_id=NEW.attempt_id FOR UPDATE;
 IF EXISTS(SELECT 1 FROM document_part_attempt_objects o JOIN repository_object_references r ON r.object_id=o.physical_object_id
  WHERE o.attempt_id=NEW.attempt_id AND r.owner_kind='ASSESSMENT') THEN
  RAISE EXCEPTION 'Assessed document attempts remain retained until explicit release';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER a0_document_assessment_cleanup_guard BEFORE INSERT OR UPDATE ON document_part_attempt_cleanup
 FOR EACH ROW EXECUTE FUNCTION protect_assessed_document_attempt_cleanup();
