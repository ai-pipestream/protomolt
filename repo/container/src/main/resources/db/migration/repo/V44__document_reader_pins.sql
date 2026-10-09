-- Native document reader protection only. Hosts must still acquire whole-plan
-- pins under sorted locks and drain actual I/O before release. No expiry.
LOCK TABLE repository_object_references IN SHARE ROW EXCLUSIVE MODE;
CREATE TABLE document_read_pins (
    pin_id UUID PRIMARY KEY,
    reader_incarnation UUID NOT NULL REFERENCES repository_reader_incarnations(incarnation),
    object_id UUID NOT NULL REFERENCES repository_physical_locations(object_id),
    source_node UUID NOT NULL,
    source_revision UUID NOT NULL,
    publication_revision BIGINT NOT NULL CHECK(publication_revision>0),
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
-- No source document/history FK: logical deletion must not release active I/O.
CREATE INDEX document_read_pin_object ON document_read_pins(object_id);
CREATE INDEX document_read_pin_incarnation ON document_read_pins(reader_incarnation);
ALTER TABLE repository_object_references DROP CONSTRAINT repository_object_references_owner_kind_check;
ALTER TABLE repository_object_references ADD CONSTRAINT repository_object_references_owner_kind_check
 CHECK(owner_kind IN ('ARCHIVE_VERSION','DOCUMENT_HISTORY','DOCUMENT_CURRENT','ARCHIVE_READER','DOCUMENT_READER'));

CREATE FUNCTION share_document_retention_owner(id UUID) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE origin UUID;
BEGIN
 PERFORM require_repository_read_committed();
 SELECT source_id INTO origin FROM repository_physical_locations WHERE object_id=id AND source_kind='DOCUMENT_PART';
 IF NOT FOUND THEN RAISE EXCEPTION 'Document read requires a registered document object'; END IF;
 PERFORM 1 FROM document_part_attempts WHERE attempt_id=origin FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Document read requires a durable origin'; END IF;
END;
$$;

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
 ELSE false END
$$;

CREATE OR REPLACE FUNCTION guard_repository_reference() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE id UUID; kind TEXT; fenced BOOLEAN;
BEGIN
 IF TG_OP='UPDATE' THEN RAISE EXCEPTION 'Repository reference identity is immutable'; END IF;
 id := CASE WHEN TG_OP='DELETE' THEN OLD.object_id ELSE NEW.object_id END;
 kind := CASE WHEN TG_OP='DELETE' THEN OLD.owner_kind ELSE NEW.owner_kind END;
 IF kind='ARCHIVE_READER' THEN
  PERFORM share_archive_retention_owner(id);
  SELECT retiring INTO STRICT fenced FROM repository_object_retention WHERE object_id=id FOR SHARE;
 ELSIF kind='DOCUMENT_READER' THEN
  PERFORM share_document_retention_owner(id);
  SELECT retiring OR reclaiming INTO STRICT fenced FROM repository_object_retention WHERE object_id=id FOR SHARE;
 ELSE
  PERFORM lock_repository_retention_owner(id);
  SELECT retiring INTO STRICT fenced FROM repository_object_retention WHERE object_id=id FOR UPDATE;
 END IF;
 IF TG_OP='INSERT' THEN
  IF fenced THEN RAISE EXCEPTION 'Repository object is permanently fenced for retirement or reclamation'; END IF;
  IF NOT repository_native_reference_exists(id,NEW.owner_kind,NEW.owner_id,NEW.owner_revision) THEN
   RAISE EXCEPTION 'Repository reference requires its exact durable native owner'; END IF;
  RETURN NEW;
 END IF;
 IF repository_native_reference_exists(id,OLD.owner_kind,OLD.owner_id,OLD.owner_revision) THEN
  RAISE EXCEPTION 'Repository reference cannot release a retained native owner'; END IF;
 RETURN OLD;
END;
$$;

CREATE FUNCTION guard_document_read_pin() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE id UUID; closed BOOLEAN;
BEGIN
 PERFORM require_repository_read_committed();
 IF TG_OP='UPDATE' THEN RAISE EXCEPTION 'Document read pin identity is immutable'; END IF;
 IF TG_OP='INSERT' THEN PERFORM require_active_repository_reader(NEW.reader_incarnation); END IF;
 id := CASE WHEN TG_OP='DELETE' THEN OLD.object_id ELSE NEW.object_id END;
 PERFORM share_document_retention_owner(id);
 SELECT retiring OR reclaiming INTO STRICT closed FROM repository_object_retention WHERE object_id=id FOR SHARE;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 IF closed OR NOT EXISTS(
  SELECT 1 FROM document_revision_current c
  JOIN document_revision_publications r USING(revision_id)
  JOIN document_revision_parts p USING(revision_id)
  JOIN repository_physical_locations l ON l.object_id=p.object_id
  JOIN document_part_attempt_objects o ON o.physical_object_id=l.object_id
    AND o.attempt_id=l.source_id AND o.ordinal=l.source_ordinal AND o.verified
  JOIN document_part_attempts a ON a.attempt_id=o.attempt_id AND a.state='VERIFIED'
  WHERE c.node_id=NEW.source_node AND r.node_id=c.node_id AND r.revision_id=NEW.source_revision
    AND r.publication_revision=NEW.publication_revision AND r.projection_sealed AND p.object_id=id
    AND repository_native_reference_exists(id,'DOCUMENT_HISTORY',r.revision_id,r.publication_revision)
    AND EXISTS(SELECT 1 FROM repository_object_references ref WHERE ref.object_id=id
      AND ref.owner_kind='DOCUMENT_HISTORY' AND ref.owner_id=r.revision_id AND ref.owner_revision=r.publication_revision)
    AND EXISTS(SELECT 1 FROM repository_object_references ref WHERE ref.object_id=id
      AND ref.owner_kind='DOCUMENT_CURRENT' AND ref.owner_id=c.node_id AND ref.owner_revision=r.publication_revision)
 ) THEN RAISE EXCEPTION 'Document read pin requires an open retained current source'; END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_read_pin_guard BEFORE INSERT OR UPDATE OR DELETE ON document_read_pins
 FOR EACH ROW EXECUTE FUNCTION guard_document_read_pin();
CREATE FUNCTION mirror_document_read_pin() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='INSERT' THEN
  INSERT INTO repository_object_references VALUES(NEW.object_id,'DOCUMENT_READER',NEW.pin_id,NEW.publication_revision);
  RETURN NEW;
 END IF;
 DELETE FROM repository_object_references WHERE object_id=OLD.object_id AND owner_kind='DOCUMENT_READER'
  AND owner_id=OLD.pin_id AND owner_revision=OLD.publication_revision;
 RETURN OLD;
END;
$$;
CREATE TRIGGER document_read_pin_mirror AFTER INSERT OR DELETE ON document_read_pins
 FOR EACH ROW EXECUTE FUNCTION mirror_document_read_pin();

CREATE FUNCTION release_document_read_pin(p_pin UUID, p_reader UUID, p_object UUID)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE AS $$
DECLARE deleted_rows BIGINT;
BEGIN
 IF p_pin IS NULL OR p_reader IS NULL OR p_object IS NULL THEN
  RAISE EXCEPTION 'Document read release requires complete identity'; END IF;
 PERFORM share_document_retention_owner(p_object);
 PERFORM 1 FROM repository_object_retention WHERE object_id=p_object FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Repository object retention row is missing'; END IF;
 DELETE FROM document_read_pins WHERE pin_id=p_pin AND reader_incarnation=p_reader AND object_id=p_object;
 GET DIAGNOSTICS deleted_rows = ROW_COUNT;
 IF deleted_rows=0 AND EXISTS(SELECT 1 FROM document_read_pins WHERE pin_id=p_pin) THEN
  RAISE EXCEPTION 'Document read pin belongs to another incarnation or object'; END IF;
 RETURN true;
END;
$$;
