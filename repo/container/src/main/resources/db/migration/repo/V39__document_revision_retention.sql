-- Drain writers/recovery before rollout. Full-revision publication guards remain.
-- Revision ownership is independent of physical objects' upload-attempt origins.
LOCK TABLE documents, document_part_attempts, document_part_attempt_objects,
 document_part_publication_history, document_part_publications,
 document_revision_publications, document_revision_parts, document_revision_current,
 repository_physical_locations, repository_object_retention, repository_object_references
 IN SHARE ROW EXCLUSIVE MODE;

-- A migration-local assertion: neither missing nor extra pins may be repaired by
-- guessing. Existing revision IDs equal legacy attempt IDs, so no keys change.
CREATE FUNCTION assert_document_revision_retention() RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS(
  WITH expected AS MATERIALIZED (
   SELECT DISTINCT p.object_id,'DOCUMENT_HISTORY'::text owner_kind,r.revision_id owner_id,r.publication_revision owner_revision
   FROM document_revision_publications r JOIN document_revision_parts p USING(revision_id) WHERE r.projection_sealed
   UNION ALL
   SELECT DISTINCT p.object_id,'DOCUMENT_CURRENT',c.node_id,r.publication_revision
   FROM document_revision_current c JOIN document_revision_publications r USING(revision_id)
   JOIN document_revision_parts p USING(revision_id) WHERE r.projection_sealed
  ), actual AS MATERIALIZED (
   SELECT object_id,owner_kind,owner_id,owner_revision FROM repository_object_references
   WHERE owner_kind IN ('DOCUMENT_HISTORY','DOCUMENT_CURRENT')
  )
  (SELECT * FROM expected EXCEPT SELECT * FROM actual)
  UNION ALL (SELECT * FROM actual EXCEPT SELECT * FROM expected)
 ) THEN RAISE EXCEPTION 'Document revision retention differs from existing references'; END IF;
END;
$$;
SELECT assert_document_revision_retention();

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
 ELSE false END
$$;

DROP TRIGGER document_history_reference_mirror ON document_part_publication_history;
DROP TRIGGER document_current_reference_mirror ON document_part_publications;
DROP FUNCTION mirror_document_history_retention();
DROP FUNCTION mirror_document_current_retention();

CREATE FUNCTION mirror_document_revision_history_retention() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO repository_object_references(object_id,owner_kind,owner_id,owner_revision)
 SELECT DISTINCT object_id,'DOCUMENT_HISTORY',NEW.revision_id,NEW.publication_revision
 FROM document_revision_parts WHERE revision_id=NEW.revision_id ORDER BY object_id;
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_revision_history_reference_mirror AFTER UPDATE OF projection_sealed ON document_revision_publications
 FOR EACH ROW WHEN (NOT OLD.projection_sealed AND NEW.projection_sealed)
 EXECUTE FUNCTION mirror_document_revision_history_retention();

CREATE FUNCTION mirror_document_revision_current_retention() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE revision BIGINT; old_revision UUID; new_revision UUID;
BEGIN
 IF TG_OP='UPDATE' AND ROW(NEW.node_id,NEW.revision_id) IS NOT DISTINCT FROM ROW(OLD.node_id,OLD.revision_id) THEN RETURN NEW; END IF;
 IF TG_OP<>'INSERT' THEN old_revision := OLD.revision_id; END IF;
 IF TG_OP<>'DELETE' THEN new_revision := NEW.revision_id; END IF;
 -- A document cascade can reach this table before the legacy pin's owner-lock
 -- trigger. Acquire the complete origin set for this switch, then retention.
 -- The Java batch prelocks the union across all members before any mirrors run;
 -- this per-switch ordering does not establish general multi-member SQL admission.
 PERFORM 1 FROM document_part_attempts a WHERE a.attempt_id IN (
  SELECT l.source_id FROM document_revision_parts p JOIN repository_physical_locations l USING(object_id)
  WHERE p.revision_id IN (old_revision,new_revision) AND l.source_kind='DOCUMENT_PART'
 ) ORDER BY a.attempt_id FOR UPDATE OF a;
 PERFORM 1 FROM repository_object_retention r WHERE r.object_id IN (
  SELECT object_id FROM document_revision_parts WHERE revision_id IN (old_revision,new_revision)
 ) ORDER BY r.object_id FOR UPDATE OF r;
 IF TG_OP<>'INSERT' THEN
  -- All affected retention rows are locked, so deletion visitation order cannot
  -- introduce additional lock waits. Use one statement, not one per part.
  SELECT publication_revision INTO STRICT revision FROM document_revision_publications WHERE revision_id=OLD.revision_id;
  DELETE FROM repository_object_references WHERE owner_kind='DOCUMENT_CURRENT'
   AND owner_id=OLD.node_id AND owner_revision=revision;
 END IF;
 IF TG_OP<>'DELETE' THEN
  SELECT publication_revision INTO STRICT revision FROM document_revision_publications WHERE revision_id=NEW.revision_id;
  INSERT INTO repository_object_references(object_id,owner_kind,owner_id,owner_revision)
  SELECT DISTINCT object_id,'DOCUMENT_CURRENT',NEW.node_id,revision
  FROM document_revision_parts WHERE revision_id=NEW.revision_id ORDER BY object_id;
 END IF;
 RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
END;
$$;
CREATE TRIGGER document_revision_current_reference_mirror AFTER INSERT OR UPDATE OR DELETE ON document_revision_current
 FOR EACH ROW EXECUTE FUNCTION mirror_document_revision_current_retention();

SELECT assert_document_revision_retention();
DROP FUNCTION assert_document_revision_retention();
