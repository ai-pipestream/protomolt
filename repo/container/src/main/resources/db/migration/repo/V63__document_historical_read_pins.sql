-- Historical reads use the same durable reader protection and drain/recovery
-- lifecycle. Current-source publication reuse retains its stronger current check.
-- The host must authorize the current document and lock the complete origin set
-- before retention rows. A pin is physical protection, not a read permission.
ALTER TABLE document_read_pins
 ADD COLUMN read_scope TEXT NOT NULL DEFAULT 'CURRENT'
 CHECK(read_scope IN ('CURRENT','HISTORICAL'));

CREATE OR REPLACE FUNCTION guard_document_read_pin() RETURNS trigger LANGUAGE plpgsql AS $$
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
  SELECT 1 FROM document_revision_publications r
  JOIN document_revision_parts p USING(revision_id)
  JOIN repository_physical_locations l ON l.object_id=p.object_id
  JOIN document_part_attempt_objects o ON o.physical_object_id=l.object_id
    AND o.attempt_id=l.source_id AND o.ordinal=l.source_ordinal AND o.verified
  JOIN document_part_attempts a ON a.attempt_id=o.attempt_id AND a.state='VERIFIED'
  WHERE r.node_id=NEW.source_node AND r.revision_id=NEW.source_revision
    AND r.publication_revision=NEW.publication_revision AND r.projection_sealed AND p.object_id=id
    AND repository_native_reference_exists(id,'DOCUMENT_HISTORY',r.revision_id,r.publication_revision)
    AND EXISTS(SELECT 1 FROM repository_object_references ref WHERE ref.object_id=id
      AND ref.owner_kind='DOCUMENT_HISTORY' AND ref.owner_id=r.revision_id AND ref.owner_revision=r.publication_revision)
    AND (NEW.read_scope='HISTORICAL' OR (NEW.read_scope='CURRENT' AND EXISTS(
      SELECT 1 FROM document_revision_current c JOIN repository_object_references ref
       ON ref.object_id=id AND ref.owner_kind='DOCUMENT_CURRENT' AND ref.owner_id=c.node_id
        AND ref.owner_revision=r.publication_revision
      WHERE c.node_id=r.node_id AND c.revision_id=r.revision_id)))
 ) THEN
  IF NEW.read_scope='CURRENT' THEN RAISE EXCEPTION 'Document read pin requires an open retained current source'; END IF;
  RAISE EXCEPTION 'Document read pin requires an open retained historical source';
 END IF;
 RETURN NEW;
END;
$$;
