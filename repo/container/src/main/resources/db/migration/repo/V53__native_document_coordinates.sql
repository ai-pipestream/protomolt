-- Independent revisions retain physical coordinates per part. A selected logical
-- drive is not evidence of a common physical prefix across those parts.
LOCK TABLE documents, document_part_publication_history, document_revision_commits
 IN SHARE ROW EXCLUSIVE MODE;
ALTER TABLE documents ALTER COLUMN object_key DROP NOT NULL;

CREATE FUNCTION require_native_document_coordinates() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM documents WHERE node_id=NEW.node_id AND object_key IS NOT NULL) THEN
  RAISE EXCEPTION 'Native revision must not claim a shared physical storage prefix';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_revision_commit_coordinates BEFORE INSERT ON document_revision_commits
 FOR EACH ROW EXECUTE FUNCTION require_native_document_coordinates();

CREATE FUNCTION require_legacy_document_coordinates() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE prefix text;
BEGIN
 SELECT object_key INTO STRICT prefix FROM documents WHERE node_id=NEW.node_id;
 IF prefix IS NULL OR btrim(prefix)='' THEN
  RAISE EXCEPTION 'Legacy publication requires a nonblank shared storage prefix';
 END IF;
 RETURN NEW;
END;
$$;
-- Preserve the existing cleanup-fence diagnostic before inspecting the document.
CREATE TRIGGER document_publication_coordinates_guard BEFORE INSERT ON document_part_publication_history
 FOR EACH ROW EXECUTE FUNCTION require_legacy_document_coordinates();

CREATE FUNCTION require_document_coordinate_authority() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE d documents%ROWTYPE;
BEGIN
 SELECT * INTO d FROM documents WHERE node_id=NEW.node_id;
 IF NOT FOUND THEN RETURN NULL; END IF;
 IF d.object_key IS NOT NULL AND btrim(d.object_key)<>'' THEN RETURN NULL; END IF;
 IF d.object_key IS NOT NULL OR NOT EXISTS(
  SELECT 1 FROM document_revision_current p JOIN document_revision_publications r USING(revision_id)
   JOIN document_revision_commits c USING(revision_id)
   JOIN repository_operation_success s USING(account_id,principal,operation_id,owner_generation)
  WHERE p.node_id=d.node_id AND r.node_id=d.node_id AND r.projection_sealed
   AND r.native_binding=r.revision_id AND c.account_id=d.account_id
   AND NOT EXISTS(SELECT 1 FROM document_part_publications legacy WHERE legacy.node_id=d.node_id)) THEN
  RAISE EXCEPTION 'Document without a shared prefix requires a committed native revision';
 END IF;
 RETURN NULL;
END;
$$;
-- V22's deferred publication consistency check separately proves exact body
-- equality. Do not repeat its potentially large manifest comparison here.
CREATE CONSTRAINT TRIGGER document_coordinate_authority AFTER INSERT OR UPDATE ON documents
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_document_coordinate_authority();
