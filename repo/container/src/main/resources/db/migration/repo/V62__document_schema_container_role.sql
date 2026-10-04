-- Preserve the exact containing Document association, rather than guessing among
-- aliases sharing a descriptor. Existing admissions retain an unknown role; no
-- historical provenance is invented. New typed admissions must supply the role.
ALTER TABLE document_revision_schema_admissions
 ADD COLUMN container_type_url_sha256 BYTEA,
 ADD COLUMN container_descriptor_sha256 BYTEA,
 ADD CONSTRAINT document_schema_container_pair CHECK(
  (container_type_url_sha256 IS NULL AND container_descriptor_sha256 IS NULL)
  OR (container_type_url_sha256 IS NOT NULL AND container_descriptor_sha256 IS NOT NULL
   AND octet_length(container_type_url_sha256)=32 AND octet_length(container_descriptor_sha256)=32)),
 ADD CONSTRAINT document_schema_container_association FOREIGN KEY(revision_id,container_type_url_sha256,container_descriptor_sha256)
  REFERENCES document_revision_schema_assets(revision_id,type_url_sha256,descriptor_sha256)
  DEFERRABLE INITIALLY DEFERRED;

CREATE FUNCTION guard_document_schema_container_role() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF (NEW.decision='TYPED' AND (NEW.container_type_url_sha256 IS NULL OR NEW.container_descriptor_sha256 IS NULL))
  OR (NEW.decision='OPAQUE' AND (NEW.container_type_url_sha256 IS NOT NULL OR NEW.container_descriptor_sha256 IS NOT NULL)) THEN
  RAISE EXCEPTION 'Document schema admission requires its exact container role';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_schema_container_role BEFORE INSERT ON document_revision_schema_admissions
 FOR EACH ROW EXECUTE FUNCTION guard_document_schema_container_role();
