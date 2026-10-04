-- Reuse the immutable account-scoped artifact catalog for descriptor, canonical
-- metadata and optional source bytes. No public typed publisher is enabled here.
-- SQL binds storage claims; trusted admission must decode metadata and verify its
-- exact type/descriptor/source identity and compiler evidence before publication.
LOCK TABLE document_revision_commits, document_revision_publications,
 document_revision_schema_artifacts IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE document_revision_schema_assets (
 revision_id UUID NOT NULL REFERENCES document_revision_commits(revision_id),
 account_id VARCHAR(200) NOT NULL,
 principal VARCHAR(200) NOT NULL,
 operation_id UUID NOT NULL,
 owner_generation BIGINT NOT NULL CHECK(owner_generation>0),
 type_url TEXT NOT NULL CHECK(char_length(type_url) BETWEEN 1 AND 4096),
 -- A URL can exceed a PostgreSQL B-tree key. Index its digest but retain and
 -- compare the exact URL on reads; a collision must fail, never alias a type.
 type_url_sha256 BYTEA NOT NULL CHECK(type_url_sha256=sha256(convert_to(type_url,'UTF8'))),
 descriptor_sha256 BYTEA NOT NULL CHECK(octet_length(descriptor_sha256)=32),
 metadata_sha256 BYTEA NOT NULL CHECK(octet_length(metadata_sha256)=32),
 metadata_codec TEXT NOT NULL CHECK(metadata_codec='repository-schema-asset'),
 metadata_version INTEGER NOT NULL CHECK(metadata_version=1),
 source_sha256 BYTEA CHECK(octet_length(source_sha256)=32),
 PRIMARY KEY(revision_id,type_url_sha256,descriptor_sha256),
 CONSTRAINT revision_schema_asset_descriptor_ref FOREIGN KEY(revision_id,descriptor_sha256)
  REFERENCES document_revision_schema_artifacts(revision_id,artifact_sha256),
 CONSTRAINT revision_schema_asset_metadata_ref FOREIGN KEY(revision_id,metadata_sha256)
  REFERENCES document_revision_schema_artifacts(revision_id,artifact_sha256),
 CONSTRAINT revision_schema_asset_source_ref FOREIGN KEY(revision_id,source_sha256)
  REFERENCES document_revision_schema_artifacts(revision_id,artifact_sha256)
);

CREATE FUNCTION guard_document_revision_schema_asset() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE c document_revision_commits%ROWTYPE;
 r document_revision_publications%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Revision schema asset bindings are immutable'; END IF;
 PERFORM require_repository_operation_write_fence(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation);
 SELECT * INTO STRICT c FROM document_revision_commits WHERE revision_id=NEW.revision_id;
 IF c.account_id<>NEW.account_id OR c.principal<>NEW.principal OR c.operation_id<>NEW.operation_id
  OR c.owner_generation<>NEW.owner_generation OR c.creation_xid<>pg_current_xact_id() THEN
  RAISE EXCEPTION 'Revision schema asset differs from native commit owner or transaction';
 END IF;
 SELECT * INTO STRICT r FROM document_revision_publications WHERE revision_id=NEW.revision_id FOR UPDATE;
 IF r.native_binding IS DISTINCT FROM NEW.revision_id OR r.projection_sealed
  OR r.projection_xid<>pg_current_xact_id() THEN
  RAISE EXCEPTION 'Revision schema asset requires the current unsealed native projection';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM repository_schema_artifacts
  WHERE account_id=NEW.account_id AND artifact_sha256=NEW.metadata_sha256
   AND size_bytes BETWEEN 1 AND 524288) THEN
  RAISE EXCEPTION 'Revision schema metadata is missing or exceeds byte bound';
 END IF;
 IF (SELECT count(*) FROM document_revision_schema_assets WHERE revision_id=NEW.revision_id)>=64 THEN
  RAISE EXCEPTION 'Revision schema asset binding count exceeds bound';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER document_revision_schema_asset_guard
 BEFORE INSERT OR UPDATE OR DELETE ON document_revision_schema_assets
 FOR EACH ROW EXECUTE FUNCTION guard_document_revision_schema_asset();
