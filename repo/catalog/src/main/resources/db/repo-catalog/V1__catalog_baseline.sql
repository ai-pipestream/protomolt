-- Parts-based repository catalog, fresh baseline (requirements §7, §14).
-- Applies only to an empty catalog; CatalogBaseline refuses any database that
-- already holds tables, including a V1-V120 repository ledger. Revisions, part
-- references and outcomes are immutable once written. Payload bytes live on the
-- stacks; this catalog holds references, binary protobuf records and the outbox.

CREATE FUNCTION require_catalog_read_committed() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'The repository catalog requires READ COMMITTED isolation';
    END IF;
END;
$$;

CREATE FUNCTION reject_catalog_rewrite() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% rows are immutable', TG_TABLE_NAME;
END;
$$;

-- One configured storage namespace. Repointing configuration creates a new stack.
CREATE TABLE catalog_stacks (
    stack_id   text PRIMARY KEY CHECK (length(btrim(stack_id)) > 0),
    provider   text NOT NULL CHECK (length(btrim(provider)) > 0),
    endpoint   text NOT NULL,
    namespace  text NOT NULL CHECK (length(btrim(namespace)) > 0),
    record     bytea NOT NULL,
    created_at timestamptz NOT NULL,
    UNIQUE (provider, endpoint, namespace)
);
CREATE TRIGGER catalog_stacks_immutable BEFORE UPDATE OR DELETE ON catalog_stacks
    FOR EACH ROW EXECUTE FUNCTION reject_catalog_rewrite();

-- The call number and its current-revision pointer: the compare-and-set row.
CREATE TABLE catalog_documents (
    document_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id       text NOT NULL CHECK (length(btrim(account_id)) > 0),
    graph_id         text NOT NULL CHECK (length(btrim(graph_id)) > 0),
    graph_address_id text NOT NULL CHECK (length(btrim(graph_address_id)) > 0),
    doc_id           text NOT NULL CHECK (length(btrim(doc_id)) > 0),
    current_revision bigint NOT NULL CHECK (current_revision > 0),
    tombstone        boolean NOT NULL,
    catalog_entry    bytea NOT NULL,
    catalog_entry_sha256 char(64) NOT NULL,
    updated_at       timestamptz NOT NULL,
    UNIQUE (account_id, graph_id, graph_address_id, doc_id)
);
CREATE INDEX catalog_documents_listing ON catalog_documents
    (account_id, graph_id COLLATE "C", graph_address_id COLLATE "C", doc_id COLLATE "C");
CREATE TRIGGER catalog_documents_no_delete BEFORE DELETE ON catalog_documents
    FOR EACH ROW EXECUTE FUNCTION reject_catalog_rewrite();

-- One accession. manifest is a binary StoredManifest; manifest_sha256 guards it.
CREATE TABLE catalog_revisions (
    document_id       bigint NOT NULL REFERENCES catalog_documents,
    revision          bigint NOT NULL CHECK (revision > 0),
    previous_revision bigint NOT NULL CHECK (previous_revision >= 0 AND previous_revision < revision),
    operation_id      text NOT NULL,
    intent_sha256     char(64) NOT NULL,
    tombstone         boolean NOT NULL,
    validation_status smallint NOT NULL,
    committed_at      timestamptz NOT NULL,
    manifest          bytea NOT NULL,
    manifest_sha256   char(64) NOT NULL,
    PRIMARY KEY (document_id, revision)
);
CREATE TRIGGER catalog_revisions_immutable BEFORE UPDATE OR DELETE ON catalog_revisions
    FOR EACH ROW EXECUTE FUNCTION reject_catalog_rewrite();

-- One write-once stack object. purged is the only mutable column (admin purge).
CREATE TABLE catalog_objects (
    stack_id   text NOT NULL REFERENCES catalog_stacks,
    object_key text NOT NULL CHECK (length(object_key) > 0),
    sha256     char(64) NOT NULL,
    length     bigint NOT NULL CHECK (length >= 0),
    purged     boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (stack_id, object_key)
);
CREATE FUNCTION protect_catalog_object() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'catalog_objects rows are kept; purge marks them'; END IF;
    IF (NEW.stack_id, NEW.object_key, NEW.sha256, NEW.length, NEW.created_at)
            IS DISTINCT FROM (OLD.stack_id, OLD.object_key, OLD.sha256, OLD.length, OLD.created_at)
            OR (OLD.purged AND NOT NEW.purged) THEN
        RAISE EXCEPTION 'catalog object identity is immutable and purge is final';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER catalog_objects_guard BEFORE UPDATE OR DELETE ON catalog_objects
    FOR EACH ROW EXECUTE FUNCTION protect_catalog_object();

-- Every part of every accession, in codec order. PRESENT parts reference an object.
CREATE TABLE catalog_part_refs (
    document_id         bigint NOT NULL,
    revision            bigint NOT NULL,
    ordinal             integer NOT NULL CHECK (ordinal >= 0),
    selector            bytea NOT NULL,
    state               smallint NOT NULL CHECK (state IN (1, 2, 3)),
    stack_id            text,
    object_key          text,
    written_in_revision bigint NOT NULL CHECK (written_in_revision > 0 AND written_in_revision <= revision),
    PRIMARY KEY (document_id, revision, ordinal),
    UNIQUE (document_id, revision, selector),
    FOREIGN KEY (document_id, revision) REFERENCES catalog_revisions,
    FOREIGN KEY (stack_id, object_key) REFERENCES catalog_objects,
    CHECK ((state = 1) = (stack_id IS NOT NULL AND object_key IS NOT NULL))
);
CREATE INDEX catalog_part_refs_object ON catalog_part_refs (stack_id, object_key);
CREATE TRIGGER catalog_part_refs_immutable BEFORE UPDATE OR DELETE ON catalog_part_refs
    FOR EACH ROW EXECUTE FUNCTION reject_catalog_rewrite();

-- Each raw blob of a BLOBS part is its own stack object (decision 45). Blob
-- references count with part references for purge and the carry-forward check.
CREATE TABLE catalog_blob_refs (
    document_id bigint NOT NULL,
    revision    bigint NOT NULL,
    blob_id     text NOT NULL CHECK (length(blob_id) > 0),
    stack_id    text NOT NULL,
    object_key  text NOT NULL,
    PRIMARY KEY (document_id, revision, blob_id),
    FOREIGN KEY (document_id, revision) REFERENCES catalog_revisions,
    FOREIGN KEY (stack_id, object_key) REFERENCES catalog_objects
);
CREATE INDEX catalog_blob_refs_object ON catalog_blob_refs (stack_id, object_key);
CREATE TRIGGER catalog_blob_refs_immutable BEFORE UPDATE OR DELETE ON catalog_blob_refs
    FOR EACH ROW EXECUTE FUNCTION reject_catalog_rewrite();

-- Terminal outcomes, unique by account and operation ID. outcome is a binary
-- RecordedOutcome; committed outcomes name their accession.
CREATE TABLE catalog_operation_outcomes (
    account_id    text NOT NULL CHECK (length(btrim(account_id)) > 0),
    operation_id  text NOT NULL CHECK (length(btrim(operation_id)) > 0),
    intent_sha256 char(64) NOT NULL,
    document_id   bigint,
    revision      bigint,
    outcome       bytea NOT NULL,
    recorded_at   timestamptz NOT NULL,
    rejection_address bytea,
    rejection_create boolean,
    rejection_issuer text,
    rejection_credential_id uuid,
    CONSTRAINT rejection_access_complete CHECK (
        (rejection_address IS NULL AND rejection_create IS NULL AND rejection_issuer IS NULL AND rejection_credential_id IS NULL)
        OR (rejection_address IS NOT NULL AND rejection_create IS NOT NULL
            AND ((rejection_issuer IS NULL) = (rejection_credential_id IS NULL)))),
    PRIMARY KEY (account_id, operation_id),
    FOREIGN KEY (document_id, revision) REFERENCES catalog_revisions,
    CHECK ((document_id IS NULL) = (revision IS NULL))
);
CREATE TRIGGER catalog_operation_outcomes_immutable BEFORE UPDATE OR DELETE ON catalog_operation_outcomes
    FOR EACH ROW EXECUTE FUNCTION reject_catalog_rewrite();

-- One binary DocumentEvent per accession, written in the commit transaction.
-- Relaying stamps relayed_at after the broker acknowledges; delivery is at-least-once.
CREATE TABLE catalog_outbox (
    sequence    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id    text NOT NULL UNIQUE,
    document_id bigint NOT NULL,
    revision    bigint NOT NULL,
    event_key   text NOT NULL,
    payload     bytea NOT NULL,
    created_at  timestamptz NOT NULL,
    relayed_at  timestamptz,
    attempts    integer NOT NULL DEFAULT 0,
    UNIQUE (document_id, revision),
    FOREIGN KEY (document_id, revision) REFERENCES catalog_revisions
);
CREATE INDEX catalog_outbox_pending ON catalog_outbox (sequence) WHERE relayed_at IS NULL;

-- Retained schema bytes (descriptor closures, metadata, sources) by SHA-256.
CREATE TABLE catalog_schema_artifacts (
    sha256  char(64) PRIMARY KEY,
    content bytea NOT NULL,
    CHECK (encode(sha256(content), 'hex') = sha256)
);
CREATE TRIGGER catalog_schema_artifacts_immutable BEFORE UPDATE OR DELETE ON catalog_schema_artifacts
    FOR EACH ROW EXECUTE FUNCTION reject_catalog_rewrite();

-- Credential authority, as the V99 ledger table: rotation moves to the next
-- generation, revocation is final, and nothing is deleted.
CREATE TABLE catalog_credential_authorities (
    issuer        varchar(128) NOT NULL CHECK (length(btrim(issuer)) > 0 AND issuer !~ '[[:cntrl:]]'),
    credential_id uuid NOT NULL,
    generation    bigint NOT NULL CHECK (generation > 0),
    principal     varchar(200) NOT NULL CHECK (length(btrim(principal)) > 0 AND principal !~ '[[:cntrl:]]'),
    revoked       boolean NOT NULL DEFAULT false,
    PRIMARY KEY (issuer, credential_id)
);
CREATE FUNCTION protect_catalog_credential_authority() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM require_catalog_read_committed();
    IF TG_OP = 'INSERT' THEN RETURN NEW; END IF;
    IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'Credential authority tombstones cannot be deleted'; END IF;
    IF (NEW.issuer, NEW.credential_id, NEW.principal) IS DISTINCT FROM (OLD.issuer, OLD.credential_id, OLD.principal) THEN
        RAISE EXCEPTION 'Credential authority identity is immutable';
    END IF;
    IF NEW.generation = OLD.generation THEN
        IF OLD.revoked AND NOT NEW.revoked THEN RAISE EXCEPTION 'Revoked credential generation cannot be revived'; END IF;
    ELSIF NEW.generation <= OLD.generation OR NEW.generation - OLD.generation <> 1 OR NEW.revoked THEN
        RAISE EXCEPTION 'Credential rotation requires the next active generation';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER catalog_credential_authority_guard BEFORE INSERT OR UPDATE OR DELETE ON catalog_credential_authorities
    FOR EACH ROW EXECUTE FUNCTION protect_catalog_credential_authority();

-- Shared lookup in one round trip. Rotation and revocation are UPDATEs, so a
-- commit holding this share lock finishes first and a later one sees the change.
CREATE FUNCTION lock_catalog_credential_authority(a text, k uuid)
RETURNS TABLE (generation bigint, principal varchar, revoked boolean) LANGUAGE plpgsql AS $$
BEGIN
    PERFORM require_catalog_read_committed();
    RETURN QUERY SELECT c.generation, c.principal, c.revoked FROM catalog_credential_authorities c
        WHERE c.issuer = a AND c.credential_id = k FOR SHARE OF c;
END;
$$;

-- Immutable writer grants (decisions 25-27). snapshot is the exact binary
-- WriterGrant; revocation is the only change and is final.
CREATE TABLE catalog_writer_grants (
    grant_id        text PRIMARY KEY CHECK (length(btrim(grant_id)) > 0),
    account_id      text NOT NULL CHECK (length(btrim(account_id)) > 0),
    issuer          varchar(128) NOT NULL,
    credential_id   uuid NOT NULL,
    snapshot        bytea NOT NULL,
    snapshot_sha256 char(64) NOT NULL,
    revoked         boolean NOT NULL DEFAULT false,
    created_at      timestamptz NOT NULL,
    CHECK (encode(sha256(snapshot), 'hex') = snapshot_sha256)
);
CREATE INDEX catalog_writer_grants_credential ON catalog_writer_grants (account_id, issuer, credential_id);
CREATE FUNCTION protect_catalog_writer_grant() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM require_catalog_read_committed();
    IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'Writer grants are never deleted'; END IF;
    IF (NEW.grant_id, NEW.account_id, NEW.issuer, NEW.credential_id, NEW.snapshot, NEW.snapshot_sha256, NEW.created_at)
            IS DISTINCT FROM (OLD.grant_id, OLD.account_id, OLD.issuer, OLD.credential_id, OLD.snapshot,
                OLD.snapshot_sha256, OLD.created_at)
            OR (OLD.revoked AND NOT NEW.revoked) THEN
        RAISE EXCEPTION 'Writer grants are immutable and revocation is final';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER catalog_writer_grant_guard BEFORE UPDATE OR DELETE ON catalog_writer_grants
    FOR EACH ROW EXECUTE FUNCTION protect_catalog_writer_grant();

-- The commit's conflict point with revocation, copied from the credential lock.
CREATE FUNCTION lock_catalog_writer_grant(g text)
RETURNS TABLE (snapshot_sha256 char(64), revoked boolean) LANGUAGE plpgsql AS $$
BEGIN
    PERFORM require_catalog_read_committed();
    RETURN QUERY SELECT w.snapshot_sha256, w.revoked FROM catalog_writer_grants w
        WHERE w.grant_id = g FOR SHARE OF w;
END;
$$;
