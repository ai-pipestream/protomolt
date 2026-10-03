CREATE TABLE archive_mutations (
    account_id varchar(200) NOT NULL,
    principal varchar(200) NOT NULL CHECK (btrim(principal) <> ''),
    operation_id uuid NOT NULL,
    command_sha256 varchar(64) NOT NULL CHECK (command_sha256 ~ '^[0-9a-f]{64}$'),
    command bytea NOT NULL,
    admission_receipt bytea NOT NULL,
    sampled_revision bigint NOT NULL CHECK (sampled_revision >= 0),
    admitted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (account_id,principal,operation_id)
);
CREATE TABLE archive_mutation_targets (
    account_id varchar(200) NOT NULL,
    principal varchar(200) NOT NULL,
    operation_id uuid NOT NULL,
    object_id uuid NOT NULL REFERENCES archive_object_bindings(object_id),
    PRIMARY KEY (account_id,principal,operation_id,object_id),
    FOREIGN KEY (account_id,principal,operation_id) REFERENCES archive_mutations(account_id,principal,operation_id)
);
CREATE INDEX archive_mutation_targets_object ON archive_mutation_targets(object_id);

-- Admission and its target set are immutable. Physical observations will be
-- recorded separately; a replay never resamples a recreated entry.
CREATE FUNCTION reject_archive_mutation_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'Archive mutation admission is immutable'; END;
$$;
CREATE TRIGGER archive_mutation_immutable BEFORE UPDATE OR DELETE ON archive_mutations
FOR EACH ROW EXECUTE FUNCTION reject_archive_mutation_change();
CREATE TRIGGER archive_mutation_target_immutable BEFORE UPDATE OR DELETE ON archive_mutation_targets
FOR EACH ROW EXECUTE FUNCTION reject_archive_mutation_change();

-- Reference publication and cleanup admission serialize on the same upload.
-- The durable target fences future publication even before a cleanup worker
-- claims the object. A LIVE state alone must not resurrect admitted targets.
CREATE OR REPLACE FUNCTION require_live_archive_reference() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE upload_state text; owner_entry uuid;
BEGIN
    SELECT u.state,b.entry_uuid INTO upload_state,owner_entry
    FROM archive_object_uploads u JOIN archive_object_bindings b USING(object_id)
    WHERE u.object_id=NEW.object_id FOR UPDATE OF u;
    IF upload_state IS DISTINCT FROM 'LIVE' OR owner_entry IS DISTINCT FROM NEW.entry_uuid THEN
        RAISE EXCEPTION 'Archive reference requires a live object owned by the entry';
    END IF;
    IF EXISTS (SELECT 1 FROM archive_mutation_targets WHERE object_id=NEW.object_id) THEN
        RAISE EXCEPTION 'Archive reference cannot republish an admitted mutation target';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION require_unpinned_archive_mutation_target() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE upload_state text;
BEGIN
    SELECT state INTO upload_state FROM archive_object_uploads WHERE object_id=NEW.object_id FOR UPDATE;
    IF upload_state IS NULL OR upload_state NOT IN ('LIVE','DELETING','DELETED') THEN
        RAISE EXCEPTION 'Archive mutation target requires a published object';
    END IF;
    IF EXISTS (SELECT 1 FROM archive_version_object_refs WHERE object_id=NEW.object_id) THEN
        RAISE EXCEPTION 'Archive mutation target has retained references';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER archive_mutation_target_unpinned BEFORE INSERT ON archive_mutation_targets
FOR EACH ROW EXECUTE FUNCTION require_unpinned_archive_mutation_target();
