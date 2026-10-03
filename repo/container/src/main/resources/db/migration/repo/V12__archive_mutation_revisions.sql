-- Version numbers alone do not observe metadata edits or retained-manifest rewrites.
-- One sequence also prevents delete/reinsert from reusing an earlier revision.
CREATE SEQUENCE archive_mutation_revision_seq AS BIGINT;
ALTER TABLE archive_entries ADD COLUMN mutation_revision BIGINT;
ALTER TABLE archive_versions ADD COLUMN mutation_revision BIGINT;
UPDATE archive_entries SET mutation_revision = nextval('archive_mutation_revision_seq');
UPDATE archive_versions SET mutation_revision = nextval('archive_mutation_revision_seq');
ALTER TABLE archive_entries ALTER COLUMN mutation_revision SET NOT NULL;
ALTER TABLE archive_versions ALTER COLUMN mutation_revision SET NOT NULL;

CREATE FUNCTION assign_archive_mutation_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.mutation_revision := nextval('archive_mutation_revision_seq');
    RETURN NEW;
END;
$$;

CREATE TRIGGER archive_entry_mutation_revision BEFORE INSERT OR UPDATE ON archive_entries
FOR EACH ROW EXECUTE FUNCTION assign_archive_mutation_revision();
CREATE TRIGGER archive_version_mutation_revision BEFORE INSERT OR UPDATE ON archive_versions
FOR EACH ROW EXECUTE FUNCTION assign_archive_mutation_revision();
