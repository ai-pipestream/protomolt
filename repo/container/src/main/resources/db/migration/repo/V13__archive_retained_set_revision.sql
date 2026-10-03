-- Bind an entry's admission revision to its complete retained-version set.
-- Transition tables touch each affected owner once per statement, including
-- direct SQL writes. Updating ownership touches both old and new owners.
CREATE FUNCTION revise_archive_version_owners() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    owners UUID[];
    owner UUID;
BEGIN
    IF TG_OP = 'INSERT' THEN
        SELECT array_agg(entry_uuid ORDER BY entry_uuid) INTO owners
        FROM (SELECT DISTINCT entry_uuid FROM new_versions) affected;
    ELSIF TG_OP = 'DELETE' THEN
        SELECT array_agg(entry_uuid ORDER BY entry_uuid) INTO owners
        FROM (SELECT DISTINCT entry_uuid FROM old_versions) affected;
    ELSE
        SELECT array_agg(entry_uuid ORDER BY entry_uuid) INTO owners
        FROM (SELECT entry_uuid FROM old_versions UNION SELECT entry_uuid FROM new_versions) affected;
    END IF;
    IF owners IS NOT NULL THEN
        FOREACH owner IN ARRAY owners LOOP
            -- V12 assigns a fresh revision even for this otherwise no-op update.
            -- During parent deletion its cascading versions have no owner left.
            UPDATE archive_entries SET mutation_revision = mutation_revision
            WHERE entry_uuid = owner;
        END LOOP;
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER archive_versions_insert_revision AFTER INSERT ON archive_versions
REFERENCING NEW TABLE AS new_versions
FOR EACH STATEMENT EXECUTE FUNCTION revise_archive_version_owners();

CREATE TRIGGER archive_versions_update_revision AFTER UPDATE ON archive_versions
REFERENCING OLD TABLE AS old_versions NEW TABLE AS new_versions
FOR EACH STATEMENT EXECUTE FUNCTION revise_archive_version_owners();

CREATE TRIGGER archive_versions_delete_revision AFTER DELETE ON archive_versions
REFERENCING OLD TABLE AS old_versions
FOR EACH STATEMENT EXECUTE FUNCTION revise_archive_version_owners();
