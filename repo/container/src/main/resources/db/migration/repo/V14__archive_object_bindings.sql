-- New registrations only. A legacy manifest's key does not prove its original
-- backend or bucket, so this migration must not infer bindings from drives.
ALTER TABLE managed_backend_profiles ADD CONSTRAINT uq_managed_generation_realm UNIQUE (generation, storage_realm);
CREATE TABLE archive_object_bindings (
    object_id UUID PRIMARY KEY,
    entry_uuid UUID NOT NULL,
    account_id TEXT NOT NULL CHECK (btrim(account_id) <> ''),
    archive TEXT NOT NULL CHECK (btrim(archive) <> ''),
    backend_generation VARCHAR(128) NOT NULL,
    storage_realm VARCHAR(128) NOT NULL,
    bucket TEXT NOT NULL CHECK (btrim(bucket) <> ''),
    object_key TEXT NOT NULL CHECK (btrim(object_key) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (backend_generation, storage_realm) REFERENCES managed_backend_profiles(generation, storage_realm),
    CONSTRAINT uq_archive_physical_object UNIQUE (storage_realm, bucket, object_key)
);
CREATE INDEX archive_object_bindings_entry ON archive_object_bindings(entry_uuid, object_key);

-- Bindings precede entry creation and outlive logical deletion. No cascading
-- entry/drive FK: cleanup must still resolve the original physical coordinates.
CREATE FUNCTION protect_archive_object_binding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Archive object storage bindings are immutable';
END;
$$;
CREATE TRIGGER archive_object_binding_immutable BEFORE UPDATE OR DELETE ON archive_object_bindings
FOR EACH ROW EXECUTE FUNCTION protect_archive_object_binding();
