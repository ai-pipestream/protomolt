-- Backend generations survive credential rotation and must never be redirected.
-- No credentials belong in this table. Legacy raw records remain unadopted.
CREATE TABLE managed_backend_profiles (
    generation varchar(128) PRIMARY KEY,
    provider varchar(32) NOT NULL,
    endpoint text NOT NULL,
    region varchar(128) NOT NULL,
    path_style boolean NOT NULL,
    storage_realm varchar(128) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (generation ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    CHECK (storage_realm ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$')
);

CREATE FUNCTION reject_managed_backend_profile_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'Managed backend generations are immutable';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER managed_backend_profile_immutable
BEFORE UPDATE OR DELETE ON managed_backend_profiles
FOR EACH ROW EXECUTE FUNCTION reject_managed_backend_profile_mutation();
