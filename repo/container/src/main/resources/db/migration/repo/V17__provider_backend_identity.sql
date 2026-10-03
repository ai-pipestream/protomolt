-- Preserve legacy profiles verbatim. New generations use provider-owned identity
-- schemas; the immutable generation/realm anchor and its foreign keys stay intact.
ALTER TABLE managed_backend_profiles
    ALTER COLUMN endpoint DROP NOT NULL,
    ALTER COLUMN region DROP NOT NULL,
    ALTER COLUMN path_style DROP NOT NULL,
    ADD COLUMN identity_schema varchar(64),
    ADD COLUMN identity_json jsonb;

ALTER TABLE managed_backend_profiles ADD CONSTRAINT managed_backend_identity_shape CHECK (
    (identity_schema IS NULL AND identity_json IS NULL AND provider='s3'
        AND endpoint IS NOT NULL AND region IS NOT NULL AND path_style IS NOT NULL)
    OR
    (identity_schema IS NOT NULL AND identity_json IS NOT NULL
        AND jsonb_typeof(identity_json)='object'
        AND endpoint IS NULL AND region IS NULL AND path_style IS NULL)
);
