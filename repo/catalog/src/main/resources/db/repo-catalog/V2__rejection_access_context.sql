-- Retain access context without changing the public protobuf outcome.
-- Existing unbound rejections remain accessible only to process authority.
ALTER TABLE catalog_operation_outcomes
    ADD COLUMN rejection_address bytea,
    ADD COLUMN rejection_create boolean,
    ADD COLUMN rejection_issuer text,
    ADD COLUMN rejection_credential_id uuid,
    ADD CONSTRAINT rejection_access_complete CHECK (
        (rejection_address IS NULL AND rejection_create IS NULL AND rejection_issuer IS NULL AND rejection_credential_id IS NULL)
        OR (rejection_address IS NOT NULL AND rejection_create IS NOT NULL
            AND ((rejection_issuer IS NULL) = (rejection_credential_id IS NULL))));
