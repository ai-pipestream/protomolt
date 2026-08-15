-- Drive provider_config: the DriveProviderConfig oneof (S3DriveConfig /
-- RedisDriveConfig + the options long-tail map) as JSONB on the drive row.
-- Pronounced per-provider knobs ride the row so a drive resolves to a fully
-- configured backing store without an out-of-band config lookup. Nullable:
-- drives without pronounced knobs carry no row value.
ALTER TABLE drives ADD COLUMN provider_config JSONB;
