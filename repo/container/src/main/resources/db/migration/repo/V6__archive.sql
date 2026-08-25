-- The archive: account-scoped named collections of entries, each entry a
-- set of named renditions with attached metadata and per-archive version
-- retention. Payload bytes live only in object storage; these tables hold
-- the manifests (locations, hashes, sizes, states) and the exact usage
-- counters, maintained in the same transaction as the mutations they
-- describe.

-- One account-scoped collection. The archive binds a drive (the storage
-- namespace) and declares how superseded entry states are treated:
-- 'NONE' (one retained state per entry) or 'RETAINED' (every save lands a
-- new immutable version).
CREATE TABLE archives (
    -- Deterministic name-based UUID over (account_id | name).
    archive_id  UUID PRIMARY KEY,
    account_id  TEXT NOT NULL,
    -- Slug, unique within the account; appears in object keys.
    name        TEXT NOT NULL,
    -- The drive whose backing store holds this archive's objects.
    drive_name  TEXT NOT NULL,
    versioning  TEXT NOT NULL,
    description TEXT,
    metadata    JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_archives_identity UNIQUE (account_id, name),
    CONSTRAINT chk_archives_versioning CHECK (versioning IN ('NONE', 'RETAINED'))
);

-- One logical document inside an archive. The entry_id is caller-chosen
-- and free-form; it never reaches an object key (the deterministic
-- entry_uuid does), so exotic identifier bytes cannot diverge a signed
-- request.
CREATE TABLE archive_entries (
    -- Deterministic name-based UUID over (account_id | archive | entry_id).
    entry_uuid         UUID PRIMARY KEY,
    account_id         TEXT NOT NULL,
    archive            TEXT NOT NULL,
    entry_id           TEXT NOT NULL,
    -- The latest version number; the optimistic-concurrency token.
    current_version    BIGINT NOT NULL,
    title              TEXT,
    filename           TEXT,
    content_type       TEXT,
    source_uri         TEXT,
    source_modified_at TIMESTAMPTZ,
    metadata           JSONB,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_archive_entries_identity UNIQUE (account_id, archive, entry_id)
);

-- The archive listing shape (and the stats denominator).
CREATE INDEX idx_archive_entries_archive ON archive_entries (account_id, archive, entry_id);

-- One immutable stored state of one entry. The manifest JSONB is the
-- authoritative list of the version's objects — descriptors, states,
-- sizes, hashes, exact keys — and everything operational (reads, deletes,
-- reconciliation) derives from it, never from prefix listing. It lives
-- here, not in object storage, because it must be transactionally
-- consistent with the version's existence.
CREATE TABLE archive_versions (
    entry_uuid    UUID NOT NULL REFERENCES archive_entries (entry_uuid) ON DELETE CASCADE,
    version       BIGINT NOT NULL,
    -- protobuf-JSON of the VersionManifest.
    manifest      JSONB NOT NULL,
    -- SHA-256 over the ordered rendition hashes: the version's identity as
    -- content (the whole-save dedupe key).
    root_checksum TEXT NOT NULL,
    -- Sum of PRESENT rendition sizes.
    total_bytes   BIGINT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (entry_uuid, version)
);

-- Exact per-archive counters, adjusted in the same transaction as the
-- mutation they describe. retained_bytes counts every stored object once
-- (entry-local content addressing shares objects across versions);
-- current_bytes is the logical size a reader of current versions sees.
CREATE TABLE archive_stats (
    account_id     TEXT NOT NULL,
    archive        TEXT NOT NULL,
    entries        BIGINT NOT NULL DEFAULT 0,
    versions       BIGINT NOT NULL DEFAULT 0,
    retained_bytes BIGINT NOT NULL DEFAULT 0,
    current_bytes  BIGINT NOT NULL DEFAULT 0,

    PRIMARY KEY (account_id, archive)
);

-- Per-rendition-name breakdown of the retained objects.
CREATE TABLE archive_rendition_stats (
    account_id     TEXT NOT NULL,
    archive        TEXT NOT NULL,
    rendition_name TEXT NOT NULL,
    object_count   BIGINT NOT NULL DEFAULT 0,
    total_bytes    BIGINT NOT NULL DEFAULT 0,

    PRIMARY KEY (account_id, archive, rendition_name)
);
