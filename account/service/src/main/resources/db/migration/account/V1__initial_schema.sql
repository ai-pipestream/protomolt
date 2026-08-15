-- Account store — initial schema.
--
-- One row per account: the tenant root of the document platform. The
-- account_id string IS the primary key — it is the tenancy key carried by
-- every repo-service request and baked into identity hashes and storage
-- prefixes, so it is never aliased behind a surrogate id.

CREATE TABLE accounts (
    -- The tenancy key, minted by the caller (never by the service).
    account_id   TEXT PRIMARY KEY,
    -- Human-facing label; not an identity input.
    display_name TEXT NOT NULL DEFAULT '',
    -- ACTIVE / SUSPENDED / DEACTIVATED. The wire API moves between ACTIVE
    -- and DEACTIVATED; SUSPENDED is the administrative hold the wire does
    -- not set yet.
    status       TEXT NOT NULL DEFAULT 'ACTIVE',
    -- Extensible key-value metadata (the wire's metadata map).
    metadata     JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_accounts_status CHECK (status IN ('ACTIVE', 'SUSPENDED', 'DEACTIVATED'))
);

-- The list path scans in stable (created_at, account_id) order; paging is
-- offset-based over that order.
CREATE INDEX idx_accounts_created ON accounts (created_at, account_id);
