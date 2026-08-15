-- Intake key store — initial schema.
--
-- One row per minted API key. The key material itself is NEVER stored: the
-- primary key is the lowercase hex SHA-256 of the credential, so a dump of
-- this table authenticates nobody. Revocation is a timestamp, not a delete —
-- the row stays as an audit trace of when the key lived and died.
--
-- Rotation-with-grace is not a schema feature: it is simply two live rows
-- resolving to the same account (mint the new key, revoke the old one when
-- the grace window ends).

CREATE TABLE intake_api_key (
    -- Lowercase hex SHA-256 of the presented credential.
    key_hash          TEXT PRIMARY KEY,
    -- The account (tenant root) every ingest under this key is owned by.
    account_id        TEXT NOT NULL,
    -- Narrowing sets; empty means unrestricted on that axis WITHIN the
    -- account (the account boundary itself is never optional).
    datasource_ids    TEXT[] NOT NULL DEFAULT '{}',
    drives            TEXT[] NOT NULL DEFAULT '{}',
    mime_types        TEXT[] NOT NULL DEFAULT '{}',
    -- Per-key payload cap in bytes; zero means no per-key cap (the
    -- service's own cap still applies).
    max_payload_bytes BIGINT NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- NULL = live; set = the key is refused from that instant.
    revoked_at        TIMESTAMPTZ
);

-- Key management tooling lists an account's keys (live and revoked).
CREATE INDEX idx_intake_api_key_account ON intake_api_key (account_id);
