-- Authorization caller store — initial schema.
--
-- One row per minted credential. The credential itself is NEVER stored: the
-- primary key is the lowercase hex SHA-256 of the credential, so a dump of
-- this table authenticates nobody. Revocation is a timestamp, not a delete —
-- the row stays as an audit trace of when the credential lived and died.
--
-- Rotation-with-grace is not a schema feature: it is simply two live rows
-- resolving to the same principal (mint the new credential, revoke the old
-- one when the grace window ends) — the same property the access-policy
-- document expresses with several digests on one principal.

CREATE TABLE authz_principal (
    -- Lowercase hex SHA-256 of the presented credential.
    credential_sha256 TEXT PRIMARY KEY,
    -- The principal this credential resolves to; named in refusals.
    principal_name    TEXT NOT NULL,
    -- The scopes the principal holds, each from the closed vocabulary.
    scopes            TEXT[] NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- NULL = live; set = the credential is refused from that instant.
    revoked_at        TIMESTAMPTZ
);

-- Credential management tooling lists a principal's credentials (live and
-- revoked).
CREATE INDEX idx_authz_principal_name ON authz_principal (principal_name);
