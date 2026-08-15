-- The platform keeps two databases in the one PostgreSQL instance: the
-- repository ledger (POSTGRES_DB=documents, created by the image) and the
-- chain-jobs store, created here. Separate databases keep the two Flyway
-- histories apart.
CREATE DATABASE jobs OWNER protomolt;
