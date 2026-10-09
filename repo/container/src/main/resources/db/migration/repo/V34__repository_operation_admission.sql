-- Internal command/ownership storage only. No existing row is adopted, no upload
-- scope is authorized, and no public commit or terminal outcome API is enabled.
CREATE TABLE repository_operations (
    account_id VARCHAR(200) NOT NULL CHECK (btrim(account_id) <> ''),
    principal VARCHAR(200) NOT NULL CHECK (btrim(principal) <> ''),
    operation_id UUID NOT NULL,
    command_codec VARCHAR(128) NOT NULL CHECK (command_codec ~ '^[a-z][a-z0-9_.-]{0,127}$'),
    command_version INTEGER NOT NULL CHECK (command_version > 0),
    command BYTEA NOT NULL CHECK (octet_length(command) BETWEEN 1 AND 1048576),
    command_sha256 BYTEA NOT NULL CHECK (command_sha256 = sha256(command)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (account_id, principal, operation_id)
);

-- A heartbeat must not fetch, compare, rehash or rewrite the command payload.
CREATE TABLE repository_operation_owners (
    account_id VARCHAR(200) NOT NULL,
    principal VARCHAR(200) NOT NULL,
    operation_id UUID NOT NULL,
    owner_token UUID NOT NULL,
    owner_generation BIGINT NOT NULL CHECK (owner_generation > 0),
    lease_until TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (account_id, principal, operation_id),
    FOREIGN KEY (account_id, principal, operation_id)
        REFERENCES repository_operations(account_id, principal, operation_id)
);

CREATE FUNCTION protect_repository_operation_command() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    -- BEFORE INSERT also runs for ON CONFLICT DO NOTHING: exact retries must
    -- obey the same fresh-statement visibility requirement as first admission.
    IF TG_OP = 'INSERT' THEN
        PERFORM require_repository_read_committed();
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'Repository operation command identity is immutable';
END;
$$;
CREATE TRIGGER repository_operation_command_guard
    BEFORE INSERT OR UPDATE OR DELETE ON repository_operations
    FOR EACH ROW EXECUTE FUNCTION protect_repository_operation_command();

CREATE FUNCTION protect_repository_operation_owner() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM require_repository_read_committed();
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Repository operation ownership cannot be deleted';
    END IF;
    IF NEW.lease_until > clock_timestamp() + interval '1 day' THEN
        RAISE EXCEPTION 'Repository operation lease exceeds one day';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.owner_generation <> 1 OR NEW.lease_until <= clock_timestamp() THEN
            RAISE EXCEPTION 'Repository admission requires first live owner';
        END IF;
    ELSE
        IF ROW(NEW.account_id, NEW.principal, NEW.operation_id)
           IS DISTINCT FROM ROW(OLD.account_id, OLD.principal, OLD.operation_id) THEN
            RAISE EXCEPTION 'Repository operation ownership scope is immutable';
        END IF;
        IF NEW.owner_generation = OLD.owner_generation THEN
            IF NEW.owner_token <> OLD.owner_token OR OLD.lease_until <= clock_timestamp()
               OR NEW.lease_until < OLD.lease_until THEN
                RAISE EXCEPTION 'Repository operation renewal requires its live owner';
            END IF;
        ELSE
            IF OLD.owner_generation = 9223372036854775807
               OR NEW.owner_generation <> OLD.owner_generation + 1
               OR NEW.owner_token = OLD.owner_token OR OLD.lease_until > clock_timestamp()
               OR NEW.lease_until <= clock_timestamp() THEN
                RAISE EXCEPTION 'Repository operation takeover requires an expired owner and next generation';
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER repository_operation_owner_guard
    BEFORE INSERT OR UPDATE OR DELETE ON repository_operation_owners
    FOR EACH ROW EXECUTE FUNCTION protect_repository_operation_owner();

CREATE FUNCTION require_repository_operation_owner() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM repository_operation_owners
                   WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id) THEN
        RAISE EXCEPTION 'Repository operation requires atomic owner admission';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER repository_operation_owner_required AFTER INSERT ON repository_operations
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_repository_operation_owner();
