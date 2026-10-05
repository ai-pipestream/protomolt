-- Registration cleanup must prove ownership even when an INSERT or COMMIT
-- acknowledgment is lost. Existing owners retain their state and read pins.
ALTER TABLE repository_reader_incarnations
    ADD COLUMN registration_nonce UUID NOT NULL DEFAULT gen_random_uuid();

CREATE FUNCTION guard_repository_reader_registration_nonce() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.registration_nonce IS DISTINCT FROM OLD.registration_nonce THEN
        RAISE EXCEPTION 'Reader registration identity is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER repository_reader_registration_identity
    BEFORE UPDATE OF registration_nonce ON repository_reader_incarnations
    FOR EACH ROW EXECUTE FUNCTION guard_repository_reader_registration_nonce();
