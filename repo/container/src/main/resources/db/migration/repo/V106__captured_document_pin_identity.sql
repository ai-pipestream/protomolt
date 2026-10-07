-- V104's capture identities must remain retired after their native pins are released.
-- The existing pin_id index keeps this check local to the inserted identity.
CREATE FUNCTION refuse_captured_document_pin_reuse() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 PERFORM require_repository_read_committed();
 IF EXISTS(SELECT 1 FROM repository_preparation_source_pins WHERE pin_id=NEW.pin_id) THEN
  RAISE EXCEPTION 'Captured document pin identity cannot be reused';
 END IF;
 RETURN NEW;
END;
$$;

-- Check AFTER insertion: an INSERT can wait at the unique index for a concurrent
-- deletion after its BEFORE triggers ran. The volatile function reads committed
-- capture evidence after that wait. The exception rolls back the new pin/mirror.
CREATE TRIGGER a_document_captured_pin_reuse AFTER INSERT ON document_read_pins
 FOR EACH ROW EXECUTE FUNCTION refuse_captured_document_pin_reuse();
