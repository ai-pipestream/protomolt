-- New captures share the execution claim fence with abandonment/terminal writers.
-- Confirmation of an existing immutable batch does not reopen capture admission.
CREATE FUNCTION require_open_repository_capture() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id);
 IF EXISTS(SELECT 1 FROM repository_publication_abandonments
   WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id)
  OR EXISTS(SELECT 1 FROM repository_operation_success
   WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id)
  OR EXISTS(SELECT 1 FROM repository_operation_rejection
   WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id) THEN
  RAISE EXCEPTION 'Publication capture admission is closed';
 END IF;
 RETURN NEW;
END;
$$;

-- AFTER excludes speculative INSERTs discarded by ON CONFLICT DO NOTHING.
-- Existing BEFORE guards establish the same transaction's claim fence first.
CREATE TRIGGER repository_capture_admission AFTER INSERT ON repository_preparation_pin_batches
 FOR EACH ROW EXECUTE FUNCTION require_open_repository_capture();
CREATE TRIGGER repository_capture_admission AFTER INSERT ON repository_preparation_pin_owners
 FOR EACH ROW EXECUTE FUNCTION require_open_repository_capture();
