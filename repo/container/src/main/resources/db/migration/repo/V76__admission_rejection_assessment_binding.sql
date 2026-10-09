-- Add relational identity for protected assessment evidence. Do not retain a
-- foreign key to the expiring owner: immutable receipts outlive evidence expiry.
-- Any old reason=2 row without evidence fails migration instead of being blessed.
ALTER TABLE repository_operation_rejection
 ADD COLUMN assessment_id UUID,
 ADD COLUMN manifest_codec TEXT,
 ADD COLUMN manifest_version INTEGER,
 ADD COLUMN manifest_sha256 BYTEA,
 ADD COLUMN retain_until_epoch_micros BIGINT,
 ADD CONSTRAINT repository_rejection_assessment_binding CHECK (
  (reason=2 AND assessment_id IS NOT NULL AND manifest_codec IS NOT NULL
   AND manifest_codec='document-publication-assessment' AND manifest_version IS NOT NULL AND manifest_version=1
   AND manifest_sha256 IS NOT NULL AND octet_length(manifest_sha256)=32
   AND retain_until_epoch_micros IS NOT NULL AND retain_until_epoch_micros>recorded_at_epoch_micros
   AND retain_until_epoch_micros<=253402300799999999)
  OR (reason<>2 AND assessment_id IS NULL AND manifest_codec IS NULL AND manifest_version IS NULL
   AND manifest_sha256 IS NULL AND retain_until_epoch_micros IS NULL)
 );

CREATE FUNCTION guard_repository_admission_rejection() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE assessment document_assessment_owners%ROWTYPE;
BEGIN
 IF NEW.reason<>2 THEN RETURN NEW; END IF;
 PERFORM require_repository_operation_write_fence(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation);
 SELECT * INTO STRICT assessment FROM document_assessment_owners WHERE assessment_id=NEW.assessment_id FOR SHARE;
 IF ROW(assessment.account_id,assessment.principal,assessment.operation_id,assessment.owner_generation,
        assessment.command_codec,assessment.command_version,assessment.command_sha256,assessment.manifest_sha256)
  IS DISTINCT FROM ROW(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation,
        NEW.command_codec,NEW.command_version,NEW.command_sha256,NEW.manifest_sha256)
  OR NOT assessment.sealed OR assessment.release_xid IS NOT NULL
  OR assessment.retain_until<=clock_timestamp()
  OR floor(extract(epoch FROM assessment.retain_until)*1000000)<>NEW.retain_until_epoch_micros THEN
  RAISE EXCEPTION 'Admission rejection requires its exact sealed retained assessment';
 END IF;
 -- Canonical receipt/manifest correspondence, whole-candidate replay, current
 -- policy/ACL checks and the minimum remaining window are trusted handler duties.
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_operation_admission_rejection_guard BEFORE INSERT ON repository_operation_rejection
 FOR EACH ROW EXECUTE FUNCTION guard_repository_admission_rejection();
