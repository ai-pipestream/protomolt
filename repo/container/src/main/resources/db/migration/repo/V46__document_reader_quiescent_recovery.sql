-- Only owning lifecycle code may attest LOCAL_DRAIN (V32). This function
-- consumes that durable proof; elapsed time or FENCED state is insufficient.
CREATE FUNCTION recover_quiesced_document_read_pins(p_reader UUID, p_claims JSONB)
RETURNS BOOLEAN LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 PERFORM require_repository_read_committed();
 -- QUIESCED is permanent, so no incarnation lock spans physical object work.
 IF NOT EXISTS(SELECT 1 FROM repository_reader_incarnations WHERE incarnation=p_reader AND state='QUIESCED') THEN
  RAISE EXCEPTION 'Reader recovery requires proven quiescence'; END IF;
 RETURN release_document_read_pins(p_reader,p_claims);
END;
$$;
