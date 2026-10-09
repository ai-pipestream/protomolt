-- Keep claim locking, after-wait expiry checks and consumed-proof stamping in
-- one database call. No lease extension and no lock survives this transaction.
CREATE FUNCTION fence_repository_execution_claim(
 scoped_account text, scoped_principal text, scoped_operation uuid,
 expected_digest bytea, expected_epoch bigint, expected_token uuid)
RETURNS TABLE(lease_until timestamptz) LANGUAGE plpgsql AS $$
DECLARE current_claim repository_execution_claims%ROWTYPE;
BEGIN
 PERFORM require_repository_read_committed();
 SELECT * INTO current_claim FROM repository_execution_claims c
 WHERE c.account_id=scoped_account AND c.principal=scoped_principal AND c.operation_id=scoped_operation
 FOR UPDATE;
 IF NOT FOUND THEN RETURN; END IF;
 -- Evaluate database time only after the row lock has been obtained.
 IF current_claim.command_sha256 IS DISTINCT FROM expected_digest
  OR current_claim.claim_epoch IS DISTINCT FROM expected_epoch
  OR current_claim.claim_token IS DISTINCT FROM expected_token
  OR current_claim.lease_until <= clock_timestamp() THEN RETURN; END IF;
 RETURN QUERY UPDATE repository_execution_claims c
 SET fence_epoch=expected_epoch,fence_token=expected_token
 WHERE c.account_id=scoped_account AND c.principal=scoped_principal AND c.operation_id=scoped_operation
 RETURNING c.lease_until;
 -- V78/V79 recheck liveness and consume the proof before stamping the actual XID.
END;
$$;
