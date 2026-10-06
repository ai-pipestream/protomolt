-- Lease expiry is not successor activation, including when a host never drained.
CREATE OR REPLACE FUNCTION require_repository_execution_claim(a text,p text,o uuid)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE required boolean;
BEGIN
 PERFORM require_repository_read_committed();
 SELECT claim_required INTO required FROM repository_execution_scopes WHERE account_id=a AND principal=p AND operation_id=o;
 IF NOT FOUND THEN RAISE EXCEPTION 'Repository execution scope is absent'; END IF;
 IF required AND EXISTS(SELECT 1 FROM repository_coordinator_local_drains
  WHERE account_id=a AND principal=p AND operation_id=o)
 AND NOT repository_successor_execution_open(a,p,o) THEN
  RAISE EXCEPTION 'Coordinator is locally drained; execution is closed';
 END IF;
 IF required AND EXISTS(SELECT 1 FROM repository_coordinator_bindings
  WHERE account_id=a AND principal=p AND operation_id=o)
 AND NOT EXISTS(SELECT 1 FROM repository_execution_claims c JOIN repository_coordinator_bindings b
  USING(account_id,principal,operation_id,claim_epoch,claim_token)
  WHERE c.account_id=a AND c.principal=p AND c.operation_id=o AND c.claim_epoch=1)
 AND NOT repository_successor_execution_open(a,p,o) THEN
  RAISE EXCEPTION 'Coordinator successor requires exact activation';
 END IF;
 IF required AND NOT EXISTS(SELECT 1 FROM repository_execution_claims WHERE account_id=a AND principal=p AND operation_id=o
  AND write_fence_xid=pg_current_xact_id_if_assigned() AND lease_until>clock_timestamp()) THEN
  RAISE EXCEPTION 'Repository mutation requires a live execution claim write fence in this transaction';
 END IF;
 RETURN true;
END;
$$;
