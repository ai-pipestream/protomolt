-- Release replaced staging claims only; retain every artifact and current claim.
CREATE OR REPLACE FUNCTION guard_repository_schema_claim() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE existing_count BIGINT; existing_bytes BIGINT; incoming_bytes INTEGER;
BEGIN
 IF TG_OP='DELETE' THEN
  PERFORM require_repository_operation_recovery_fence(OLD.account_id,OLD.principal,OLD.operation_id);
  IF NOT EXISTS(SELECT 1 FROM repository_operation_owners o
   JOIN repository_schema_artifact_claims c
    ON c.account_id=o.account_id AND c.principal=o.principal AND c.operation_id=o.operation_id
     AND c.owner_generation=o.owner_generation AND c.artifact_sha256=OLD.artifact_sha256
   WHERE o.account_id=OLD.account_id AND o.principal=OLD.principal AND o.operation_id=OLD.operation_id
    AND o.owner_generation>OLD.owner_generation) THEN
   RAISE EXCEPTION 'Schema claim release requires a current-generation replacement claim';
  END IF;
  RETURN OLD;
 END IF;
 IF TG_OP<>'INSERT' THEN
  RAISE EXCEPTION 'Schema claim mutation requires the future retention cleanup protocol';
 END IF;
 PERFORM require_repository_operation_write_fence(NEW.account_id,NEW.principal,NEW.operation_id,NEW.owner_generation);
 SELECT size_bytes INTO STRICT incoming_bytes FROM repository_schema_artifacts
  WHERE account_id=NEW.account_id AND artifact_sha256=NEW.artifact_sha256 FOR KEY SHARE;
 IF EXISTS(SELECT 1 FROM repository_schema_artifact_claims
   WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
    AND owner_generation=NEW.owner_generation AND artifact_sha256=NEW.artifact_sha256) THEN RETURN NEW; END IF;
 SELECT count(*),COALESCE(sum(a.size_bytes),0) INTO existing_count,existing_bytes
 FROM repository_schema_artifact_claims c JOIN repository_schema_artifacts a USING(account_id,artifact_sha256)
 WHERE c.account_id=NEW.account_id AND c.principal=NEW.principal AND c.operation_id=NEW.operation_id
  AND c.owner_generation=NEW.owner_generation;
 IF existing_count>=64 OR existing_bytes+incoming_bytes>67108864 THEN
  RAISE EXCEPTION 'Schema staging exceeds operation generation limits';
 END IF;
 RETURN NEW;
END;
$$;
CREATE INDEX repository_schema_claim_operation_artifact
 ON repository_schema_artifact_claims(account_id,principal,operation_id,artifact_sha256,owner_generation);

CREATE FUNCTION release_repository_replaced_schema_claims(
 p_account TEXT,p_principal TEXT,p_operation UUID,p_limit INTEGER)
RETURNS INTEGER LANGUAGE plpgsql VOLATILE AS $$
DECLARE generation BIGINT; candidates JSONB; removed INTEGER; locked_artifacts INTEGER;
BEGIN
 IF p_limit IS NULL OR p_limit<1 OR p_limit>256 THEN
  RAISE EXCEPTION 'Schema claim release requires a batch limit between 1 and 256';
 END IF;
 generation:=fence_repository_operation_recovery(p_account,p_principal,p_operation);
 -- Owner lock prevents this operation's staging and other cleanup from changing
 -- the replacement proof. Bound the selected claims before artifact locking.
 SELECT jsonb_agg(jsonb_build_object('generation',q.owner_generation,'sha',encode(q.artifact_sha256,'hex')))
 INTO candidates FROM (
  -- There are at most 64 current-generation claims. Probe only matching older
  -- claims, with a bounded index scan for each, instead of scanning unreplaced history.
  SELECT c.owner_generation,c.artifact_sha256
  FROM repository_schema_artifact_claims replacement
  JOIN LATERAL (
   SELECT older.owner_generation,older.artifact_sha256 FROM repository_schema_artifact_claims older
   WHERE older.account_id=p_account AND older.principal=p_principal AND older.operation_id=p_operation
    AND older.artifact_sha256=replacement.artifact_sha256 AND older.owner_generation<generation
   ORDER BY older.owner_generation LIMIT p_limit
  ) c ON TRUE
  WHERE replacement.account_id=p_account AND replacement.principal=p_principal
   AND replacement.operation_id=p_operation AND replacement.owner_generation=generation
  ORDER BY c.artifact_sha256,c.owner_generation LIMIT p_limit
 ) q;
 IF candidates IS NULL THEN RETURN 0; END IF;
 PERFORM 1 FROM repository_schema_artifacts a
 WHERE a.account_id=p_account AND a.artifact_sha256 IN (
  SELECT decode(q.sha,'hex') FROM jsonb_to_recordset(candidates) q(generation BIGINT,sha TEXT))
 ORDER BY a.artifact_sha256 FOR KEY SHARE OF a;
 GET DIAGNOSTICS locked_artifacts=ROW_COUNT;
 IF locked_artifacts<>(SELECT count(DISTINCT q.sha) FROM jsonb_to_recordset(candidates) q(generation BIGINT,sha TEXT)) THEN
  RAISE EXCEPTION 'Schema claim release requires every retained artifact';
 END IF;
 PERFORM 1 FROM repository_schema_artifact_claims c
 JOIN jsonb_to_recordset(candidates) q(generation BIGINT,sha TEXT)
 ON c.owner_generation=q.generation AND c.artifact_sha256=decode(q.sha,'hex')
 WHERE c.account_id=p_account AND c.principal=p_principal AND c.operation_id=p_operation
 ORDER BY c.artifact_sha256,c.owner_generation FOR UPDATE OF c;
 DELETE FROM repository_schema_artifact_claims c
 USING jsonb_to_recordset(candidates) q(generation BIGINT,sha TEXT)
 WHERE c.account_id=p_account AND c.principal=p_principal AND c.operation_id=p_operation
  AND c.owner_generation=q.generation AND c.artifact_sha256=decode(q.sha,'hex');
 GET DIAGNOSTICS removed=ROW_COUNT;
 RETURN removed;
END;
$$;
