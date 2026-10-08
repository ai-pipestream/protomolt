-- Java certifies canonical protobuf coverage. The backend SQL role is trusted;
-- direct administrative DML cannot independently prove protobuf interpretation.
-- Existing rows remain unresolved until explicit bounded reconciliation.
CREATE TABLE repository_preparation_coverage_certificates (
 account_id varchar(200) NOT NULL,
 principal varchar(200) NOT NULL,
 operation_id uuid NOT NULL,
 predecessor_generation bigint NOT NULL,
 preparation_sha256 bytea NOT NULL CHECK(octet_length(preparation_sha256)=32),
 command_sha256 bytea NOT NULL CHECK(octet_length(command_sha256)=32),
 root_count integer NOT NULL CHECK(root_count BETWEEN 0 AND 10000),
 roots_sha256 bytea NOT NULL CHECK(octet_length(roots_sha256)=32),
 verifier_version integer NOT NULL CHECK(verifier_version=1),
 proof_kind text NOT NULL CHECK(proof_kind IN ('LIVE_ROOTS','RELEASE_RECEIPT')),
 certified_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(account_id,principal,operation_id,predecessor_generation),
 FOREIGN KEY(account_id,principal,operation_id,predecessor_generation)
  REFERENCES repository_publication_preparations,
 FOREIGN KEY(account_id,principal,operation_id,predecessor_generation)
  REFERENCES repository_preparation_history_sets
);

CREATE TABLE repository_preparation_coverage_unresolved (
 account_id varchar(200) NOT NULL,
 principal varchar(200) NOT NULL,
 operation_id uuid NOT NULL,
 predecessor_generation bigint NOT NULL,
 PRIMARY KEY(account_id,principal,operation_id,predecessor_generation),
 FOREIGN KEY(account_id,principal,operation_id,predecessor_generation)
  REFERENCES repository_publication_preparations
);
-- Account-leading primary key supports pruning's bounded EXISTS lookup.
INSERT INTO repository_preparation_coverage_unresolved
 SELECT account_id,principal,operation_id,predecessor_generation FROM repository_publication_preparations;

CREATE FUNCTION guard_preparation_coverage_certificate() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE h repository_preparation_history_sets%ROWTYPE; actual_count bigint; actual_digest bytea;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Preparation coverage certificate is immutable'; END IF;
 PERFORM require_repository_read_committed();
 IF NOT EXISTS(SELECT 1 FROM repository_publication_preparations p
  WHERE p.account_id=NEW.account_id AND p.principal=NEW.principal AND p.operation_id=NEW.operation_id
   AND p.predecessor_generation=NEW.predecessor_generation
   AND p.preparation_sha256=NEW.preparation_sha256 AND p.command_sha256=NEW.command_sha256) THEN
  RAISE EXCEPTION 'Coverage certificate differs from immutable preparation';
 END IF;
 -- Callers hold claim/preparation fences before this header; never acquire them here.
 SELECT * INTO h FROM repository_preparation_history_sets
  WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
   AND predecessor_generation=NEW.predecessor_generation FOR SHARE;
 IF NOT FOUND OR NOT h.sealed OR h.preparation_sha256<>NEW.preparation_sha256
  OR h.command_sha256<>NEW.command_sha256 OR h.expected_count<>NEW.root_count OR h.roots_sha256<>NEW.roots_sha256 THEN
  RAISE EXCEPTION 'Coverage certificate differs from sealed history header';
 END IF;
 SELECT count(*),sha256(convert_to('protomolt/preparation-history/v1' || E'\n' ||
  coalesce(string_agg(node_id::text || '/' || revision_id::text || E'\n','' ORDER BY node_id,revision_id),''),'UTF8'))
 INTO actual_count,actual_digest FROM repository_preparation_history_roots
 WHERE account_id=NEW.account_id AND principal=NEW.principal AND operation_id=NEW.operation_id
  AND predecessor_generation=NEW.predecessor_generation;
 IF NEW.proof_kind='LIVE_ROOTS' THEN
  IF actual_count<>NEW.root_count OR actual_digest<>NEW.roots_sha256
   OR EXISTS(SELECT 1 FROM repository_preparation_root_releases r WHERE r.account_id=NEW.account_id
    AND r.principal=NEW.principal AND r.operation_id=NEW.operation_id AND r.predecessor_generation=NEW.predecessor_generation) THEN
   RAISE EXCEPTION 'Coverage certificate requires its exact live root set';
  END IF;
 ELSE
  IF actual_count<>0 OR NOT EXISTS(SELECT 1 FROM repository_preparation_root_releases r
   WHERE r.account_id=NEW.account_id AND r.principal=NEW.principal AND r.operation_id=NEW.operation_id
    AND r.predecessor_generation=NEW.predecessor_generation AND r.preparation_sha256=NEW.preparation_sha256
    AND r.command_sha256=NEW.command_sha256 AND r.root_count=NEW.root_count AND r.roots_sha256=NEW.roots_sha256) THEN
   RAISE EXCEPTION 'Coverage certificate requires exact completed root release';
  END IF;
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER preparation_coverage_certificate_guard BEFORE INSERT OR UPDATE OR DELETE
 ON repository_preparation_coverage_certificates FOR EACH ROW EXECUTE FUNCTION guard_preparation_coverage_certificate();

CREATE FUNCTION guard_preparation_coverage_unresolved() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='UPDATE' THEN RAISE EXCEPTION 'Unresolved preparation identity is immutable'; END IF;
 IF TG_OP='DELETE' THEN
  IF NOT EXISTS(SELECT 1 FROM repository_preparation_coverage_certificates c
   WHERE c.account_id=OLD.account_id AND c.principal=OLD.principal AND c.operation_id=OLD.operation_id
    AND c.predecessor_generation=OLD.predecessor_generation) THEN
   RAISE EXCEPTION 'Unresolved preparation requires a coverage certificate';
  END IF;
  RETURN OLD;
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER preparation_coverage_unresolved_guard BEFORE INSERT OR UPDATE OR DELETE
 ON repository_preparation_coverage_unresolved FOR EACH ROW EXECUTE FUNCTION guard_preparation_coverage_unresolved();

CREATE FUNCTION register_unresolved_preparation_coverage() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO repository_preparation_coverage_unresolved VALUES
  (NEW.account_id,NEW.principal,NEW.operation_id,NEW.predecessor_generation);
 RETURN NEW;
END;
$$;
CREATE TRIGGER preparation_coverage_registration AFTER INSERT ON repository_publication_preparations
 FOR EACH ROW EXECUTE FUNCTION register_unresolved_preparation_coverage();

CREATE FUNCTION resolve_preparation_coverage() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 DELETE FROM repository_preparation_coverage_unresolved WHERE account_id=NEW.account_id AND principal=NEW.principal
  AND operation_id=NEW.operation_id AND predecessor_generation=NEW.predecessor_generation;
 RETURN NEW;
END;
$$;
CREATE TRIGGER preparation_coverage_resolution AFTER INSERT ON repository_preparation_coverage_certificates
 FOR EACH ROW EXECUTE FUNCTION resolve_preparation_coverage();

CREATE FUNCTION require_new_preparation_coverage() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 -- Install-only successors reference older retained roots. They are not new
 -- root owners and installation does not grant activation or capture authority.
 -- V93 verifies the same transaction's claim, modes and owner completion.
 IF repository_successor_preparation(NEW.account_id,NEW.principal,NEW.operation_id,
  NEW.predecessor_generation,NEW.owner_nonce,NEW.command_sha256,NEW.preparation_sha256)
  AND EXISTS(SELECT 1 FROM repository_preparation_coverage_unresolved u WHERE u.account_id=NEW.account_id
   AND u.principal=NEW.principal AND u.operation_id=NEW.operation_id AND u.predecessor_generation=NEW.predecessor_generation)
  AND NOT EXISTS(SELECT 1 FROM repository_preparation_history_sets h WHERE h.account_id=NEW.account_id
   AND h.principal=NEW.principal AND h.operation_id=NEW.operation_id AND h.predecessor_generation=NEW.predecessor_generation)
  AND NOT EXISTS(SELECT 1 FROM repository_preparation_coverage_certificates c WHERE c.account_id=NEW.account_id
   AND c.principal=NEW.principal AND c.operation_id=NEW.operation_id AND c.predecessor_generation=NEW.predecessor_generation) THEN
  RETURN NULL;
 END IF;
 IF NOT EXISTS(SELECT 1 FROM repository_preparation_coverage_certificates c
  WHERE c.account_id=NEW.account_id AND c.principal=NEW.principal AND c.operation_id=NEW.operation_id
   AND c.predecessor_generation=NEW.predecessor_generation AND c.preparation_sha256=NEW.preparation_sha256
   AND c.command_sha256=NEW.command_sha256 AND c.proof_kind='LIVE_ROOTS')
  OR EXISTS(SELECT 1 FROM repository_preparation_coverage_unresolved u WHERE u.account_id=NEW.account_id
   AND u.principal=NEW.principal AND u.operation_id=NEW.operation_id AND u.predecessor_generation=NEW.predecessor_generation) THEN
  RAISE EXCEPTION 'New preparation requires atomic canonical coverage certification';
 END IF;
 RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER preparation_coverage_complete AFTER INSERT ON repository_publication_preparations
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION require_new_preparation_coverage();
