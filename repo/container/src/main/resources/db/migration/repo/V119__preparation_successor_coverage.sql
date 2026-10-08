-- Canonical successor coverage is a lineage attestation, not a new root owner.
CREATE TABLE repository_preparation_coverage_lineage (
 account_id varchar(200) NOT NULL,
 principal varchar(200) NOT NULL,
 operation_id uuid NOT NULL,
 predecessor_generation bigint NOT NULL CHECK(predecessor_generation>0),
 preparation_sha256 bytea NOT NULL CHECK(octet_length(preparation_sha256)=32),
 command_sha256 bytea NOT NULL CHECK(octet_length(command_sha256)=32),
 owner_nonce uuid NOT NULL,
 previous_preparation_sha256 bytea NOT NULL CHECK(octet_length(previous_preparation_sha256)=32),
 previous_owner_nonce uuid NOT NULL,
 anchor_generation bigint NOT NULL CHECK(anchor_generation>=0),
 anchor_sha256 bytea NOT NULL CHECK(octet_length(anchor_sha256)=32),
 root_count integer NOT NULL CHECK(root_count BETWEEN 0 AND 10000),
 roots_sha256 bytea NOT NULL CHECK(octet_length(roots_sha256)=32),
 depth integer NOT NULL CHECK(depth BETWEEN 1 AND 64),
 verifier_version integer NOT NULL CHECK(verifier_version=1),
 certified_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(predecessor_generation-anchor_generation=depth),
 PRIMARY KEY(account_id,principal,operation_id,predecessor_generation),
 FOREIGN KEY(account_id,principal,operation_id,predecessor_generation) REFERENCES repository_publication_preparations,
 FOREIGN KEY(account_id,principal,operation_id,anchor_generation)
  REFERENCES repository_preparation_coverage_certificates(account_id,principal,operation_id,predecessor_generation)
);

CREATE FUNCTION guard_preparation_coverage_lineage() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE edge repository_successor_installs%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Preparation coverage lineage is immutable'; END IF;
 PERFORM require_repository_read_committed();
 SELECT * INTO edge FROM repository_successor_installs WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id AND predecessor_generation=NEW.predecessor_generation;
 IF NOT FOUND OR edge.install_xid=pg_current_xact_id() OR edge.preparation_sha256<>NEW.preparation_sha256
  OR edge.command_sha256<>NEW.command_sha256 OR edge.owner_nonce<>NEW.owner_nonce
  OR edge.predecessor_preparation_sha256<>NEW.previous_preparation_sha256 OR edge.predecessor_nonce<>NEW.previous_owner_nonce THEN
  RAISE EXCEPTION 'Coverage lineage differs from committed successor edge';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM repository_publication_preparations p JOIN repository_publication_preparations prior
   ON prior.account_id=p.account_id AND prior.principal=p.principal AND prior.operation_id=p.operation_id
    AND prior.predecessor_generation=p.predecessor_generation-1
  WHERE p.account_id=NEW.account_id AND p.principal=NEW.principal AND p.operation_id=NEW.operation_id
   AND p.predecessor_generation=NEW.predecessor_generation AND p.preparation_sha256=NEW.preparation_sha256
   AND p.command_sha256=NEW.command_sha256 AND p.owner_nonce=NEW.owner_nonce
   AND prior.preparation_sha256=NEW.previous_preparation_sha256 AND prior.owner_nonce=NEW.previous_owner_nonce
   AND prior.command_sha256=NEW.command_sha256)
  OR EXISTS(SELECT 1 FROM repository_preparation_history_sets h WHERE h.account_id=NEW.account_id
   AND h.principal=NEW.principal AND h.operation_id=NEW.operation_id AND h.predecessor_generation=NEW.predecessor_generation)
  OR EXISTS(SELECT 1 FROM repository_preparation_coverage_certificates c WHERE c.account_id=NEW.account_id
   AND c.principal=NEW.principal AND c.operation_id=NEW.operation_id AND c.predecessor_generation=NEW.predecessor_generation) THEN
  RAISE EXCEPTION 'Coverage lineage requires exact headerless successor identity';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM repository_preparation_coverage_certificates c WHERE c.account_id=NEW.account_id
  AND c.principal=NEW.principal AND c.operation_id=NEW.operation_id AND c.predecessor_generation=NEW.anchor_generation
  AND c.preparation_sha256=NEW.anchor_sha256 AND c.command_sha256=NEW.command_sha256
  AND c.root_count=NEW.root_count AND c.roots_sha256=NEW.roots_sha256) THEN
  RAISE EXCEPTION 'Coverage lineage differs from certified root anchor';
 END IF;
 IF NEW.depth=1 THEN
  IF NEW.previous_preparation_sha256<>NEW.anchor_sha256 THEN RAISE EXCEPTION 'Coverage lineage root predecessor differs'; END IF;
 ELSE
  IF NOT EXISTS(SELECT 1 FROM repository_preparation_coverage_lineage prior WHERE prior.account_id=NEW.account_id
   AND prior.principal=NEW.principal AND prior.operation_id=NEW.operation_id
   AND prior.predecessor_generation=NEW.predecessor_generation-1
   AND prior.preparation_sha256=NEW.previous_preparation_sha256 AND prior.owner_nonce=NEW.previous_owner_nonce
   AND prior.command_sha256=NEW.command_sha256 AND prior.anchor_generation=NEW.anchor_generation
   AND prior.anchor_sha256=NEW.anchor_sha256 AND prior.root_count=NEW.root_count AND prior.roots_sha256=NEW.roots_sha256
   AND prior.depth=NEW.depth-1) THEN RAISE EXCEPTION 'Coverage lineage predecessor is not verified'; END IF;
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER preparation_coverage_lineage_guard BEFORE INSERT OR UPDATE OR DELETE
 ON repository_preparation_coverage_lineage FOR EACH ROW EXECUTE FUNCTION guard_preparation_coverage_lineage();

CREATE OR REPLACE FUNCTION guard_preparation_coverage_unresolved() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='UPDATE' THEN RAISE EXCEPTION 'Unresolved preparation identity is immutable'; END IF;
 IF TG_OP='DELETE' THEN
  IF NOT EXISTS(SELECT 1 FROM repository_preparation_coverage_certificates c
   WHERE c.account_id=OLD.account_id AND c.principal=OLD.principal AND c.operation_id=OLD.operation_id
    AND c.predecessor_generation=OLD.predecessor_generation)
   AND NOT EXISTS(SELECT 1 FROM repository_preparation_coverage_lineage l
   WHERE l.account_id=OLD.account_id AND l.principal=OLD.principal AND l.operation_id=OLD.operation_id
    AND l.predecessor_generation=OLD.predecessor_generation) THEN
   RAISE EXCEPTION 'Unresolved preparation requires a coverage certificate or verified lineage';
  END IF;
  RETURN OLD;
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER preparation_coverage_lineage_resolution AFTER INSERT ON repository_preparation_coverage_lineage
 FOR EACH ROW EXECUTE FUNCTION resolve_preparation_coverage();

CREATE FUNCTION reject_lineage_root_ownership() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM repository_preparation_coverage_lineage l WHERE l.account_id=NEW.account_id
  AND l.principal=NEW.principal AND l.operation_id=NEW.operation_id AND l.predecessor_generation=NEW.predecessor_generation) THEN
  RAISE EXCEPTION 'Verified successor lineage cannot become a root owner';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER preparation_lineage_excludes_header BEFORE INSERT ON repository_preparation_history_sets
 FOR EACH ROW EXECUTE FUNCTION reject_lineage_root_ownership();
CREATE TRIGGER preparation_lineage_excludes_certificate BEFORE INSERT ON repository_preparation_coverage_certificates
 FOR EACH ROW EXECUTE FUNCTION reject_lineage_root_ownership();
