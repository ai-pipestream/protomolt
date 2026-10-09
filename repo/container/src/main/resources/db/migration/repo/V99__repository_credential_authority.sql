-- Repository-local authority state. No token material and no implicit grants for existing callers.
CREATE TABLE repository_credential_authorities (
 issuer varchar(128) NOT NULL CHECK(length(btrim(issuer))>0 AND issuer !~ '[[:cntrl:]]'),
 credential_id uuid NOT NULL,
 generation bigint NOT NULL CHECK(generation>0),
 principal varchar(200) NOT NULL CHECK(length(btrim(principal))>0 AND principal !~ '[[:cntrl:]]'),
 revoked boolean NOT NULL DEFAULT false,
 PRIMARY KEY(issuer,credential_id)
);
CREATE FUNCTION protect_repository_credential_authority() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 PERFORM require_repository_read_committed();
 IF TG_OP='INSERT' THEN RETURN NEW; END IF;
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Credential authority tombstones cannot be deleted'; END IF;
 IF (NEW.issuer,NEW.credential_id,NEW.principal) IS DISTINCT FROM (OLD.issuer,OLD.credential_id,OLD.principal) THEN
  RAISE EXCEPTION 'Credential authority identity is immutable';
 END IF;
 IF NEW.generation=OLD.generation THEN
  IF OLD.revoked AND NOT NEW.revoked THEN RAISE EXCEPTION 'Revoked credential generation cannot be revived'; END IF;
 ELSIF NEW.generation<=OLD.generation OR NEW.generation-OLD.generation<>1 OR NEW.revoked THEN
  RAISE EXCEPTION 'Credential rotation requires the next active generation';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_credential_authority_guard BEFORE INSERT OR UPDATE OR DELETE ON repository_credential_authorities
 FOR EACH ROW EXECUTE FUNCTION protect_repository_credential_authority();

-- Keep the isolation guard and shared lookup in one client round trip, even for an absent key.
CREATE FUNCTION lock_repository_credential_authority(a text,k uuid)
RETURNS TABLE(generation bigint,principal varchar,revoked boolean) LANGUAGE plpgsql AS $$
BEGIN
 PERFORM require_repository_read_committed();
 RETURN QUERY SELECT c.generation,c.principal,c.revoked FROM repository_credential_authorities c
 WHERE c.issuer=a AND c.credential_id=k FOR SHARE OF c;
END;
$$;
