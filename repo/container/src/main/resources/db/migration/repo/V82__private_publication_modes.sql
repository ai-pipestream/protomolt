-- Private recovery prerequisite. Session admission does not consume this journal yet.
CREATE TABLE repository_publication_modes (
 account_id varchar(200) NOT NULL,
 principal varchar(200) NOT NULL,
 operation_id uuid NOT NULL,
 predecessor_generation bigint NOT NULL,
 owner_nonce uuid NOT NULL,
 modes jsonb NOT NULL CHECK(jsonb_typeof(modes)='object' AND octet_length(modes::text) BETWEEN 2 AND 1048576),
 PRIMARY KEY(account_id,principal,operation_id,predecessor_generation),
 FOREIGN KEY(account_id,principal,operation_id,predecessor_generation)
  REFERENCES repository_publication_preparations(account_id,principal,operation_id,predecessor_generation)
);
CREATE FUNCTION protect_repository_publication_modes() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE preparation repository_publication_preparations%ROWTYPE; existing repository_publication_modes%ROWTYPE;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'Publication modes are immutable'; END IF;
 PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id);
 SELECT * INTO preparation FROM repository_publication_preparations WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id AND predecessor_generation=NEW.predecessor_generation FOR UPDATE;
 IF NOT FOUND OR preparation.owner_nonce<>NEW.owner_nonce THEN RAISE EXCEPTION 'Publication modes require exact preparation'; END IF;
 IF jsonb_typeof(NEW.modes)<>'object' THEN RAISE EXCEPTION 'Publication modes require object'; END IF;
 IF (SELECT count(*) FROM jsonb_each(NEW.modes)) NOT BETWEEN 1 AND 10000
  OR EXISTS(SELECT 1 FROM jsonb_each(NEW.modes) entry WHERE length(entry.key)=0
    OR length(entry.key)>200 OR entry.value NOT IN ('"TYPED"'::jsonb,'"OPAQUE"'::jsonb)) THEN
  RAISE EXCEPTION 'Publication modes contain invalid entries';
 END IF;
 SELECT * INTO existing FROM repository_publication_modes WHERE account_id=NEW.account_id
  AND principal=NEW.principal AND operation_id=NEW.operation_id AND predecessor_generation=NEW.predecessor_generation;
 IF FOUND THEN
  IF existing.owner_nonce<>NEW.owner_nonce OR existing.modes<>NEW.modes THEN RAISE EXCEPTION 'Publication modes changed'; END IF;
 ELSE
  -- Choices must precede the owner generation that can execute them.
  IF EXISTS(SELECT 1 FROM repository_operation_owners WHERE account_id=NEW.account_id AND principal=NEW.principal
    AND operation_id=NEW.operation_id AND owner_generation>NEW.predecessor_generation) THEN
   RAISE EXCEPTION 'Publication modes must precede owner admission';
  END IF;
 END IF;
 PERFORM require_repository_execution_claim(NEW.account_id,NEW.principal,NEW.operation_id);
 RETURN NEW;
END;
$$;
CREATE TRIGGER repository_publication_modes_guard BEFORE INSERT OR UPDATE OR DELETE ON repository_publication_modes
 FOR EACH ROW EXECUTE FUNCTION protect_repository_publication_modes();
