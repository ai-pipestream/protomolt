-- A successor resumes the original fixed mode; it cannot reinterpret the operation.
-- Fail migration explicitly if prior internal callers installed inconsistent modes.
DO $$ BEGIN
 IF EXISTS (
  SELECT 1 FROM repository_successor_installs i
  LEFT JOIN repository_publication_modes m ON
   (m.account_id,m.principal,m.operation_id,m.predecessor_generation,m.owner_nonce)=
   (i.account_id,i.principal,i.operation_id,i.predecessor_generation-1,i.predecessor_nonce)
  WHERE m.owner_nonce IS NULL OR sha256(convert_to(m.modes::text,'UTF8'))<>i.modes_sha256
 ) THEN RAISE EXCEPTION 'Existing successor modes differ from predecessor'; END IF;
END; $$;

CREATE FUNCTION preserve_repository_successor_modes() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS (
  SELECT 1 FROM repository_publication_modes m
  WHERE (m.account_id,m.principal,m.operation_id,m.predecessor_generation,m.owner_nonce)=
   (NEW.account_id,NEW.principal,NEW.operation_id,NEW.predecessor_generation-1,NEW.predecessor_nonce)
   AND sha256(convert_to(m.modes::text,'UTF8'))=NEW.modes_sha256
 ) THEN RAISE EXCEPTION 'Successor modes differ from predecessor'; END IF;
 RETURN NEW;
END; $$;

-- PostgreSQL orders same-kind triggers by name. The existing *_guard trigger
-- acquires claim then owner locks before this immutable predecessor read.
CREATE TRIGGER repository_successor_install_modes_guard
 BEFORE INSERT ON repository_successor_installs
 FOR EACH ROW EXECUTE FUNCTION preserve_repository_successor_modes();
