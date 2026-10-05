-- Block writers while checking old rows and installing the guards. Never repair
-- counters by clamping: a corrupt aggregate needs an explicit reconciliation.
LOCK TABLE archive_stats, archive_rendition_stats IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
 IF EXISTS(SELECT 1 FROM archive_stats
   WHERE entries<0 OR versions<0 OR retained_bytes<0 OR current_bytes<0)
 OR EXISTS(SELECT 1 FROM archive_rendition_stats WHERE object_count<0 OR total_bytes<0) THEN
  RAISE EXCEPTION 'Archive statistics contain negative counters; reconcile before migration'
   USING ERRCODE='23514';
 END IF;
END;
$$;

-- An ordinary CHECK also sees the proposed INSERT delta before ON CONFLICT.
-- A valid decrement has a negative delta but a nonnegative final stored row.
-- AFTER row triggers enforce the final INSERT/UPDATE result without an extra
-- lookup, a second DML statement, or weakening transactional rollback.
CREATE FUNCTION require_nonnegative_archive_statistics() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.entries<0 OR NEW.versions<0 OR NEW.retained_bytes<0 OR NEW.current_bytes<0 THEN
  RAISE EXCEPTION 'Archive statistics would become negative' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER archive_statistics_nonnegative AFTER INSERT OR UPDATE ON archive_stats
 FOR EACH ROW EXECUTE FUNCTION require_nonnegative_archive_statistics();

CREATE FUNCTION require_nonnegative_archive_rendition_statistics() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.object_count<0 OR NEW.total_bytes<0 THEN
  RAISE EXCEPTION 'Archive rendition statistics would become negative' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER archive_rendition_statistics_nonnegative AFTER INSERT OR UPDATE ON archive_rendition_stats
 FOR EACH ROW EXECUTE FUNCTION require_nonnegative_archive_rendition_statistics();
