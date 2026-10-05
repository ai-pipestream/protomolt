package ai.protomolt.proto.repo.container.archive;

import jakarta.persistence.EntityManager;
import java.util.TreeSet;

/** Atomic additive counters in the caller's publication/deletion transaction. */
final class ArchiveStatistics {
    private ArchiveStatistics() {}

    // Callers must not manage counter entities in this EntityManager: native DML
    // does not refresh JPA's first-level cache. Public reads use a fresh Tx scope.
    static void apply(EntityManager em, String account, String archive, ArchiveLedger.StatsDelta delta) {
        if (delta.entries() == 0 && delta.versions() == 0 && delta.retainedBytes() == 0
                && delta.currentBytes() == 0 && delta.renditionObjects().isEmpty() && delta.renditionBytes().isEmpty()) return;

        // Complete pending domain changes and their triggers before acquiring the
        // shared counter lock. Never rely on native-query AUTO-flush ordering.
        em.flush();
        int updated = em.createNativeQuery("""
                INSERT INTO archive_stats(account_id,archive,entries,versions,retained_bytes,current_bytes)
                VALUES (:account,:archive,:entries,:versions,:retained,:current)
                ON CONFLICT (account_id,archive) DO UPDATE SET
                    entries=archive_stats.entries+EXCLUDED.entries,
                    versions=archive_stats.versions+EXCLUDED.versions,
                    retained_bytes=archive_stats.retained_bytes+EXCLUDED.retained_bytes,
                    current_bytes=archive_stats.current_bytes+EXCLUDED.current_bytes
                """).setParameter("account", account).setParameter("archive", archive)
                .setParameter("entries", delta.entries()).setParameter("versions", delta.versions())
                .setParameter("retained", delta.retainedBytes()).setParameter("current", delta.currentBytes()).executeUpdate();
        if (updated != 1) throw new IllegalStateException("Archive statistics update did not affect exactly one row");

        var names = new TreeSet<>(delta.renditionObjects().keySet());
        names.addAll(delta.renditionBytes().keySet());
        for (String name : names) {
            updated = em.createNativeQuery("""
                    INSERT INTO archive_rendition_stats(account_id,archive,rendition_name,object_count,total_bytes)
                    VALUES (:account,:archive,:name,:objects,:bytes)
                    ON CONFLICT (account_id,archive,rendition_name) DO UPDATE SET
                        object_count=archive_rendition_stats.object_count+EXCLUDED.object_count,
                        total_bytes=archive_rendition_stats.total_bytes+EXCLUDED.total_bytes
                    """).setParameter("account", account).setParameter("archive", archive).setParameter("name", name)
                    .setParameter("objects", delta.renditionObjects().getOrDefault(name, 0L))
                    .setParameter("bytes", delta.renditionBytes().getOrDefault(name, 0L)).executeUpdate();
            if (updated != 1) throw new IllegalStateException("Rendition statistics update did not affect exactly one row");
        }
    }
}
