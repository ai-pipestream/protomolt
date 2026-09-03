package ai.protomolt.proto.repo.container.archive;

import ai.protomolt.proto.repo.container.ledger.Tx;
import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Row-level operations on the archive tables — every method is one unit of
 * work through {@link Tx} (see Tx's Javadoc for the usage contract: virtual
 * threads, one EntityManager per call, detached results).
 * <p>
 * The mutating methods are deliberately composite: a save is one entry row,
 * one version row, possibly one superseded version's removal, and the stats
 * deltas — all or nothing, because the counters' whole claim is that they
 * are exactly consistent with the rows they describe. Key derivation,
 * object IO, and policy live in the calling service; blob IO never happens
 * inside these transactions.
 */
public final class ArchiveLedger {

    /**
     * The exact counter adjustments one mutation makes.
     *
     * @param entries live-entry delta
     * @param versions retained-version delta
     * @param retainedBytes stored-object byte delta (each object once)
     * @param currentBytes current-version logical byte delta
     * @param renditionObjects rendition name → stored-object count delta
     * @param renditionBytes rendition name → stored-object byte delta
     */
    public record StatsDelta(long entries, long versions, long retainedBytes,
                             long currentBytes,
                             Map<String, Long> renditionObjects,
                             Map<String, Long> renditionBytes) {

        /** A delta that changes nothing. */
        public static StatsDelta none() {
            return new StatsDelta(0, 0, 0, 0, Map.of(), Map.of());
        }
    }

    /** Signals a concurrent writer won: the entry moved under the mutation. */
    public static class VersionConflictException extends RuntimeException {
        /**
         * @param message which entry moved, and from where to where
         */
        public VersionConflictException(String message) {
            super(message);
        }
    }

    private final Tx tx;

    /**
     * @param tx the transactional EntityManager wrapper shared by this service
     */
    public ArchiveLedger(Tx tx) {
        this.tx = tx;
    }

    // ------------------------------------------------------------------
    // Archives
    // ------------------------------------------------------------------

    /**
     * Inserts an archive row. A duplicate (account, name) surfaces as an
     * intact {@code PersistenceException} from the unique constraint.
     *
     * @param record the archive to store
     */
    public void createArchive(ArchiveRecord record) {
        // Block lambda: pins the Consumer overload (an expression lambda is
        // ambiguous against the Function overload).
        tx.inTransaction(em -> {
            em.persist(record);
        });
    }

    /**
     * One archive by account-scoped name.
     *
     * @param accountId owning account
     * @param name the archive name
     * @return the archive, when it exists
     */
    public Optional<ArchiveRecord> findArchive(String accountId, String name) {
        return tx.readOnly(em ->
                Optional.ofNullable(em.find(ArchiveRecord.class,
                        ArchiveIds.archiveId(accountId, name))));
    }

    /**
     * One page of an account's archives, ordered by name.
     *
     * @param accountId owning account
     * @param limit page size
     * @param offset zero-based row offset
     * @return the page
     */
    public List<ArchiveRecord> listArchives(String accountId, int limit, long offset) {
        return tx.readOnly(em -> em.createQuery(
                        "SELECT a FROM ArchiveRecord a WHERE a.accountId = :account"
                                + " ORDER BY a.name", ArchiveRecord.class)
                .setParameter("account", accountId)
                .setFirstResult((int) offset)
                .setMaxResults(limit)
                .getResultList());
    }

    // ------------------------------------------------------------------
    // Entries and versions: reads
    // ------------------------------------------------------------------

    /**
     * One entry by its deterministic id.
     *
     * @param entryUuid the entry id
     * @return the entry, when it exists
     */
    public Optional<ArchiveEntryRecord> findEntry(UUID entryUuid) {
        return tx.readOnly(em ->
                Optional.ofNullable(em.find(ArchiveEntryRecord.class, entryUuid)));
    }

    /**
     * One stored version of one entry.
     *
     * @param entryUuid the entry id
     * @param version the version number
     * @return the version row, when it exists
     */
    public Optional<ArchiveVersionRecord> findVersion(UUID entryUuid, long version) {
        return tx.readOnly(em ->
                Optional.ofNullable(em.find(ArchiveVersionRecord.class,
                        new ArchiveVersionRecord.Key(entryUuid, version))));
    }

    /**
     * Every retained version of one entry, oldest first — the ownership
     * walk deletes and prunes derive from.
     *
     * @param entryUuid the entry id
     * @return all version rows
     */
    public List<ArchiveVersionRecord> allVersions(UUID entryUuid) {
        return tx.readOnly(em -> em.createQuery(
                        "SELECT v FROM ArchiveVersionRecord v WHERE v.entryUuid = :entry"
                                + " ORDER BY v.version", ArchiveVersionRecord.class)
                .setParameter("entry", entryUuid)
                .getResultList());
    }

    /**
     * One page of an entry's versions, newest first.
     *
     * @param entryUuid the entry id
     * @param limit page size
     * @param offset zero-based row offset
     * @return the page
     */
    public List<ArchiveVersionRecord> listVersions(UUID entryUuid, int limit, long offset) {
        return tx.readOnly(em -> em.createQuery(
                        "SELECT v FROM ArchiveVersionRecord v WHERE v.entryUuid = :entry"
                                + " ORDER BY v.version DESC", ArchiveVersionRecord.class)
                .setParameter("entry", entryUuid)
                .setFirstResult((int) offset)
                .setMaxResults(limit)
                .getResultList());
    }

    /**
     * One page of an archive's entries, ordered by entry id, optionally
     * filtered to one classification state.
     *
     * @param accountId owning account
     * @param archive the archive
     * @param classificationState the state to filter to, or null for all
     * @param limit page size
     * @param offset zero-based row offset
     * @return the page
     */
    public List<ArchiveEntryRecord> listEntries(String accountId, String archive,
                                                String classificationState,
                                                int limit, long offset) {
        String where = "e.accountId = :account AND e.archive = :archive"
                + (classificationState != null
                        ? " AND e.classificationState = :state" : "");
        return tx.readOnly(em -> {
            var query = em.createQuery(
                            "SELECT e FROM ArchiveEntryRecord e WHERE " + where
                                    + " ORDER BY e.entryId", ArchiveEntryRecord.class)
                    .setParameter("account", accountId)
                    .setParameter("archive", archive);
            if (classificationState != null) {
                query.setParameter("state", classificationState);
            }
            return query.setFirstResult((int) offset)
                    .setMaxResults(limit)
                    .getResultList();
        });
    }

    /**
     * The exact per-state entry counts of one archive: one indexed
     * aggregate over the state column.
     *
     * @param accountId owning account
     * @param archive the archive
     * @return classification state name → live entry count
     */
    public Map<String, Long> countByClassificationState(String accountId, String archive) {
        return tx.readOnly(em -> {
            Map<String, Long> counts = new java.util.LinkedHashMap<>();
            for (Object[] row : em.createQuery(
                            "SELECT e.classificationState, COUNT(e)"
                                    + " FROM ArchiveEntryRecord e"
                                    + " WHERE e.accountId = :account AND e.archive = :archive"
                                    + " GROUP BY e.classificationState"
                                    + " ORDER BY e.classificationState", Object[].class)
                    .setParameter("account", accountId)
                    .setParameter("archive", archive)
                    .getResultList()) {
                counts.put((String) row[0], (Long) row[1]);
            }
            return counts;
        });
    }

    /**
     * Number of live entries in an archive.
     *
     * @param accountId owning account
     * @param archive the archive
     * @return the exact count
     */
    public long countEntries(String accountId, String archive) {
        return tx.readOnly(em -> em.createQuery(
                        "SELECT COUNT(e) FROM ArchiveEntryRecord e WHERE e.accountId = :account"
                                + " AND e.archive = :archive", Long.class)
                .setParameter("account", accountId)
                .setParameter("archive", archive)
                .getSingleResult());
    }

    /**
     * One archive's counters and per-rendition breakdown.
     *
     * @param accountId owning account
     * @param archive the archive
     * @return the stats row (absent = never written to) and breakdown rows
     */
    public Optional<ArchiveStatsRecord> findStats(String accountId, String archive) {
        return tx.readOnly(em ->
                Optional.ofNullable(em.find(ArchiveStatsRecord.class,
                        new ArchiveStatsRecord.Key(accountId, archive))));
    }

    /**
     * The per-rendition-name breakdown rows of one archive, ordered by name.
     *
     * @param accountId owning account
     * @param archive the archive
     * @return the breakdown rows
     */
    public List<ArchiveRenditionStatsRecord> findRenditionStats(String accountId,
                                                                String archive) {
        return tx.readOnly(em -> em.createQuery(
                        "SELECT r FROM ArchiveRenditionStatsRecord r"
                                + " WHERE r.accountId = :account AND r.archive = :archive"
                                + " ORDER BY r.renditionName",
                        ArchiveRenditionStatsRecord.class)
                .setParameter("account", accountId)
                .setParameter("archive", archive)
                .getResultList());
    }

    // ------------------------------------------------------------------
    // Entries and versions: atomic mutations
    // ------------------------------------------------------------------

    /**
     * Lands one save atomically: the entry row (inserted or updated under a
     * pessimistic lock), the new version row, the superseded version's
     * removal when the archive retains nothing, and the stats deltas.
     *
     * @param entry the entry state to store (currentVersion already bumped)
     * @param baseVersion the version the mutation was computed against
     *        (0 = the entry must not exist yet); a mismatch under the lock
     *        throws {@link VersionConflictException}
     * @param version the new version row to insert
     * @param dropVersion a superseded version to remove in the same
     *        transaction, or 0 for none
     * @param delta the exact counter adjustments
     */
    public void commitSave(ArchiveEntryRecord entry, long baseVersion,
                           ArchiveVersionRecord version, long dropVersion,
                           StatsDelta delta) {
        tx.inTransaction(em -> {
            ArchiveEntryRecord existing = em.find(ArchiveEntryRecord.class,
                    entry.entryUuid, LockModeType.PESSIMISTIC_WRITE);
            if (existing == null) {
                if (baseVersion != 0) {
                    throw new VersionConflictException("entry '" + entry.entryId
                            + "' vanished under the save (expected version "
                            + baseVersion + ")");
                }
                em.persist(entry);
            } else {
                if (existing.currentVersion != baseVersion) {
                    throw new VersionConflictException("entry '" + entry.entryId
                            + "' moved to version " + existing.currentVersion
                            + " under a save computed against " + baseVersion);
                }
                em.merge(entry);
            }
            em.persist(version);
            if (dropVersion != 0) {
                ArchiveVersionRecord dropped = em.find(ArchiveVersionRecord.class,
                        new ArchiveVersionRecord.Key(entry.entryUuid, dropVersion));
                if (dropped != null) {
                    em.remove(dropped);
                }
            }
            applyDelta(em, entry.accountId, entry.archive, delta);
        });
    }

    /**
     * Removes one entry atomically: the entry row (versions cascade) plus
     * the stats deltas.
     *
     * @param entryUuid the entry to remove
     * @param delta the exact counter adjustments
     * @return false when the entry did not exist (nothing changed)
     */
    public boolean commitDeleteEntry(UUID entryUuid, StatsDelta delta) {
        return tx.inTransaction(em -> {
            ArchiveEntryRecord entry = em.find(ArchiveEntryRecord.class, entryUuid,
                    LockModeType.PESSIMISTIC_WRITE);
            if (entry == null) {
                return false;
            }
            em.createQuery("DELETE FROM ArchiveVersionRecord v WHERE v.entryUuid = :entry")
                    .setParameter("entry", entryUuid)
                    .executeUpdate();
            em.remove(entry);
            applyDelta(em, entry.accountId, entry.archive, delta);
            return true;
        });
    }

    /**
     * Removes old versions atomically, with the stats deltas.
     *
     * @param entryUuid the entry
     * @param versions the version numbers to remove
     * @param accountId owning account (for the counters)
     * @param archive the archive (for the counters)
     * @param delta the exact counter adjustments
     */
    public void commitPrune(UUID entryUuid, List<Long> versions,
                            String accountId, String archive, StatsDelta delta) {
        tx.inTransaction(em -> {
            for (long version : versions) {
                ArchiveVersionRecord row = em.find(ArchiveVersionRecord.class,
                        new ArchiveVersionRecord.Key(entryUuid, version));
                if (row != null) {
                    em.remove(row);
                }
            }
            applyDelta(em, accountId, archive, delta);
        });
    }

    /**
     * Rewrites version manifests atomically (the tombstone path), with the
     * stats deltas.
     *
     * @param rows the version rows with rewritten manifests
     * @param accountId owning account (for the counters)
     * @param archive the archive (for the counters)
     * @param delta the exact counter adjustments
     */
    public void commitManifestRewrite(List<ArchiveVersionRecord> rows,
                                      String accountId, String archive,
                                      StatsDelta delta) {
        tx.inTransaction(em -> {
            for (ArchiveVersionRecord row : rows) {
                em.merge(row);
            }
            applyDelta(em, accountId, archive, delta);
        });
    }

    /**
     * Rewrites the entry row alongside a manifest rewrite of its current
     * version (the tombstone path when the current version changes shape).
     *
     * @param entry the entry state to merge
     */
    public void mergeEntry(ArchiveEntryRecord entry) {
        tx.inTransaction(em -> {
            em.merge(entry);
        });
    }

    private static void applyDelta(EntityManager em, String accountId, String archive,
                                   StatsDelta delta) {
        if (delta.entries() == 0 && delta.versions() == 0
                && delta.retainedBytes() == 0 && delta.currentBytes() == 0
                && delta.renditionObjects().isEmpty()) {
            return;
        }
        ArchiveStatsRecord stats = em.find(ArchiveStatsRecord.class,
                new ArchiveStatsRecord.Key(accountId, archive),
                LockModeType.PESSIMISTIC_WRITE);
        if (stats == null) {
            stats = new ArchiveStatsRecord();
            stats.accountId = accountId;
            stats.archive = archive;
            em.persist(stats);
        }
        stats.entries += delta.entries();
        stats.versions += delta.versions();
        stats.retainedBytes += delta.retainedBytes();
        stats.currentBytes += delta.currentBytes();
        for (Map.Entry<String, Long> adjustment : delta.renditionObjects().entrySet()) {
            String name = adjustment.getKey();
            long objects = adjustment.getValue();
            long bytes = delta.renditionBytes().getOrDefault(name, 0L);
            ArchiveRenditionStatsRecord row = em.find(ArchiveRenditionStatsRecord.class,
                    new ArchiveRenditionStatsRecord.Key(accountId, archive, name),
                    LockModeType.PESSIMISTIC_WRITE);
            if (row == null) {
                row = new ArchiveRenditionStatsRecord();
                row.accountId = accountId;
                row.archive = archive;
                row.renditionName = name;
                em.persist(row);
            }
            row.objectCount += objects;
            row.totalBytes += bytes;
        }
    }
}
