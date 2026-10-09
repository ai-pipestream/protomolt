package ai.protomolt.proto.repo.container.archive;

import ai.protomolt.proto.repo.container.ledger.Tx;
import java.util.Objects;
import java.util.UUID;

/** Bounded release of pins protected by durable local or externally verified quiescence. */
public final class ArchiveReadRecovery {
    private final Tx tx;
    private Candidate cursor;

    public ArchiveReadRecovery(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    /**
     * Each selected pin has its own transaction. A failed release is visible;
     * earlier commits remain valid and remaining pins are discoverable on retry.
     * Returns processed candidates, including an already-released racing pin.
     * A local keyset cursor advances across failed batches and wraps at the end;
     * it is not durable authority. Restart begins a new traversal. Each call
     * performs at most two candidate queries and never fills a short page by
     * wrapping, so one pin cannot be attempted twice in a call.
     */
    public synchronized int recover(int limit) {
        if (limit < 1 || limit > 1000) throw new IllegalArgumentException("Recovery limit must be between 1 and 1000");
        var candidates = candidates(limit, cursor);
        if (candidates.isEmpty() && cursor != null) candidates = candidates(limit, null);
        RuntimeException failures = releaseCandidates(candidates);
        // Scheduling only: failed pins remain durable and are retried after wrap.
        // Update before reporting failures so they cannot monopolize the first page.
        if (!candidates.isEmpty()) cursor = candidates.getLast();
        if (failures != null) throw failures;
        return candidates.size();
    }

    /**
     * Recover at most limit pins for one already-QUIESCED reader. Requires proven
     * quiescence even when no pins exist. Zero proves this reader has no archive
     * pins; other retention families must be checked separately. Each pin release
     * commits independently. A failure is reported after trying the selected page;
     * prior commits remain valid and remaining pins can be rediscovered on retry.
     * Counts selected identities, including pins concurrently released elsewhere.
     * This method does not advance the global recovery cursor.
     */
    public int recoverReaderBatch(UUID reader, int limit) {
        Objects.requireNonNull(reader);
        if (limit < 1 || limit > 1000) throw new IllegalArgumentException("Recovery limit must be between 1 and 1000");
        var candidates = tx.inTransaction(em -> {
            em.createNativeQuery("SELECT require_repository_read_committed()").getSingleResult();
            if (!Boolean.TRUE.equals(em.createNativeQuery("""
                    SELECT EXISTS(SELECT 1 FROM repository_reader_incarnations
                    WHERE incarnation=:reader AND state='QUIESCED')
                    """).setParameter("reader", reader).getSingleResult()))
                throw new IllegalStateException("Archive recovery requires proven reader quiescence");
            java.util.List<Object[]> rows = em.createNativeQuery("""
                    SELECT pin_id,object_id FROM archive_read_pins
                    WHERE reader_incarnation=:reader ORDER BY pin_id LIMIT :limit
                    """).setParameter("reader", reader).setParameter("limit", limit).getResultList();
            return rows.stream().map(row -> new Candidate((UUID) row[0], reader, (UUID) row[1])).toList();
        });
        RuntimeException failures = releaseCandidates(candidates);
        if (failures != null) throw failures;
        return candidates.size();
    }

    private RuntimeException releaseCandidates(java.util.List<Candidate> candidates) {
        RuntimeException failures = null;
        for (var candidate : candidates) {
            try {
                tx.inTransaction(em -> {
                    if (!Boolean.TRUE.equals(em.createNativeQuery("SELECT recover_quiesced_archive_read_pin(:pin,:reader,:object)")
                            .setParameter("pin", candidate.pin()).setParameter("reader", candidate.reader())
                            .setParameter("object", candidate.object()).getSingleResult()))
                        throw new IllegalStateException("Reader pin recovery was not acknowledged");
                });
            } catch (RuntimeException failure) {
                if (failures == null) failures = new IllegalStateException("Reader pin recovery failed; durable retry remains pending", failure);
                else failures.addSuppressed(failure);
            }
        }
        return failures;
    }

    private java.util.List<Candidate> candidates(int limit, Candidate after) {
        return tx.readOnly(em -> {
            var query = em.createNativeQuery("""
                    SELECT p.pin_id,p.reader_incarnation,p.object_id FROM archive_read_pins p
                    JOIN repository_reader_incarnations r ON r.incarnation=p.reader_incarnation
                    WHERE r.state='QUIESCED'
                    """ + (after == null ? "" : " AND (p.object_id,p.pin_id) > (:object,:pin)")
                    + " ORDER BY p.object_id,p.pin_id LIMIT :limit").setParameter("limit", limit);
            if (after != null) query.setParameter("object", after.object()).setParameter("pin", after.pin());
            java.util.List<Object[]> rows = query.getResultList();
            return rows.stream().map(row -> new Candidate((UUID) row[0], (UUID) row[1], (UUID) row[2])).toList();
        });
    }

    private record Candidate(UUID pin, UUID reader, UUID object) {}
}
