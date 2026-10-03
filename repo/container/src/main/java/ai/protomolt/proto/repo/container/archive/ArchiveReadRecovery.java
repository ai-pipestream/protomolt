package ai.protomolt.proto.repo.container.archive;

import ai.protomolt.proto.repo.container.ledger.Tx;
import java.util.Objects;
import java.util.UUID;

/** Bounded release of pins whose owning lifecycle durably attested local quiescence. */
public final class ArchiveReadRecovery {
    private final Tx tx;

    public ArchiveReadRecovery(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    /**
     * Each selected pin has its own transaction. A failed release is visible;
     * earlier commits remain valid and remaining pins are discoverable on retry.
     * Returns processed candidates, including an already-released racing pin.
     */
    public int recover(int limit) {
        if (limit < 1 || limit > 1000) throw new IllegalArgumentException("Recovery limit must be between 1 and 1000");
        var candidates = tx.readOnly(em -> {
            java.util.List<Object[]> rows = em.createNativeQuery("""
                    SELECT p.pin_id,p.reader_incarnation,p.object_id FROM archive_read_pins p
                    JOIN repository_reader_incarnations r ON r.incarnation=p.reader_incarnation
                    WHERE r.state='QUIESCED' ORDER BY p.object_id,p.pin_id LIMIT :limit
                    """).setParameter("limit", limit).getResultList();
            return rows.stream().map(row -> new Candidate((UUID) row[0], (UUID) row[1], (UUID) row[2])).toList();
        });
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
        if (failures != null) throw failures;
        return candidates.size();
    }

    private record Candidate(UUID pin, UUID reader, UUID object) {}
}
