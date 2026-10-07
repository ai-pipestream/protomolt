package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/** Private capture lifecycle and durable evidence. Does not release historical preparation roots. */
final class DocumentPreparationCaptureDrain {
    private DocumentPreparationCaptureDrain() {}
    record Identity(RepositoryCoordinatorDrain.Identity owner, long generation, String pinsSha256) {
        Identity {
            Objects.requireNonNull(owner); Objects.requireNonNull(pinsSha256);
            if (generation < 0 || !pinsSha256.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("Invalid capture identity");
        }
    }
    record Receipt(String kind, Instant recordedAt) {}

    /**
     * Tentative until registration commits. Even a lost registration acknowledgement
     * retains this capability; recording drain still requires the durable exact owner.
     */
    static Capture register(Tx tx, EntityManager em, DocumentPublicationPreparationRecord record,
            DocumentPreparationSourcePins.Prepared pins, RepositoryExecutionClaimLedger.Claim claim,
            UUID coordinator, DocumentHistoricalAssessmentSources sources,
            DocumentHistoricalAssessmentSources.Work work, RepositoryReadControl control) {
        var histories = work.histories(sources);
        DocumentPreparationSourcePins.insert(em, record, pins, claim, coordinator, control::check);
        return new Capture(tx, new Identity(new RepositoryCoordinatorDrain.Identity(record.key(), record.command().sha256(),
                claim.epoch(), claim.token(), coordinator), record.predecessorGeneration(), HexFormat.of().formatHex(pins.digest())),
                sources, histories);
    }

    static final class Capture {
        private final Tx tx;
        private final Identity identity;
        private final DocumentHistoricalAssessmentSources sources;
        private final List<DocumentReadLedger.PinnedHistory> histories;
        private Capture(Tx tx, Identity identity, DocumentHistoricalAssessmentSources sources,
                List<DocumentReadLedger.PinnedHistory> histories) {
            this.tx = tx; this.identity = identity; this.sources = sources; this.histories = List.copyOf(histories);
        }
        Identity identity() { return identity; }

        /** Timeout bounds local waiting, not JDBC. Partial release remains retryable; no reader-wide fence. */
        Optional<Receipt> complete(RepositoryCaller caller, Duration timeout, RepositoryReadControl control) throws InterruptedException {
            require(caller, identity, control);
            if (!releaseLocal(timeout, control)) return Optional.empty();
            return Optional.of(record(tx, caller, identity, "LOCAL", control));
        }

        /** Owning local resources only; does not assert durable capture registration or remote quiescence. */
        boolean releaseLocal(Duration timeout, RepositoryReadControl control) throws InterruptedException {
            Objects.requireNonNull(control).check();
            Objects.requireNonNull(timeout);
            if (timeout.isNegative()) throw new IllegalArgumentException("Negative capture drain wait");
            long budget = timeout.toNanos(), start = System.nanoTime();
            sources.close();
            histories.forEach(DocumentReadLedger.PinnedHistory::close);
            if (!await(sources::awaitDrained, budget, start, control)) return false;
            for (var history : histories) {
                control.check();
                if (!await(history::awaitDrained, budget, start, control)) return false;
            }
            for (var history : histories) { control.check(); history.release(); }
            control.check();
            return histories.stream().allMatch(DocumentReadLedger.PinnedHistory::isReleased);
        }
    }

    /** Existing V46/V47 recovery must have removed the exact pins first. Does not manufacture quiescence. */
    static Receipt recover(Tx tx, RepositoryCaller caller, Identity identity, RepositoryReadControl control) {
        return record(tx, caller, identity, "QUIESCED", control);
    }

    private static Receipt record(Tx tx, RepositoryCaller caller, Identity identity, String kind, RepositoryReadControl control) {
        require(caller, identity, control);
        var existing = confirm(tx, caller, identity, control);
        if (existing.isPresent()) return existing.orElseThrow(); // Preserve the original evidence kind.
        try {
            var result = tx.inTransaction(em -> {
                control.check();
                scope(em.createNativeQuery("""
                        INSERT INTO repository_preparation_capture_drains(account_id,principal,operation_id,predecessor_generation,
                         pins_sha256,claim_epoch,claim_token,incarnation,command_sha256,drain_kind)
                        VALUES(:a,:p,:o,:g,:digest,:epoch,:token,:incarnation,:command,:kind)
                        ON CONFLICT(account_id,principal,operation_id,predecessor_generation,pins_sha256) DO NOTHING
                        """), identity).setParameter("epoch", identity.owner().epoch()).setParameter("token", identity.owner().token())
                        .setParameter("incarnation", identity.owner().incarnation())
                        .setParameter("command", HexFormat.of().parseHex(identity.owner().commandSha256()))
                        .setParameter("kind", kind).executeUpdate();
                var receipt = read(em, identity).orElseThrow(() -> new IllegalStateException("Capture drain absent after insert"));
                control.check(); return receipt;
            });
            control.check(); return result;
        } catch (RuntimeException failure) {
            try {
                var committed = confirm(tx, caller, identity, control);
                if (committed.isPresent()) return committed.orElseThrow();
            } catch (RuntimeException confirmation) { if (failure != confirmation) failure.addSuppressed(confirmation); }
            throw failure;
        }
    }

    static Optional<Receipt> confirm(Tx tx, RepositoryCaller caller, Identity identity, RepositoryReadControl control) {
        require(caller, identity, control);
        // Bounded host views apply transaction-local timeouts, including on reads.
        var result = tx.inTransaction(em -> { return read(em, identity); });
        control.check(); return result;
    }

    private static Optional<Receipt> read(EntityManager em, Identity identity) {
        var rows = scope(em.createNativeQuery("""
                SELECT claim_epoch,claim_token,incarnation,command_sha256,drain_kind,recorded_at
                FROM repository_preparation_capture_drains
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g AND pins_sha256=:digest
                """), identity).getResultList();
        if (rows.isEmpty()) return Optional.empty();
        Object[] row = (Object[]) rows.getFirst();
        if (((Number) row[0]).longValue() != identity.owner().epoch() || !identity.owner().token().equals(row[1])
                || !identity.owner().incarnation().equals(row[2])
                || !identity.owner().commandSha256().equals(HexFormat.of().formatHex((byte[]) row[3])))
            throw new IllegalArgumentException("Capture drain differs from original binding");
        Instant time = switch (row[5]) {
            case Instant value -> value;
            case java.time.OffsetDateTime value -> value.toInstant();
            case java.sql.Timestamp value -> value.toInstant();
            default -> throw new IllegalStateException("Unsupported capture drain timestamp");
        };
        return Optional.of(new Receipt((String) row[4], time));
    }

    private static Query scope(Query query, Identity identity) {
        return query.setParameter("a", identity.owner().key().account()).setParameter("p", identity.owner().key().principal())
                .setParameter("o", identity.owner().key().operationId()).setParameter("g", identity.generation())
                .setParameter("digest", HexFormat.of().parseHex(identity.pinsSha256()));
    }
    @FunctionalInterface private interface DrainWait { boolean await(Duration timeout) throws InterruptedException; }
    private static boolean await(DrainWait wait, long budget, long start, RepositoryReadControl control) throws InterruptedException {
        while (true) {
            control.check();
            long remaining = Math.max(0, budget - (System.nanoTime() - start));
            // Observe cancellation while a worker is held; deadline and caller budget also bound each wait.
            long slice = Math.min(remaining, Math.min(100_000_000L, Math.max(0, control.remainingNanos())));
            boolean drained = wait.await(Duration.ofNanos(slice));
            control.check();
            if (drained) return true;
            if (System.nanoTime() - start >= budget) return false;
        }
    }
    private static void require(RepositoryCaller caller, Identity identity, RepositoryReadControl control) {
        Objects.requireNonNull(control).check(); Objects.requireNonNull(identity);
        DocumentAdmissionAuthorization.requireCaller(caller, identity.owner().key(), identity.owner().key().account());
        if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Capture drain requires private process authority");
    }
}
