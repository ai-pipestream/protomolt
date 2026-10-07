package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Private sticky staging intent. It does not establish a committed assessment or authorize restaging. */
final class DocumentAssessmentStartJournal {
    record Started(UUID assessment, Instant retainUntil) {}
    /** The inserted flag is tentative until the owning transaction and delivery checks finish. */
    record StartOutcome(Started started, boolean inserted) {}
    private final Tx tx;
    private final DocumentPublicationModesJournal modes;
    DocumentAssessmentStartJournal(Tx tx, PayloadBudget budget) {
        this.tx = Objects.requireNonNull(tx); modes = new DocumentPublicationModesJournal(tx, budget);
    }

    /** Recover coordinates from shared SQL without retaining the proposed ID in process memory. */
    Optional<Started> load(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, RepositoryReadControl control) {
        var claim = owner.executionClaim().orElseThrow(() -> new IllegalArgumentException("Durable staging requires execution claim"));
        if (!claim.commandSha256().equals(command.sha256()) || !owner.key().operationId().equals(command.operationId()))
            throw new IllegalArgumentException("Staging command differs from owner");
        modes.load(caller, claim, owner.generation()-1, control).orElseThrow(() ->
                new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Fixed publication modes are absent"));
        var result = tx.inTransaction(em -> {
            RepositoryExecutionClaimLedger.lockLive(em, claim);
            var rows = em.createNativeQuery("""
                    SELECT assessment_id,retain_until FROM repository_publication_assessment_starts
                    WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g FOR UPDATE
                    """).setParameter("a", owner.key().account()).setParameter("p", owner.key().principal())
                    .setParameter("o", owner.key().operationId()).setParameter("g", owner.generation()-1).getResultList();
            if (rows.isEmpty()) return Optional.<Started>empty();
            Object[] row = (Object[]) rows.getFirst();
            var saved = new Started((UUID) row[0], instant(row[1]));
            requireCreation(em, owner, command, saved.assessment(), saved.retainUntil());
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            RepositoryOperationLedger.requireCommand(em, owner.key(), command);
            control.check(); return Optional.of(saved);
        });
        control.check(); return result;
    }

    Started start(RepositoryCaller caller, RepositoryOperationLedger.Owner owner, DocumentPublicationCommand command,
            UUID proposedId, Duration retention, RepositoryReadControl control) {
        return startRetained(caller, owner, command, proposedId, retention, control, null);
    }

    Started startOwned(DocumentPublicationRegistration.JournalAccess access, RepositoryCaller caller,
            RepositoryOperationLedger.Owner owner, DocumentPublicationCommand command,
            UUID proposedId, Duration retention, RepositoryReadControl control) {
        Objects.requireNonNull(access).requireOwner(caller, owner, command, control);
        return startRetained(caller, owner, command, proposedId, retention, control, access);
    }

    private Started startRetained(RepositoryCaller caller, RepositoryOperationLedger.Owner owner, DocumentPublicationCommand command,
            UUID proposedId, Duration retention, RepositoryReadControl control, DocumentPublicationRegistration.JournalAccess access) {
        command.requireExecutionSupported();
        Objects.requireNonNull(proposedId); Objects.requireNonNull(retention); Objects.requireNonNull(control).check();
        if (retention.isNegative() || retention.isZero() || retention.compareTo(Duration.ofDays(1))>0 || retention.getNano()%1000!=0)
            throw new IllegalArgumentException("Retention requires exact microseconds within one day");
        var claim = owner.executionClaim().orElseThrow(() -> new IllegalArgumentException("Durable staging requires execution claim"));
        if (!claim.commandSha256().equals(command.sha256()) || !owner.key().operationId().equals(command.operationId())
                || !owner.key().account().equals(command.intent().getAccountId()))
            throw new IllegalArgumentException("Staging command differs from owner");
        var fixed = access == null ? modes.load(caller, claim, owner.generation()-1, control)
                : modes.loadOwned(access, caller, claim, owner.generation()-1, control);
        fixed.orElseThrow(() ->
                new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Fixed publication modes are absent"));
        var started = tx.inTransaction(em -> {
            RepositoryExecutionClaimLedger.lockLive(em, claim);
            em.createNativeQuery("""
                    SELECT owner_nonce FROM repository_publication_modes
                    WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g FOR UPDATE
                    """).setParameter("a", owner.key().account()).setParameter("p", owner.key().principal())
                    .setParameter("o", owner.key().operationId()).setParameter("g", owner.generation()-1).getSingleResult();
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            RepositoryOperationLedger.requireCommand(em, owner.key(), command);
            return insertStarted(em, owner, command, proposedId, retention, control);
        });
        control.check(); return started;
    }

    /** Caller holds the historical handle's complete current execution/authority fence. */
    static Started startOrLoadHistorical(EntityManager em, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, Duration retention, RepositoryReadControl control) {
        return startOrLoadHistoricalOwned(em, owner, command, retention, control).started();
    }

    static StartOutcome startOrLoadHistoricalOwned(EntityManager em, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, Duration retention, RepositoryReadControl control) {
        if (owner.generation() != 1 || owner.executionClaim().orElseThrow().epoch() != 1)
            throw new IllegalArgumentException("Historical start requires the initial execution owner");
        return startOrLoadHistoricalBound(em, owner, command, retention, control);
    }

    /** Caller has verified initial registration or exact live successor activation and capture. */
    static StartOutcome startOrLoadHistoricalBound(EntityManager em, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, Duration retention, RepositoryReadControl control) {
        Objects.requireNonNull(retention); control.check();
        if (!owner.key().operationId().equals(command.operationId())
                || !owner.executionClaim().orElseThrow().commandSha256().equals(command.sha256()))
            throw new IllegalArgumentException("Historical start command differs from owner");
        if (retention.isNegative() || retention.isZero() || retention.compareTo(Duration.ofDays(1)) > 0 || retention.getNano() % 1000 != 0)
            throw new IllegalArgumentException("Retention requires exact microseconds within one day");
        var rows = em.createNativeQuery("""
                SELECT assessment_id,retain_until,owner_nonce,command_sha256,retention_micros
                FROM repository_publication_assessment_starts
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g FOR UPDATE
                """).setParameter("a", owner.key().account()).setParameter("p", owner.key().principal())
                .setParameter("o", owner.key().operationId()).setParameter("g", owner.generation()-1).getResultList();
        if (rows.isEmpty()) return insertStartedOutcome(em, owner, command, UUID.randomUUID(), retention, control, true);
        return new StartOutcome(historicalBinding(em, owner, command, retention, (Object[]) rows.getFirst(), control), false);
    }

    private static Started historicalBinding(EntityManager em, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, Duration retention, Object[] row, RepositoryReadControl control) {
        if (!owner.token().equals(row[2]) || !command.sha256().equals(HexFormat.of().formatHex((byte[]) row[3])))
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Historical assessment start binding differs");
        if (((Number) row[4]).longValue() != retention.toNanos() / 1000)
            throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Historical assessment retention differs from its start");
        // Read time after acquiring the row lock, including an INSERT conflict.
        var now = instant(em.createNativeQuery("SELECT clock_timestamp()").getSingleResult());
        if (!instant(row[1]).isAfter(now))
            throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Historical assessment start has expired");
        control.check();
        return new Started((UUID) row[0], instant(row[1]));
    }

    private static Started insertStarted(EntityManager em, RepositoryOperationLedger.Owner owner, DocumentPublicationCommand command,
            UUID proposedId, Duration retention, RepositoryReadControl control) {
        return insertStartedOutcome(em, owner, command, proposedId, retention, control, false).started();
    }

    private static StartOutcome insertStartedOutcome(EntityManager em, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, UUID proposedId, Duration retention, RepositoryReadControl control, boolean historical) {
        int inserted = em.createNativeQuery("""
                    INSERT INTO repository_publication_assessment_starts(account_id,principal,operation_id,predecessor_generation,
                      owner_nonce,command_sha256,assessment_id,retention_micros,retain_until)
                    VALUES (:a,:p,:o,:g,:owner,:digest,:id,:micros,clock_timestamp())
                    ON CONFLICT(account_id,principal,operation_id,predecessor_generation) DO NOTHING
                    """).setParameter("a", owner.key().account()).setParameter("p", owner.key().principal())
                    .setParameter("o", owner.key().operationId()).setParameter("g", owner.generation()-1)
                    .setParameter("owner", owner.token()).setParameter("digest", HexFormat.of().parseHex(command.sha256()))
                    .setParameter("id", proposedId).setParameter("micros", retention.toNanos()/1000).executeUpdate();
        if (inserted != 0 && inserted != 1)
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Unexpected assessment start insert count");
        Object[] row = (Object[]) em.createNativeQuery("""
                SELECT assessment_id,retain_until,owner_nonce,command_sha256,retention_micros
                FROM repository_publication_assessment_starts
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g FOR UPDATE
                """).setParameter("a", owner.key().account()).setParameter("p", owner.key().principal())
                .setParameter("o", owner.key().operationId()).setParameter("g", owner.generation()-1).getSingleResult();
        control.check();
        if (inserted == 1 && !proposedId.equals(row[0]))
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Inserted assessment start identity differs");
        var started = historical ? historicalBinding(em, owner, command, retention, row, control)
                : new Started((UUID) row[0], instant(row[1]));
        return new StartOutcome(started, inserted == 1);
    }

    /** Called before the owner lock; SQL repeats this check for direct CREATE callers. */
    static void requireCreation(EntityManager em, RepositoryOperationLedger.Owner owner, DocumentPublicationCommand command,
            UUID assessment, Instant deadline) {
        if (owner.executionClaim().isEmpty()) return;
        RepositoryExecutionClaimLedger.lockLive(em, owner.executionClaim().orElseThrow());
        em.createNativeQuery("SELECT require_repository_assessment_start(:a,:p,:o,:g,:id,:digest,:deadline)")
                .setParameter("a", owner.key().account()).setParameter("p", owner.key().principal())
                .setParameter("o", owner.key().operationId()).setParameter("g", owner.generation())
                .setParameter("id", assessment).setParameter("digest", HexFormat.of().parseHex(command.sha256()))
                .setParameter("deadline", OffsetDateTime.ofInstant(deadline, ZoneOffset.UTC)).getSingleResult();
    }

    private static Instant instant(Object value) {
        return switch (value) {
            case Instant time -> time;
            case OffsetDateTime time -> time.toInstant();
            case java.sql.Timestamp time -> time.toInstant();
            default -> throw new IllegalStateException("Unsupported database timestamp representation");
        };
    }
}
