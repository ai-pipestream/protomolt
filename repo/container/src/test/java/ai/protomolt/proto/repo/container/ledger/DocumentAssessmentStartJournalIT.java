package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class DocumentAssessmentStartJournalIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final Duration LEASE = Duration.ofMinutes(1), RETENTION = Duration.ofMinutes(10);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    @Test void lostAcknowledgmentRetainsExactDatabaseDeadlineAndAssessmentIdentity() {
        try (var c = context(POSTGRES)) {
            var state = setup(c); var id = UUID.randomUUID();
            var journal = new DocumentAssessmentStartJournal(c.tx(), state.budget());
            var lost = new RepositoryReadControl() {
                @Override public boolean isCancelled() { return false; }
                @Override public long remainingNanos() { return Long.MAX_VALUE; }
                @Override public void check() {
                    long count = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                            "SELECT count(*) FROM repository_publication_assessment_starts").getSingleResult()).longValue());
                    if (count==1) throw new RepositoryException(RepositoryException.Code.CANCELLED, "Lost acknowledgment");
                }
            };
            assertThatThrownBy(() -> journal.start(CALLER, state.owner(), state.value().command(), id, RETENTION, lost))
                    .isInstanceOf(RepositoryException.class);
            var retry = new DocumentAssessmentStartJournal(new Tx(c.emf()), state.budget()).start(CALLER, state.owner(), state.value().command(), id, RETENTION, NONE);
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(0.05)").getSingleResult());
            assertThat(journal.start(CALLER, state.owner(), state.value().command(), id, RETENTION, NONE)).isEqualTo(retry);
            assertThat(retry.assessment()).isEqualTo(id);
            assertThat(new DocumentAssessmentStartJournal(new Tx(c.emf()), state.budget())
                    .load(CALLER, state.owner(), state.value().command(), NONE)).contains(retry);
            assertThat(retry.retainUntil().getNano()%1000).isZero();
            assertThat(state.budget().reservedBytes()).isZero();
        }
    }

    @Test void changedIntentAndMutationAreRefused() {
        try (var c = context(POSTGRES)) {
            var state = setup(c); var id = UUID.randomUUID(); var journal = new DocumentAssessmentStartJournal(c.tx(), state.budget());
            journal.start(CALLER, state.owner(), state.value().command(), id, RETENTION, NONE);
            assertThatThrownBy(() -> journal.start(CALLER, state.owner(), state.value().command(), UUID.randomUUID(), RETENTION, NONE))
                    .hasStackTraceContaining("Assessment start changed");
            assertThatThrownBy(() -> journal.start(CALLER, state.owner(), state.value().command(), id, RETENTION.plusSeconds(1), NONE))
                    .hasStackTraceContaining("Assessment start changed");
            for (String sql : new String[]{"DELETE FROM repository_publication_assessment_starts", "UPDATE repository_publication_assessment_starts SET retain_until=clock_timestamp()"})
                assertThatThrownBy(() -> c.tx().inTransaction(em -> { return em.createNativeQuery(sql).executeUpdate(); }))
                        .hasStackTraceContaining("Assessment start is immutable");
        }
    }

    @Test void directAssessmentInsertRequiresExactDurableStart() {
        try (var c = context(POSTGRES)) {
            var state = setup(c); var id = UUID.randomUUID();
            assertThatThrownBy(() -> insertAssessment(c, state, id, Instant.now().plusSeconds(60)))
                    .hasStackTraceContaining("Assessment requires durable start");
            var start = new DocumentAssessmentStartJournal(c.tx(), state.budget()).start(CALLER, state.owner(), state.value().command(), id, RETENTION, NONE);
            assertThatThrownBy(() -> insertAssessment(c, state, UUID.randomUUID(), start.retainUntil()))
                    .hasStackTraceContaining("Assessment differs from durable start");
            assertThatThrownBy(() -> insertAssessment(c, state, id, start.retainUntil().plusSeconds(1)))
                    .hasStackTraceContaining("Assessment differs from durable start");
            c.tx().inTransaction(em -> {
                DocumentAssessmentStartJournal.requireCreation(em, state.owner(), state.value().command(), id, start.retainUntil()); return null;
            }); // Binding-only acceptance, not a complete staged evidence fixture.
        }
    }

    @Test void wrongOwnerAndUnrepresentableRetentionAreRefused() {
        try (var c = context(POSTGRES)) {
            var state = setup(c); var journal = new DocumentAssessmentStartJournal(c.tx(), state.budget());
            var wrong = new RepositoryOperationLedger.Owner(state.owner().key(), 1, UUID.randomUUID(), state.owner().leaseUntil(), state.owner().executionClaim());
            assertThatThrownBy(() -> journal.start(CALLER, wrong, state.value().command(), UUID.randomUUID(), RETENTION, NONE))
                    .isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
            for (Duration invalid : new Duration[]{Duration.ZERO, Duration.ofDays(2), Duration.ofNanos(1)})
                assertThatThrownBy(() -> journal.start(CALLER, state.owner(), state.value().command(), UUID.randomUUID(), invalid, NONE))
                        .isInstanceOf(IllegalArgumentException.class);
            assertThat(state.budget().reservedBytes()).isZero();
        }
    }

    @Test void assessmentCreationCannotShareTheStartMarkerTransaction() {
        try (var c = context(POSTGRES)) {
            var state = setup(c); var id = UUID.randomUUID();
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                RepositoryOperationLedger.fenceLiveOwner(em, state.owner());
                em.createNativeQuery("""
                        INSERT INTO repository_publication_assessment_starts(account_id,principal,operation_id,predecessor_generation,
                          owner_nonce,command_sha256,assessment_id,retention_micros,retain_until)
                        VALUES (:a,:p,:o,0,:owner,:digest,:id,600000000,clock_timestamp())
                        """).setParameter("a", state.owner().key().account()).setParameter("p", state.owner().key().principal())
                        .setParameter("o", state.owner().key().operationId()).setParameter("owner", state.owner().token())
                        .setParameter("digest", HexFormat.of().parseHex(state.value().command().sha256())).setParameter("id", id).executeUpdate();
                em.createNativeQuery("""
                        SELECT require_repository_assessment_start(account_id,principal,operation_id,predecessor_generation+1,
                          assessment_id,command_sha256,retain_until) FROM repository_publication_assessment_starts
                        """).getResultList(); return null;
            })).hasStackTraceContaining("Assessment start must be committed before creation");
        }
    }

    private static void insertAssessment(Context c, State state, UUID id, Instant deadline) {
        c.tx().inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, state.owner());
            em.createNativeQuery("""
                    INSERT INTO document_assessment_owners(assessment_id,account_id,principal,operation_id,owner_generation,
                      command_codec,command_version,command_sha256,manifest_bytes,manifest_sha256,expected_slots,retain_until,creation_xid)
                    VALUES (:id,:a,:p,:o,1,'document-publication',1,:digest,decode('01','hex'),sha256(decode('01','hex')),1,:deadline,pg_current_xact_id())
                    """).setParameter("id", id).setParameter("a", state.owner().key().account()).setParameter("p", state.owner().key().principal())
                    .setParameter("o", state.owner().key().operationId()).setParameter("digest", HexFormat.of().parseHex(state.value().command().sha256()))
                    .setParameter("deadline", OffsetDateTime.ofInstant(deadline, ZoneOffset.UTC)).executeUpdate(); return null;
        });
    }
    private record State(DocumentPublicationPreparationRecord value, RepositoryOperationLedger.Owner owner, PayloadBudget budget) {}
    private static State setup(Context c) {
        var source = prepare(c, 1, true);
        var command = new DocumentPublicationCommand(source.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString()).build());
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), "principal", command.operationId());
        UUID drive = UUID.fromString(command.intent().getMembers(0).getDriveId());
        var placement = DocumentUploadPlan.Placement.sample(new DriveLedger(c.tx()).findById(drive).orElseThrow(), "native-test",
                new ManagedBackendLedger(c.tx()).find("native-test").orElseThrow());
        var value = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command), Map.of(drive, placement), LEASE, 0);
        var budget = new PayloadBudget(32L*1024*1024);
        var claim = new RepositoryExecutionClaimLedger(c.tx()).acquire(key, command, UUID.randomUUID(), LEASE);
        new DocumentPublicationPreparationJournal(c.tx(), budget).save(CALLER, claim, value, NONE);
        new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, claim, 0, Map.of("member-0", DocumentPublicationCandidate.Mode.TYPED), NONE);
        var owner = new RepositoryOperationLedger(c.tx()).admit(key, command, value.seeds().ownerNonce(), LEASE, claim).owner().orElseThrow();
        return new State(value, owner, budget);
    }
}
