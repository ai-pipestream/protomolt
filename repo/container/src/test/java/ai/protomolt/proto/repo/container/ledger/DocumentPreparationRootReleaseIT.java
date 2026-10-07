package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static org.assertj.core.api.Assertions.*;

/** Raw SQL atomicity qualification; no public release API or historical execution. */
@Testcontainers
class DocumentPreparationRootReleaseIT {
    @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER=new RepositoryCaller("principal",true);
    private static final RepositoryReadControl NONE=RepositoryReadControl.NONE;
    private static final Duration LEASE=Duration.ofMinutes(5);

    @Test void pendingAndUndrainedPreparationsCannotRelease() throws Exception {
        try (var c=context(POSTGRES); var rig=historicalInitial(c,LEASE)) {
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { insert(em,rig,""); }))
                    .hasStackTraceContaining("one exact terminal alternative");
            abandon(c,rig);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { insert(em,rig,""); }))
                    .hasStackTraceContaining("has not drained exactly");
            assertLive(c,rig);
        }
    }

    @Test void receiptAndAllRootDeletionMustCommitTogether() throws Exception {
        try (var c=context(POSTGRES); var rig=historicalInitial(c,LEASE);
             var memory=rig.budget().reserve(32L*1024*1024)) {
            abandon(c,rig); drain(c,rig);
            var terminal=new DocumentPreparationTerminalEvidence(rig.record());
            var coverage=DocumentPreparationCaptureCoverage.prepare(rig.record(),NONE);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { delete(em,rig); }))
                    .hasStackTraceContaining("require this transaction release");
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { insert(em,rig,""); }))
                    .hasStackTraceContaining("remove every target root atomically");
            assertLive(c,rig);
            assertThatThrownBy(() -> c.tx().inTransaction((java.util.function.Consumer<EntityManager>)em -> {
                insert(em,rig,""); delete(em,rig); throw new Abort();
            })).isInstanceOf(Abort.class);
            assertLive(c,rig);
            c.tx().inTransaction(em -> {
                terminal.lockAndRequire(em,CALLER,NONE);
                assertThat(coverage.lockAndRequireDrained(em,NONE)).isEqualTo(1);
                insert(em,rig,""); assertThat(delete(em,rig)).isEqualTo(1);
            });
            assertThat(count(c,rig,"repository_preparation_history_roots")).isZero();
            assertThat(count(c,rig,"repository_preparation_root_releases")).isEqualTo(1);
            assertThat(count(c,rig,"repository_preparation_history_sets")).isEqualTo(1);
            assertThat(count(c,rig,"repository_preparation_pin_batches")).isEqualTo(1);
            assertThat(count(c,rig,"repository_preparation_capture_drains")).isEqualTo(1);
            for (String mutation : new String[]{"DELETE FROM repository_preparation_root_releases",
                    "UPDATE repository_preparation_root_releases SET root_count=root_count"}) {
                assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                    em.createNativeQuery(mutation+" WHERE operation_id=:o").setParameter("o",rig.record().command().operationId()).executeUpdate();
                })).hasStackTraceContaining("release is permanent");
            }
        }
    }

    @ParameterizedTest @ValueSource(strings={"roots", "captures", "token", "preparation"})
    void changedReceiptIdentityCannotRelease(String changed) throws Exception {
        try (var c=context(POSTGRES); var rig=historicalInitial(c,LEASE)) {
            abandon(c,rig); drain(c,rig);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { insert(em,rig,changed); delete(em,rig); }))
                    .hasStackTraceContaining("Release");
            assertLive(c,rig);
        }
    }

    @Test void migrationPreservesExistingRootsAndRequiresExistingDrainEvidence() throws Exception {
        try (var c=context(POSTGRES,"110"); var rig=historicalInitial(c,LEASE)) {
            abandon(c,rig);
            org.flywaydb.core.Flyway.configure().dataSource(c.pool()).schemas(c.pool().getSchema())
                    .defaultSchema(c.pool().getSchema()).locations("classpath:db/migration/repo").load().migrate();
            assertLive(c,rig);
            assertThat(count(c,rig,"repository_preparation_capture_drains")).isZero();
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { insert(em,rig,""); }))
                    .hasStackTraceContaining("has not drained exactly");
            drain(c,rig);
            c.tx().inTransaction(em -> { insert(em,rig,""); assertThat(delete(em,rig)).isEqualTo(1); });
            assertThat(count(c,rig,"repository_preparation_root_releases")).isEqualTo(1);
        }
    }

    @Test void canonicalCancellationBindsExactTerminalReceipt() throws Exception {
        try (var c=context(POSTGRES); var rig=historicalInitial(c,LEASE)) {
            var command=rig.record().command();
            try (var work=rig.sources().work()) {
                var admission=RepositoryOperationLedger.prepareHistoricalAdmission(rig.record().key(),command,
                        rig.record().seeds().ownerNonce(),LEASE,work);
                var owner=c.tx().inTransaction(em -> {
                    RepositoryExecutionClaimLedger.lockLive(em,rig.claim());
                    DocumentPublicationModesJournal.insert(em,rig.claim(),rig.record(),DocumentPublicationModesJournal.encode(command,
                            java.util.Map.of(command.intent().getMembers(0).getMemberId(),DocumentPublicationCandidate.Mode.TYPED)));
                    return admission.apply(em,rig.claim()).owner().orElseThrow();
                });
                new DocumentPublicationRejections(c.tx()).cancel(CALLER,owner,command,NONE).rejection().orElseThrow();
            }
            drain(c,rig);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { insertRejection(em,rig,true); }))
                    .hasStackTraceContaining("Release rejection differs");
            assertLive(c,rig);
            var released=DocumentPreparationRootReleases.release(c.tx(),rig.budget(),
                    CALLER,rig.record(),NONE);
            assertThat(released.terminal()).isInstanceOfSatisfying(DocumentPreparationTerminalEvidence.Outcome.class,
                    outcome -> assertThat(outcome.kind()).isEqualTo("REJECTION"));
            assertThat(count(c,rig,"repository_preparation_root_releases")).isEqualTo(1);
            assertThat(count(c,rig,"repository_preparation_history_roots")).isZero();
        }
    }

    static void insertRejection(EntityManager em,Rig rig,boolean changed) {
        assertThat(em.createNativeQuery("""
                INSERT INTO repository_preparation_root_releases(account_id,principal,operation_id,predecessor_generation,
                 preparation_sha256,command_sha256,root_count,roots_sha256,capture_count,captures_sha256,
                 terminal_kind,terminal_generation,terminal_sha256,terminal_xid)
                SELECT h.account_id,h.principal,h.operation_id,h.predecessor_generation,h.preparation_sha256,
                 h.command_sha256,h.expected_count,h.roots_sha256,1,
                 repository_preparation_capture_fingerprint(h.account_id,h.principal,h.operation_id,h.predecessor_generation),
                 'REJECTION',r.owner_generation,
                """+(changed?"sha256('wrong'::bytea)":"r.result_sha256")+",r.creation_xid "+"""
                FROM repository_preparation_history_sets h JOIN repository_operation_rejection r USING(account_id,principal,operation_id)
                WHERE h.operation_id=:o AND h.predecessor_generation=0
                """).setParameter("o",rig.record().command().operationId()).executeUpdate()).isEqualTo(1);
    }

    static void abandon(Context c,Rig rig) {
        DocumentPublicationAbandonment.abandon(c.tx(),rig.budget(),CALLER,rig.claim(),rig.record(),NONE);
    }
    static void drain(Context c,Rig rig) {
        rig.sources().close(); rig.history().close(); rig.history().release();
        rig.reads().fence(); rig.reads().attestLocalQuiescence();
        var digest=c.tx().readOnly(em -> (byte[])em.createNativeQuery(
                "SELECT pins_sha256 FROM repository_preparation_pin_batches WHERE operation_id=:o")
                .setParameter("o",rig.record().command().operationId()).getSingleResult());
        var identity=new DocumentPreparationCaptureDrain.Identity(new RepositoryCoordinatorDrain.Identity(
                rig.record().key(),rig.record().command().sha256(),rig.claim().epoch(),rig.claim().token(),rig.coordinator()),
                0,HexFormat.of().formatHex(digest));
        assertThat(DocumentPreparationCaptureDrain.recover(c.tx(),CALLER,identity,NONE).kind()).isEqualTo("QUIESCED");
    }
    static void insert(EntityManager em,Rig rig,String changed) {
        String preparation=changed.equals("preparation")?"sha256('wrong'::bytea)":"h.preparation_sha256";
        String roots=changed.equals("roots")?"sha256('wrong'::bytea)":"h.roots_sha256";
        String captures=changed.equals("captures")?"sha256('wrong'::bytea)":
                "repository_preparation_capture_fingerprint(h.account_id,h.principal,h.operation_id,h.predecessor_generation)";
        String token=changed.equals("token")?"gen_random_uuid()":"CAST(:token AS uuid)";
        var q=em.createNativeQuery("""
                INSERT INTO repository_preparation_root_releases(account_id,principal,operation_id,predecessor_generation,
                 preparation_sha256,command_sha256,root_count,roots_sha256,capture_count,captures_sha256,
                 terminal_kind,abandonment_token,abandonment_nonce)
                SELECT h.account_id,h.principal,h.operation_id,h.predecessor_generation,
                """+preparation+",h.command_sha256,h.expected_count,"+roots+",1,"+captures+",'ABANDONMENT',"+token+",CAST(:nonce AS uuid) "+"""
                FROM repository_preparation_history_sets h WHERE h.operation_id=:o AND h.predecessor_generation=0
                """).setParameter("o",rig.record().command().operationId()).setParameter("nonce",rig.record().seeds().ownerNonce());
        if (!changed.equals("token")) q.setParameter("token",rig.claim().token());
        assertThat(q.executeUpdate()).isEqualTo(1);
    }
    static int delete(EntityManager em,Rig rig) {
        return em.createNativeQuery("DELETE FROM repository_preparation_history_roots WHERE operation_id=:o AND predecessor_generation=0")
                .setParameter("o",rig.record().command().operationId()).executeUpdate();
    }
    static long count(Context c,Rig rig,String table) {
        return c.tx().readOnly(em -> ((Number)em.createNativeQuery("SELECT count(*) FROM "+table+" WHERE operation_id=:o")
                .setParameter("o",rig.record().command().operationId()).getSingleResult()).longValue());
    }
    static void assertLive(Context c,Rig rig) {
        assertThat(count(c,rig,"repository_preparation_history_roots")).isEqualTo(1);
        assertThat(count(c,rig,"repository_preparation_root_releases")).isZero();
    }
    private static final class Abort extends RuntimeException {}
}
