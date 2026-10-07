package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPreparationRootReleaseIT.*;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class DocumentPreparationRootReleasesIT {
    @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER=new RepositoryCaller("principal",true);
    private static final RepositoryReadControl NONE=RepositoryReadControl.NONE;
    private static final Duration LEASE=Duration.ofMinutes(5);
    private static final long BYTES=3L*DocumentPublicationPreparationCodec.MAX_BYTES+1024*1024;

    @Test void releaseAndRetryUseExactBudgetAndRejectScopedCallers() throws Exception {
        try (var c=context(POSTGRES); var rig=historicalInitial(c,LEASE)) {
            abandon(c,rig); drain(c,rig);
            var budget=new PayloadBudget(BYTES);
            var scoped=new RepositoryCaller("principal",false,Set.of("account"),Set.of());
            assertThatThrownBy(() -> DocumentPreparationRootReleases.release(c.tx(),budget,scoped,rig.record(),NONE))
                    .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
            try (var occupied=budget.reserve(1)) {
                assertThatThrownBy(() -> DocumentPreparationRootReleases.release(c.tx(),budget,CALLER,rig.record(),NONE))
                        .isInstanceOf(PayloadBudget.CapacityExceededException.class);
                assertLive(c,rig);
                assertThat(budget.reservedBytes()).isEqualTo(1);
            }
            var receipt=DocumentPreparationRootReleases.release(c.tx(),budget,CALLER,rig.record(),NONE);
            assertThat(receipt.rootCount()).isEqualTo(1);
            assertThat(receipt.captureCount()).isEqualTo(1);
            assertThat(budget.reservedBytes()).isZero();
            assertThat(DocumentPreparationRootReleases.release(c.tx(),budget,CALLER,rig.record(),NONE)).isEqualTo(receipt);
            assertThat(budget.reservedBytes()).isZero();
            var changed=new DocumentPublicationPreparationRecord(rig.record().key(),rig.record().command(),rig.record().seeds(),
                    rig.record().placements(),Duration.ofMinutes(4),0);
            assertThatThrownBy(() -> DocumentPreparationRootReleases.release(c.tx(),budget,CALLER,changed,NONE))
                    .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.DATA_LOSS));
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void retryDoesNotQuerySourcePublicationOrPartTables() throws Exception {
        try (var c=context(POSTGRES); var rig=historicalInitial(c,LEASE)) {
            abandon(c,rig); drain(c,rig);
            var receipt=DocumentPreparationRootReleases.release(c.tx(),rig.budget(),CALLER,rig.record(),NONE);
            // Controlled source-query failure, not a claim that archival pruning is implemented.
            c.tx().inTransaction(em -> {
                em.createNativeQuery("ALTER TABLE document_revision_publications RENAME TO source_publications_unavailable").executeUpdate();
                em.createNativeQuery("ALTER TABLE document_revision_parts RENAME TO source_parts_unavailable").executeUpdate();
            });
            try {
                assertThatThrownBy(() -> c.tx().readOnly(em -> em.createNativeQuery("SELECT count(*) FROM document_revision_publications").getSingleResult()))
                        .hasStackTraceContaining("does not exist");
                assertThat(DocumentPreparationRootReleases.release(c.tx(),rig.budget(),CALLER,rig.record(),NONE)).isEqualTo(receipt);
            } finally {
                c.tx().inTransaction(em -> {
                    em.createNativeQuery("ALTER TABLE source_publications_unavailable RENAME TO document_revision_publications").executeUpdate();
                    em.createNativeQuery("ALTER TABLE source_parts_unavailable RENAME TO document_revision_parts").executeUpdate();
                });
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void actualCommitFaultPreservesAtomicStateAndExactRetry(boolean afterCommit) throws Exception {
        try (var c=context(POSTGRES); var rig=historicalInitial(c,LEASE)) {
            abandon(c,rig); drain(c,rig);
            var fault=new AtomicBoolean(true);
            var datasource=afterCommit?DocumentJdbcFaults.afterCommit(c.pool(),() -> {
                if (count(c,rig,"repository_preparation_root_releases")==1 && fault.compareAndSet(true,false))
                    throw new java.sql.SQLException("Lost root release acknowledgement","08006");
            }):DocumentJdbcFaults.beforeCommit(c.pool(),connection -> {
                try (var statement=connection.createStatement(); var rows=statement.executeQuery("SELECT count(*) FROM repository_preparation_root_releases")) {
                    rows.next();
                    if (rows.getInt(1)==1 && fault.compareAndSet(true,false)) throw new java.sql.SQLException("Abort root release commit","40001");
                }
            });
            try (var emf=jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of(
                    "hibernate.connection.datasource",datasource,"hibernate.hbm2ddl.auto","validate"))) {
                assertThatThrownBy(() -> DocumentPreparationRootReleases.release(new Tx(emf),rig.budget(),CALLER,rig.record(),NONE))
                        .hasStackTraceContaining(afterCommit?"Lost root release acknowledgement":"Abort root release commit");
            }
            assertThat(fault).isFalse();
            assertThat(count(c,rig,"repository_preparation_history_roots")).isEqualTo(afterCommit?0:1);
            assertThat(count(c,rig,"repository_preparation_root_releases")).isEqualTo(afterCommit?1:0);
            assertThat(rig.budget().reservedBytes()).isZero();
            var receipt=DocumentPreparationRootReleases.release(c.tx(),rig.budget(),CALLER,rig.record(),NONE);
            assertThat(DocumentPreparationRootReleases.release(c.tx(),rig.budget(),CALLER,rig.record(),NONE)).isEqualTo(receipt);
            assertThat(count(c,rig,"repository_preparation_root_releases")).isEqualTo(1);
        }
    }

    @ParameterizedTest @ValueSource(strings={"missing", "residual", "fingerprint"})
    void inconsistentRetentionIsCorruptionNotUnknown(String corruption) throws Exception {
        try (var c=context(POSTGRES); var rig=historicalInitial(c,LEASE)) {
            abandon(c,rig); drain(c,rig);
            if (!corruption.equals("missing")) DocumentPreparationRootReleases.release(c.tx(),rig.budget(),CALLER,rig.record(),NONE);
            // Test-admin corruption. Restore each guard before the handler observes it.
            c.tx().inTransaction(em -> {
                if (corruption.equals("fingerprint")) {
                    em.createNativeQuery("ALTER TABLE repository_preparation_root_releases DISABLE TRIGGER repository_preparation_root_release_guard").executeUpdate();
                    em.createNativeQuery("UPDATE repository_preparation_root_releases SET captures_sha256=sha256('wrong'::bytea) WHERE operation_id=:o")
                            .setParameter("o",rig.record().command().operationId()).executeUpdate();
                    em.createNativeQuery("ALTER TABLE repository_preparation_root_releases ENABLE TRIGGER repository_preparation_root_release_guard").executeUpdate();
                } else {
                    em.createNativeQuery("ALTER TABLE repository_preparation_history_roots DISABLE TRIGGER repository_preparation_history_root_guard").executeUpdate();
                    if (corruption.equals("missing")) assertThat(delete(em,rig)).isEqualTo(1);
                    else assertThat(em.createNativeQuery("""
                            INSERT INTO repository_preparation_history_roots(account_id,principal,operation_id,predecessor_generation,node_id,revision_id)
                            SELECT DISTINCT account_id,principal,operation_id,predecessor_generation,node_id,revision_id
                            FROM repository_preparation_source_pins WHERE operation_id=:o
                            """).setParameter("o",rig.record().command().operationId()).executeUpdate()).isEqualTo(1);
                    em.createNativeQuery("ALTER TABLE repository_preparation_history_roots ENABLE TRIGGER repository_preparation_history_root_guard").executeUpdate();
                }
            });
            assertThatThrownBy(() -> DocumentPreparationRootReleases.release(c.tx(),rig.budget(),CALLER,rig.record(),NONE))
                    .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.DATA_LOSS));
            assertThat(rig.budget().reservedBytes()).isZero();
        }
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void cancellationBeforeAndAfterCommitHasDistinctOutcomes(boolean afterCommit) throws Exception {
        try (var c=context(POSTGRES); var rig=historicalInitial(c,LEASE)) {
            abandon(c,rig); drain(c,rig);
            var cancelled=new AtomicBoolean(true);
            var control=new RepositoryReadControl() {
                @Override public boolean isCancelled() { return cancelled.get(); }
                @Override public long remainingNanos() { return Long.MAX_VALUE; }
            };
            assertThatThrownBy(() -> DocumentPreparationRootReleases.release(c.tx(),rig.budget(),CALLER,rig.record(),control))
                    .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CANCELLED));
            assertLive(c,rig); cancelled.set(false);
            var datasource=afterCommit?DocumentJdbcFaults.afterCommit(c.pool(),() -> {
                if (count(c,rig,"repository_preparation_root_releases")==1) cancelled.set(true);
            }):DocumentJdbcFaults.beforeCommit(c.pool(),connection -> {
                try (var statement=connection.createStatement(); var rows=statement.executeQuery("SELECT count(*) FROM repository_preparation_root_releases")) {
                    rows.next();
                    if (rows.getInt(1)==1) { cancelled.set(true); control.check(); }
                }
            });
            try (var emf=jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of(
                    "hibernate.connection.datasource",datasource,"hibernate.hbm2ddl.auto","validate"))) {
                if (afterCommit) {
                    var receipt=DocumentPreparationRootReleases.release(new Tx(emf),rig.budget(),CALLER,rig.record(),control);
                    assertThat(DocumentPreparationRootReleases.release(c.tx(),rig.budget(),CALLER,rig.record(),NONE)).isEqualTo(receipt);
                } else {
                    var failure=catchThrowable(() -> DocumentPreparationRootReleases.release(new Tx(emf),rig.budget(),CALLER,rig.record(),control));
                    assertThat(failure).isNotNull();
                    while (failure!=null && !(failure instanceof RepositoryException)) failure=failure.getCause();
                    assertThat(failure).isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                    assertLive(c,rig);
                }
            }
            assertThat(cancelled).isTrue();
            assertThat(rig.budget().reservedBytes()).isZero();
        }
    }
}
