package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.*;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real PostgreSQL/retention; provider observations in the shared fixture are synthetic. */
@Testcontainers
class DocumentRevisionRetentionInventoryIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller ADMIN = new RepositoryCaller("principal", true);

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void inventoriesNormalizedAssetsAndKeepsReadPinsUntilExplicitRelease(boolean typed) throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c, typed);
            new DocumentSchemaPolicies(c.tx()).activate(f.batch().policy().policy(), 0, () -> {});
            var revision = DocumentSchemaRetentionFixture.publishBound(c, f, (em, command) -> {},
                    (em, id, manifest) -> { if (typed) f.retention().write(em, f.owner(), id, () -> {}); });
            var address = f.command().intent().getMembers(0).getDestination().getAddress();
            var inventory = new DocumentRevisionRetentionInventory(c.tx());
            var initial = inventory.inspect(ADMIN, address, revision, RepositoryReadControl.NONE);
            assertThat(initial.current()).isTrue();
            assertThat(initial.sourceReadPins()).isZero();
            assertThat(initial.sourceAssessmentSlots()).isZero();
            assertThat(initial.sourcePreparationRoots()).isZero();
            assertThat(initial.objects()).hasSize(1).allSatisfy(object -> {
                assertThat(object.historicalRevisions()).isEqualTo(1);
                assertThat(object.currentRevisions()).isEqualTo(1);
                assertThat(object.documentReaders()).isZero();
                assertThat(object.archiveReaders()).isZero();
                assertThat(object.archiveVersions()).isZero();
                assertThat(object.assessments()).isZero();
                assertThat(object.mirrors()).isEqualTo(2);
                assertThat(object.reclaiming()).isFalse();
                assertThat(object.retiring()).isFalse();
            });
            assertThat(initial.artifacts()).extracting(DocumentRevisionRetentionInventory.ArtifactReferences::sha256)
                    .containsExactlyInAnyOrderElementsOf(typed ? f.batch().artifacts().keySet() : java.util.Set.of());
            assertThat(initial.artifacts()).allSatisfy(artifact -> {
                assertThat(artifact.revisions()).isEqualTo(1);
                assertThat(artifact.operationClaims()).isEqualTo(1);
                assertThat(artifact.assessments()).isZero();
            });
            assertThat(initial.unresolved()).containsExactly(DocumentRevisionRetentionInventory.UnresolvedReachability.values());
            var reads = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = reads.captureHistorical(ADMIN, address, revision);
            try (var use = history.use()) {
                history.close();
                assertThat(history.isDrained()).isFalse();
                var held = inventory.inspect(ADMIN, address, revision, RepositoryReadControl.NONE);
                assertThat(held.sourceReadPins()).isEqualTo(1);
                assertThat(held.objects()).allSatisfy(object -> {
                    assertThat(object.documentReaders()).isEqualTo(1);
                    assertThat(object.mirrors()).isEqualTo(3);
                });
            } finally { DocumentHistoricalRestoreAssessmentIT.release(reads, history); }
            assertThat(inventory.inspect(ADMIN, address, revision, RepositoryReadControl.NONE)).isEqualTo(initial);
            assertThatThrownBy(() -> initial.objects().clear()).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { em.createNativeQuery(
                    "DELETE FROM document_revision_publications WHERE revision_id=:revision")
                    .setParameter("revision", revision).executeUpdate(); })).isInstanceOf(RuntimeException.class);
            if (typed) assertThatThrownBy(() -> c.tx().inTransaction(em -> { em.createNativeQuery(
                    "DELETE FROM document_revision_schema_artifacts WHERE revision_id=:revision")
                    .setParameter("revision", revision).executeUpdate(); })).isInstanceOf(RuntimeException.class);
            assertThat(inventory.inspect(ADMIN, address, revision, RepositoryReadControl.NONE)).isEqualTo(initial);
        }
    }

    @Test void requiresAdministrativeAuthorityExactAddressAndActiveControl() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c);
            var revision = DocumentSchemaRetentionFixture.publish(c, f, (em, id) -> f.retention().write(em, f.owner(), id, () -> {}));
            var address = f.command().intent().getMembers(0).getDestination().getAddress();
            var inventory = new DocumentRevisionRetentionInventory(c.tx());
            assertThatThrownBy(() -> inventory.inspect(new RepositoryCaller("principal", false), address, revision, RepositoryReadControl.NONE))
                    .isInstanceOfSatisfying(RepositoryException.class, failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
            assertThatThrownBy(() -> inventory.inspect(ADMIN, address.toBuilder().setAccountId("other").build(), revision, RepositoryReadControl.NONE))
                    .isInstanceOfSatisfying(RepositoryException.class, failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
            assertThatThrownBy(() -> inventory.inspect(ADMIN, address, UUID.randomUUID(), RepositoryReadControl.NONE))
                    .isInstanceOfSatisfying(RepositoryException.class, failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
            var cancelled = new CancellationException("inventory cancelled");
            var control = new RepositoryReadControl() {
                public boolean isCancelled() { return true; }
                public void check() { throw cancelled; }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            assertThatThrownBy(() -> inventory.inspect(ADMIN, address, revision, control)).isSameAs(cancelled);
        }
    }

    @Test void migrationPreservesRevisionAndCountsPendingAssessmentArtifactOwnershipUntilRelease() throws Exception {
        try (var c = context(POSTGRES, "87")) {
            var f = DocumentSchemaRetentionFixture.prepare(c);
            var revision = DocumentSchemaRetentionFixture.publish(c, f, (em, id) -> f.retention().write(em, f.owner(), id, () -> {}));
            var address = f.command().intent().getMembers(0).getDestination().getAddress();
            String schema = c.pool().getSchema();
            org.flywaydb.core.Flyway.configure().dataSource(c.pool().getJdbcUrl(), c.pool().getUsername(), c.pool().getPassword())
                    .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo").load().migrate();
            long indexes = c.tx().readOnly(em -> ((Number) em.createNativeQuery("""
                    SELECT count(*) FROM pg_indexes WHERE schemaname=current_schema()
                    AND indexname IN ('document_read_pin_revision','document_assessment_slot_source_revision',
                                      'repository_preparation_history_revision')
                    """).getSingleResult()).longValue());
            assertThat(indexes).isEqualTo(3);
            var inventory = new DocumentRevisionRetentionInventory(c.tx());
            var baseline = inventory.inspect(ADMIN, address, revision, RepositoryReadControl.NONE);
            assertThat(baseline.sourcePreparationRoots()).isZero();
            assertThat(baseline.unresolved()).contains(
                    DocumentRevisionRetentionInventory.UnresolvedReachability.PREPARATION_JOURNAL_SELECTORS);
            var stagedFixture = new DocumentAssessmentRetentionFixture(c.tx());
            var pending = stagedFixture.candidate(120);
            var digests = new RepositorySchemaArtifacts(c.tx()).stage(pending.owner(),
                    java.util.List.copyOf(f.batch().artifacts().values()), () -> {});
            c.tx().inTransaction(em -> {
                RepositoryOperationLedger.fenceLiveOwner(em, pending.owner());
                // Synthetic assessment declarations; actual retained artifact bytes and SQL ownership.
                stagedFixture.insertOwner(em, pending, 1, 2, digests.size());
                stagedFixture.insertSlots(em, pending, "revision_ordinal", 1);
                stagedFixture.seal(em, pending);
                for (String sha : digests) em.createNativeQuery("""
                        INSERT INTO document_assessment_artifacts(assessment_id,account_id,artifact_sha256)
                        VALUES(:assessment,'account',decode(:sha,'hex'))
                        """).setParameter("assessment", pending.assessment()).setParameter("sha", sha).executeUpdate();
            });
            var held = inventory.inspect(ADMIN, address, revision, RepositoryReadControl.NONE);
            assertThat(held.objects()).isEqualTo(baseline.objects());
            assertThat(held.artifacts()).hasSize(digests.size()).allSatisfy(artifact -> {
                assertThat(artifact.revisions()).isEqualTo(1);
                assertThat(artifact.operationClaims()).isEqualTo(2);
                assertThat(artifact.assessments()).isEqualTo(1);
            });
            stagedFixture.expire("document_assessment_owners", "assessment_id", pending.assessment(), "retain_until");
            assertThat(stagedFixture.release(pending)).isTrue();
            var released = inventory.inspect(ADMIN, address, revision, RepositoryReadControl.NONE);
            assertThat(released.artifacts()).hasSize(digests.size()).allSatisfy(artifact -> {
                assertThat(artifact.revisions()).isEqualTo(1);
                assertThat(artifact.operationClaims()).isEqualTo(2);
                assertThat(artifact.assessments()).isZero();
            });
            assertThat(released.objects()).isEqualTo(baseline.objects());
        }
    }
}
