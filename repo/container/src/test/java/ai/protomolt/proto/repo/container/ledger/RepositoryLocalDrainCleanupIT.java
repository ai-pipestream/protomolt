package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import com.google.protobuf.StringValue;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPublicationRegistrationInspectionIT.*;
import static org.assertj.core.api.Assertions.*;

/** SQL-only positive coverage for local-drain-gated schema retention cleanup. */
@Testcontainers
class RepositoryLocalDrainCleanupIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void localDrainAllowsRecoveryOnlyReleaseOfReplacedSchemaClaim() throws Exception {
        try (var c = context(POSTGRES)) {
            var value = input(c);
            var budget = new PayloadBudget(64_000_000);
            var incarnation = UUID.randomUUID();
            var claim = new DocumentPublicationPreparationJournal(c.tx(), budget)
                    .acquireInitial(CALLER, value, UUID.randomUUID(), incarnation, NONE);
            new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, claim, 0, MODES, NONE);

            var operations = new RepositoryOperationLedger(c.tx());
            var first = operations.admit(value.key(), value.command(), value.seeds().ownerNonce(), Duration.ofSeconds(1), claim)
                    .owner().orElseThrow();
            var descriptor = ai.protomolt.proto.descriptors.DescriptorFingerprints
                    .closure(StringValue.getDescriptor()).toByteString();
            var artifacts = new RepositorySchemaArtifacts(c.tx());
            artifacts.stage(first, value.command(), List.of(descriptor), () -> {});

            // Let only the operation-owner lease expire. The independent execution claim stays live.
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            var current = operations.takeOver(value.key(), value.command(), first.generation(), UUID.randomUUID(), LEASE, claim);
            artifacts.stage(current, value.command(), List.of(descriptor), () -> {});
            assertThat(claimCount(c, value, 1)).isEqualTo(1);
            assertThat(claimCount(c, value, 2)).isEqualTo(1);
            assertThat(artifactCount(c, value)).isEqualTo(1);

            RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, incarnation, NONE);
            RepositoryCoordinatorLocalDrain.record(c.tx(), CALLER, new RepositoryCoordinatorDrain.Identity(
                    claim.key(), claim.commandSha256(), claim.epoch(), claim.token(), incarnation), NONE);

            assertThatThrownBy(() -> operations.renew(current, LEASE)).hasStackTraceContaining("locally drained");
            assertThatThrownBy(() -> new RepositoryExecutionClaimLedger(c.tx()).renew(claim, LEASE))
                    .hasStackTraceContaining("locally drained");

            var ownerBefore = ownerSnapshot(c, value);
            assertThat(ownerBefore[4]).isNull();
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { em.createNativeQuery("""
                    DELETE FROM repository_schema_artifact_claims
                    WHERE account_id=:a AND principal=:p AND operation_id=:o AND owner_generation=1
                    """).setParameter("a", value.key().account()).setParameter("p", value.key().principal())
                    .setParameter("o", value.key().operationId()).executeUpdate(); }))
                    .hasStackTraceContaining("recovery fence");
            assertThat(claimCount(c, value, 1)).isEqualTo(1);

            int removed = c.tx().inTransaction(em -> { return ((Number) em.createNativeQuery(
                    "SELECT release_repository_replaced_schema_claims(:a,:p,:o,10)")
                    .setParameter("a", value.key().account()).setParameter("p", value.key().principal())
                    .setParameter("o", value.key().operationId()).getSingleResult()).intValue(); });
            assertThat(removed).isEqualTo(1);
            assertThat(claimCount(c, value, 1)).isZero();
            assertThat(claimCount(c, value, 2)).isEqualTo(1);
            assertThat(artifactCount(c, value)).isEqualTo(1);

            var ownerAfter = ownerSnapshot(c, value);
            assertThat(List.of(ownerAfter[0], ownerAfter[1], ownerAfter[2], ownerAfter[3]))
                    .containsExactly(ownerBefore[0], ownerBefore[1], ownerBefore[2], ownerBefore[3]);
            assertThat(ownerAfter[4]).isNotNull();
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    private static long claimCount(Context c, DocumentPublicationPreparationRecord value,
            int generation) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("""
                SELECT count(*) FROM repository_schema_artifact_claims
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND owner_generation=:g
                """).setParameter("a", value.key().account()).setParameter("p", value.key().principal())
                .setParameter("o", value.key().operationId()).setParameter("g", generation).getSingleResult()).longValue());
    }

    private static long artifactCount(Context c, DocumentPublicationPreparationRecord value) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("""
                SELECT count(*) FROM repository_schema_artifacts
                WHERE account_id=:a AND artifact_sha256=(
                    SELECT artifact_sha256 FROM repository_schema_artifact_claims
                    WHERE account_id=:a AND principal=:p AND operation_id=:o LIMIT 1)
                """).setParameter("a", value.key().account()).setParameter("p", value.key().principal())
                .setParameter("o", value.key().operationId()).getSingleResult()).longValue());
    }

    private static Object[] ownerSnapshot(Context c, DocumentPublicationPreparationRecord value) {
        return c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT owner_generation,owner_token,lease_until,CAST(write_fence_xid AS text),CAST(recovery_fence_xid AS text)
                FROM repository_operation_owners WHERE account_id=:a AND principal=:p AND operation_id=:o
                """).setParameter("a", value.key().account()).setParameter("p", value.key().principal())
                .setParameter("o", value.key().operationId()).getSingleResult());
    }
}
