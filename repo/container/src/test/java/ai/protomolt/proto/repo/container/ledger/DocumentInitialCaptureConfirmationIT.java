package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real capture SQL; privileged corruption cases deliberately bypass immutable-row triggers. */
@Testcontainers
class DocumentInitialCaptureConfirmationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @ParameterizedTest
    @ValueSource(strings = {"valid", "missing-owner", "missing-batch", "wrong-token", "changed-child", "noninitial", "creation-xid", "closed", "pins-released", "drained"})
    void confirmationNeverRepairsMissingOrIneligibleCapture(String variant) throws Exception {
        try (var c = context(POSTGRES);
             var rig = DocumentCaptureAdmissionClosureIT.historicalInitial(c, Duration.ofMinutes(5))) {
            var pins = DocumentPreparationSourcePins.prepare(rig.record().command(),
                    rig.sources().references(rig.record().command(), () -> {}), () -> {});
            switch (variant) {
                case "missing-owner" -> corrupt(c, rig, "DELETE FROM repository_preparation_pin_owners WHERE operation_id=:o");
                case "missing-batch" -> {
                    corrupt(c, rig, "DELETE FROM repository_preparation_pin_owners WHERE operation_id=:o");
                    corrupt(c, rig, "DELETE FROM repository_preparation_source_pins WHERE operation_id=:o");
                    corrupt(c, rig, "DELETE FROM repository_preparation_pin_batches WHERE operation_id=:o");
                }
                case "changed-child" -> corrupt(c, rig, "UPDATE repository_preparation_source_pins SET publication_revision=publication_revision+1 WHERE operation_id=:o");
                case "noninitial" -> corrupt(c, rig, "UPDATE repository_preparation_pin_batches SET initial_capture=false WHERE operation_id=:o");
                case "creation-xid" -> corrupt(c, rig, "UPDATE repository_preparation_pin_batches SET creation_xid=pg_current_xact_id() WHERE operation_id=:o");
                case "closed" -> DocumentPreparationRootReleaseIT.abandon(c, rig);
                case "drained" -> DocumentPreparationRootReleaseIT.drain(c, rig);
                case "pins-released" -> {
                    rig.sources().close(); rig.history().close();
                    rig.history().release();
                }
                default -> { }
            }
            var claim = variant.equals("wrong-token")
                    ? new RepositoryExecutionClaimLedger.Claim(rig.claim().key(), rig.claim().commandSha256(),
                            rig.claim().epoch(), UUID.randomUUID(), rig.claim().leaseUntil())
                    : rig.claim();
            long before = count(c, rig, "repository_preparation_pin_batches");
            var error = catchThrowable(() -> c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, claim);
                DocumentPreparationSourcePins.requireInitial(em, rig.record(), pins, claim, rig.coordinator(), () -> {});
                return null;
            }));
            if (variant.equals("valid")) {
                assertThat(error).isNull();
                // Confirmation is repeatable and does not append another capture.
                c.tx().inTransaction(em -> {
                    RepositoryExecutionClaimLedger.lockLive(em, claim);
                    DocumentPreparationSourcePins.requireInitial(em, rig.record(), pins, claim, rig.coordinator(), () -> {});
                    return null;
                });
            } else {
                assertThat(error).isNotNull();
                switch (variant) {
                    case "missing-owner", "missing-batch" -> assertThat(error).hasMessage("Initial preparation capture is absent");
                    case "changed-child", "noninitial", "creation-xid" -> assertThat(error).hasMessage("Preparation source pin batch differs from its captured identities");
                    case "closed" -> assertThat(error).hasMessage("Initial preparation capture admission is closed");
                    case "drained" -> assertThat(error).hasMessage("Initial preparation capture is no longer executable");
                    case "pins-released" -> assertThat(error).hasMessage("Initial preparation capture no longer has its live historical pins");
                    case "wrong-token" -> assertThat(error).isInstanceOf(RepositoryExecutionClaimLedger.Fenced.class);
                    default -> throw new AssertionError(variant);
                }
            }
            assertThat(count(c, rig, "repository_preparation_pin_batches")).isEqualTo(before);
            assertThat(count(c, rig, "repository_preparation_pin_owners"))
                    .isEqualTo(variant.equals("missing-owner") || variant.equals("missing-batch") ? 0 : 1);
        }
    }

    private static void corrupt(Context c, DocumentCaptureAdmissionClosureIT.Rig rig, String sql) {
        c.tx().inTransaction(em -> {
            em.createNativeQuery("SET LOCAL session_replication_role=replica").executeUpdate();
            em.createNativeQuery(sql).setParameter("o", rig.record().key().operationId()).executeUpdate();
        });
    }

    private static long count(Context c, DocumentCaptureAdmissionClosureIT.Rig rig, String table) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM " + table + " WHERE operation_id=:o")
                .setParameter("o", rig.record().key().operationId()).getSingleResult()).longValue());
    }
}
