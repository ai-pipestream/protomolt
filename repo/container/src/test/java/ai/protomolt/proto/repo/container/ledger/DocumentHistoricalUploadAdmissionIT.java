package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentHistoricalRestoreAssessmentIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL admission and JDBC faults. Retained source provider observations are synthetic fixtures. */
@Testcontainers
class DocumentHistoricalUploadAdmissionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    @ParameterizedTest @ValueSource(strings = {"reuse-only", "mixed", "rollback", "lost-ack", "witness", "pin", "closed", "budget"})
    void initialAdmissionRequiresLiveHistoricalCapabilityAndKeepsExactAttempts(String variant) throws Exception {
        try (var c = context(POSTGRES)) {
            var original = DocumentSchemaRetentionFixture.prepare(c);
            new DocumentSchemaPolicies(c.tx()).activate(original.batch().policy().policy(), 0, () -> {});
            var revision = DocumentSchemaRetentionFixture.publishBound(c, original, (em, candidate) -> {},
                    (em, id, manifest) -> original.retention().write(em, original.owner(), id, () -> {}));
            var fixture = new Fixture(original, revision);
            var reads = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = reads.captureHistorical(CALLER, fixture.address(), revision);
            var budget = new PayloadBudget(64L * 1024 * 1024);
            try {
                var member = member(fixture, history).toBuilder();
                if (!variant.equals("reuse-only")) {
                    byte[] bytes = Document.newBuilder().setDocId("fresh parsed fragment").build().toByteArray();
                    member.addParts(DocumentPublicationPart.newBuilder()
                            .setSlot(DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED))
                            .setUpload(PublicationUpload.newBuilder().setSizeBytes(bytes.length)
                                    .setSha256(DocumentPartCodec.sha256Hex(bytes)).setContentType("application/protobuf")));
                }
                var command = new DocumentPublicationCommand(original.command().intent().toBuilder()
                        .setOperationId(UUID.randomUUID().toString()).setMembers(0, member).build());
                var key = new RepositoryOperationLedger.Key("account", CALLER.principalName(), command.operationId());
                var placement = original.prepared().members().getFirst().placement();
                var record = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                        Map.of(placement.drive().id(), placement), Duration.ofMinutes(5), 0);
                var armed = new AtomicBoolean();
                var lost = new AtomicBoolean();
                var faulted = DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                    if (!armed.get()) return;
                    try (var statement = connection.prepareStatement("SELECT count(*) FROM document_operation_selections WHERE operation_id=?")) {
                        statement.setObject(1, command.operationId());
                        try (var rows = statement.executeQuery()) {
                            rows.next();
                            if (rows.getLong(1) == 1 && armed.compareAndSet(true, false)) {
                                if (variant.equals("rollback")) throw new java.sql.SQLException("injected historical upload rollback");
                                lost.set(true);
                            }
                        }
                    }
                });
                var datasource = DocumentJdbcFaults.afterCommit(faulted, () -> {
                    if (lost.compareAndSet(true, false)) throw new java.sql.SQLException("injected historical upload lost acknowledgement");
                });
                try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                            Map.of("hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"));
                     var sources = DocumentHistoricalAssessmentSources.open(command, CALLER, List.of(history), NONE)) {
                    var registration = DocumentPublicationRegistration.historical(new Tx(emf), budget, record, sources,
                            UUID.randomUUID(), new DocumentPublicationScopeCalls(), new DriveLedger(c.tx()), NONE);
                    var modes = Map.of("member", DocumentPublicationCandidate.Mode.TYPED);
                    var owner = registration.admitInitial(CALLER, modes, NONE).orElseThrow();
                    try (var execution = registration.historicalExecution(CALLER, owner, modes, NONE)) {
                        var unqualified = DocumentOperationUploadAdmission.prepareHistorical(command, record.placements(),
                                record.seeds().attempts(), record.lease(), record.seeds().uploadTokens(), sources.references(command, () -> {}), () -> {});
                        assertThatThrownBy(() -> new DocumentOperationUploadAdmission(c.tx(), new DriveLedger(c.tx()))
                                .admitOrReuseVerified(CALLER, owner, unqualified))
                                .isInstanceOf(UnsupportedOperationException.class);
                        assertThat(count(c, "document_operation_selections", command)).isZero();
                        if (variant.equals("witness") || variant.equals("pin")) {
                            c.tx().inTransaction(em -> {
                                em.createNativeQuery("SET LOCAL session_replication_role=replica").executeUpdate();
                                if (variant.equals("pin")) em.createNativeQuery("DELETE FROM document_read_pins").executeUpdate();
                                else em.createNativeQuery("UPDATE document_part_attempt_objects SET provider_version='changed' WHERE physical_object_id=:id")
                                        .setParameter("id", UUID.fromString(command.intent().getMembers(0).getParts(0)
                                                .getHistoricalReuse().getObject().getObjectId())).executeUpdate();
                            });
                            assertThatThrownBy(() -> execution.admitUploads(CALLER, NONE)).isInstanceOf(DocumentPartAttemptLedger.FenceException.class);
                        } else if (variant.equals("closed")) {
                            execution.close();
                            assertThatThrownBy(() -> execution.admitUploads(CALLER, NONE)).isInstanceOf(IllegalStateException.class);
                        } else {
                            if (variant.equals("budget")) {
                                try (var occupied = budget.reserve(budget.capacity() - budget.reservedBytes())) {
                                    assertThatThrownBy(() -> execution.admitUploads(CALLER, NONE))
                                            .isInstanceOf(PayloadBudget.CapacityExceededException.class);
                                    assertThat(count(c, "document_operation_selections", command)).isZero();
                                    assertThat(count(c, "document_part_attempts", command)).isZero();
                                }
                            }
                            if (variant.equals("rollback") || variant.equals("lost-ack")) {
                                armed.set(true);
                                assertThatThrownBy(() -> execution.admitUploads(CALLER, NONE)).hasStackTraceContaining("injected historical upload");
                                assertThat(count(c, "document_operation_selections", command)).isEqualTo(variant.equals("lost-ack") ? 1 : 0);
                                assertThat(count(c, "document_part_attempts", command)).isEqualTo(variant.equals("lost-ack") ? 1 : 0);
                            }
                            if (!variant.equals("lost-ack")) {
                                var result = execution.admitUploads(CALLER, NONE);
                                assertThat(result.reusedVerified()).isFalse();
                                assertThat(result.attempts()).hasSize(variant.equals("reuse-only") ? 0 : 1);
                                if (!result.attempts().isEmpty()) {
                                    assertThat(result.attempts().getFirst().id()).isEqualTo(record.seeds().attempts().get("member"));
                                    assertThat(result.attempts().getFirst().state()).isEqualTo("STAGING");
                                }
                            }
                            if (variant.equals("reuse-only")) assertThat(execution.admitUploads(CALLER, NONE).reusedVerified()).isTrue();
                            else {
                                var before = attempts(c, command);
                                assertThat(before.get(0)).isEqualTo(record.seeds().attempts().get("member"));
                                assertThat(before.get(1)).isEqualTo(record.seeds().uploadTokens().get("member"));
                                assertThatThrownBy(() -> execution.admitUploads(CALLER, NONE))
                                        .hasMessage("Initial upload is not an exact live verified retry");
                                assertThat(attempts(c, command)).isEqualTo(before);
                            }
                            assertThat(count(c, "document_operation_selections", command)).isEqualTo(1);
                        }
                        if (Set.of("witness", "pin", "closed").contains(variant)) {
                            assertThat(count(c, "document_operation_selections", command)).isZero();
                            assertThat(count(c, "document_part_attempts", command)).isZero();
                        }
                        assertThat(count(c, "repository_publication_assessment_starts", command)).isZero();
                    }
                }
            } finally {
                assertThat(budget.reservedBytes()).isZero();
                release(reads, history);
            }
        }
    }

    private static long count(Context c, String table, DocumentPublicationCommand command) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE operation_id=:o")
                .setParameter("o", command.operationId()).getSingleResult()).longValue());
    }
    private static List<?> attempts(Context c, DocumentPublicationCommand command) {
        return c.tx().readOnly(em -> Arrays.asList((Object[]) em.createNativeQuery(
                "SELECT attempt_id,lease_token,lease_until,state FROM document_part_attempts WHERE operation_id=:o")
                .setParameter("o", command.operationId()).getSingleResult()));
    }
}
