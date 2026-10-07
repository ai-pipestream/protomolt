package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentHistoricalRestoreAssessmentIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real V104-to-V105 migration; provider observations used to publish the source are synthetic. */
@Testcontainers
class DocumentPreparationPinOwnerMigrationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);

    @Test void oldCaptureRemainsUnownedAndCannotBeAssignedToTheCurrentCoordinator() throws Exception {
        try (var c = context(POSTGRES, "104")) {
            var original = DocumentSchemaRetentionFixture.prepare(c);
            new DocumentSchemaPolicies(c.tx()).activate(original.batch().policy().policy(), 0, () -> {});
            var revision = DocumentSchemaRetentionFixture.publishBound(c, original, (em, candidate) -> {},
                    (em, id, manifest) -> original.retention().write(em, original.owner(), id, () -> {}));
            var fixture = new Fixture(original, revision);
            var reads = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = reads.captureHistorical(CALLER, fixture.address(), revision);
            try {
                var command = new DocumentPublicationCommand(original.command().intent().toBuilder()
                        .setOperationId(UUID.randomUUID().toString()).setMembers(0, member(fixture, history)).build());
                var key = new RepositoryOperationLedger.Key("account", "principal", command.operationId());
                var placement = original.prepared().members().getFirst().placement();
                var record = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                        Map.of(placement.drive().id(), placement), Duration.ofMinutes(5), 0);
                var coordinator = UUID.randomUUID();
                try (var sources = DocumentHistoricalAssessmentSources.open(command, CALLER, List.of(history), RepositoryReadControl.NONE)) {
                    var pins = DocumentPreparationSourcePins.prepare(command, sources.references(command, () -> {}), () -> {});
                    var claim = c.tx().inTransaction(em -> {
                        var acquired = RepositoryExecutionClaimLedger.acquireHistoricalInitialInTransaction(
                                em, key, command, UUID.randomUUID(), Duration.ofMinutes(5), sources);
                        RepositoryCoordinatorBinding.bindInitial(em, acquired, coordinator);
                        var bytes = DocumentPublicationPreparationCodec.encode(record);
                        DocumentPublicationPreparationJournal.insert(em, acquired.claim(), record, bytes,
                                DocumentPublicationPreparationJournal.digest(bytes), sources.references(command, () -> {}));
                        var objects = pins.pins().stream().map(DocumentHistoricalSourcePin::object).collect(java.util.stream.Collectors.toSet());
                        var locks = DocumentPublicationLocks.lockIndependentOrigins(em,
                                Set.of(ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(fixture.address())), objects, Set.of());
                        DocumentPublicationLocks.lockIndependentRetention(em, locks);
                        // Execute the V104 storage protocol, before capture ownership existed.
                        em.createNativeQuery("""
                                INSERT INTO repository_preparation_pin_batches(account_id,principal,operation_id,predecessor_generation,
                                 pins_sha256,expected_count,initial_capture)
                                VALUES('account','principal',:id,0,:digest,:count,true)
                                """).setParameter("id", key.operationId()).setParameter("digest", pins.digest())
                                .setParameter("count", pins.pins().size()).executeUpdate();
                        em.createNativeQuery("""
                                INSERT INTO repository_preparation_source_pins(account_id,principal,operation_id,predecessor_generation,
                                 pins_sha256,pin_id,reader_incarnation,object_id,node_id,revision_id,publication_revision)
                                SELECT 'account','principal',:id,0,:digest,q.pin,q.reader,q.object,q.node,q.revision,q.publication
                                FROM jsonb_to_recordset(CAST(:rows AS jsonb))
                                 q(reader uuid,pin uuid,object uuid,node uuid,revision uuid,publication bigint) ORDER BY q.pin
                                """).setParameter("id", key.operationId()).setParameter("digest", pins.digest())
                                .setParameter("rows", pins.json()).executeUpdate();
                        em.createNativeQuery("UPDATE repository_preparation_pin_batches SET sealed=true WHERE operation_id=:id")
                                .setParameter("id", key.operationId()).executeUpdate();
                        return acquired.claim();
                    });
                    org.flywaydb.core.Flyway.configure().dataSource(c.pool()).schemas(c.pool().getSchema()).defaultSchema(c.pool().getSchema())
                            .locations("classpath:db/migration/repo").target("105").load().migrate();
                    assertThat(count(c, "repository_preparation_pin_owners")).isZero();
                    assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                            DocumentPreparationSourcePins.insert(em, record, pins, claim, coordinator, () -> {}); }))
                            .hasMessageContaining("ownership is unknown");
                    assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                        RepositoryExecutionClaimLedger.lockLive(em, claim);
                        em.createNativeQuery("""
                                INSERT INTO repository_preparation_pin_owners(account_id,principal,operation_id,predecessor_generation,
                                 pins_sha256,claim_epoch,claim_token,incarnation)
                                VALUES('account','principal',:id,0,:digest,:epoch,:token,:incarnation)
                                """).setParameter("id", key.operationId()).setParameter("digest", pins.digest())
                                .setParameter("epoch", claim.epoch()).setParameter("token", claim.token())
                                .setParameter("incarnation", coordinator).executeUpdate();
                    })).hasStackTraceContaining("exact live bound creation claim");
                    assertThat(count(c, "repository_preparation_pin_owners")).isZero();
                    assertThat(count(c, "repository_preparation_pin_batches")).isEqualTo(1);
                }
            } finally { release(reads, history); }
        }
    }
}
