package ai.protomolt.proto.repo.container.ledger;

import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Populated upgrade with real pin guards; physical verification is synthetic fixture evidence. */
@Testcontainers
class DocumentHistoricalPinCompatibilityIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void nativeRevisionRemainsPinnableAfterAnotherPublicationReplacesIt() {
        try (var c = context(POSTGRES)) {
            var f = prepare(c, 1);
            var result = publish(c, f, Fault.NONE, em -> {});
            var revision = UUID.fromString(result.getMembers(0).getRevisionId());
            var source = f.sources().getFirst();
            var member = f.command().intent().getMembers(0);
            var drive = new DriveLedger(c.tx()).findById(UUID.fromString(member.getDriveId())).orElseThrow();
            var profile = new ManagedBackendLedger(c.tx()).find("native-test").orElseThrow();
            var current = new DocumentLedger(c.tx()).findByNodeId(source.row().nodeId).orElseThrow();
            var condition = member.getDestination().toBuilder().setExpectedMutationRevision(current.mutationRevision).build();
            var nextMember = member.toBuilder().setDestination(condition).clearParts();
            for (var part : member.getPartsList())
                nextMember.addParts(part.toBuilder().setReuse(part.getReuse().toBuilder().setSource(condition)));
            var command = new ai.protomolt.proto.repo.spi.DocumentPublicationCommand(f.command().intent().toBuilder()
                    .setOperationId(UUID.randomUUID().toString()).clearMembers().addMembers(nextMember).build());
            var owner = new RepositoryOperationLedger(c.tx()).admit(
                    new RepositoryOperationLedger.Key("account", "principal", command.operationId()), command,
                    UUID.randomUUID(), java.time.Duration.ofMinutes(5)).owner().orElseThrow();
            var plan = DocumentOperationUploadAdmission.prepare(command,
                    java.util.Map.of(drive.driveId, DocumentUploadPlan.Placement.sample(drive, "native-test", profile)),
                    java.util.Map.of(), java.time.Duration.ofMinutes(5));
            new DocumentOperationUploadAdmission(c.tx(), new DriveLedger(c.tx())).admit(
                    new ai.protomolt.proto.repo.spi.RepositoryCaller("principal", true), owner, plan);
            var nextSource = new ManagedDocumentFixture(current, revision, source.slots(), source.identities());
            publish(c, new Prepared(command, owner, java.util.List.of(nextSource), java.util.Map.of()), Fault.NONE, em -> {});
            var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var captured = ledger.captureHistorical(new ai.protomolt.proto.repo.spi.RepositoryCaller("principal", true),
                    member.getDestination().getAddress(), revision);
            try (var use = captured.use()) {
                assertThat(use.plan().revision()).isEqualTo(revision);
                assertThat(use.plan().manifest()).isEqualTo(current.readManifest());
                assertThat(use.plan().entries()).extracting(DocumentHistoricalReadPlan.Entry::objectId)
                        .containsExactlyElementsOf(source.identities().stream()
                                .map(identity -> UUID.fromString(identity.getObjectId())).toList());
                var latest = new DocumentLedger(c.tx()).findByNodeId(current.nodeId).orElseThrow();
                assertThat(latest.readManifest().getDocVersion()).isGreaterThan(use.plan().manifest().getDocVersion());
            } finally {
                captured.close(); captured.release(); ledger.fence(); ledger.attestLocalQuiescence();
            }
            UUID reader = UUID.randomUUID(), pin = UUID.randomUUID();
            UUID object = UUID.fromString(source.identities().getFirst().getObjectId());
            c.tx().inTransaction(em -> {
                em.createNativeQuery("INSERT INTO repository_reader_incarnations(incarnation,state) VALUES(:reader,'ACTIVE')")
                        .setParameter("reader", reader).executeUpdate();
                int inserted = em.createNativeQuery("""
                        INSERT INTO document_read_pins(pin_id,reader_incarnation,object_id,source_node,source_revision,publication_revision,read_scope)
                        SELECT :pin,:reader,:object,r.node_id,r.revision_id,r.publication_revision,'HISTORICAL'
                        FROM document_revision_publications r JOIN document_revision_commits c USING(revision_id)
                        WHERE r.revision_id=:revision AND NOT EXISTS(
                         SELECT 1 FROM document_revision_current current_revision WHERE current_revision.revision_id=r.revision_id)
                        """).setParameter("pin", pin).setParameter("reader", reader).setParameter("object", object)
                        .setParameter("revision", revision).executeUpdate();
                assertThat(inserted).isEqualTo(1);
            });
            long mirrors = c.tx().readOnly(em -> ((Number) em.createNativeQuery("""
                    SELECT count(*) FROM repository_object_references
                    WHERE object_id=:object AND owner_kind='DOCUMENT_READER' AND owner_id=:pin
                    """).setParameter("object", object).setParameter("pin", pin).getSingleResult()).longValue());
            assertThat(mirrors).isEqualTo(1);
            c.tx().inTransaction(em -> {
                em.createNativeQuery("SELECT release_document_read_pin(:pin,:reader,:object)")
                        .setParameter("pin", pin).setParameter("reader", reader).setParameter("object", object).getSingleResult();
            });
            assertThat(count(c, "document_read_pins")).isZero();
        }
    }

    @Test void existingCurrentPinKeepsItsIdentityAndCanReleaseAfterUpgrade() {
        try (var c = context(POSTGRES, "62")) {
            var f = prepare(c, 1);
            var source = f.sources().getFirst();
            UUID reader = UUID.randomUUID(), pin = UUID.randomUUID();
            UUID object = UUID.fromString(source.identities().getFirst().getObjectId());
            c.tx().inTransaction(em -> {
                em.createNativeQuery("INSERT INTO repository_reader_incarnations(incarnation,state) VALUES(:reader,'ACTIVE')")
                        .setParameter("reader", reader).executeUpdate();
                em.createNativeQuery("""
                        INSERT INTO document_read_pins(pin_id,reader_incarnation,object_id,source_node,source_revision,publication_revision)
                        SELECT :pin,:reader,:object,node_id,revision_id,publication_revision
                        FROM document_revision_publications WHERE revision_id=:revision
                        """).setParameter("pin", pin).setParameter("reader", reader).setParameter("object", object)
                        .setParameter("revision", source.attempt()).executeUpdate();
            });
            String schema = c.tx().readOnly(em -> (String) em.createNativeQuery("SELECT current_schema()").getSingleResult());
            Flyway.configure().dataSource(c.pool().getJdbcUrl(), c.pool().getUsername(), c.pool().getPassword())
                    .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo").target("63").load().migrate();
            Object[] retained = c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                    SELECT p.read_scope,p.reader_incarnation,p.object_id,p.source_revision,
                     r.owner_revision=p.publication_revision
                    FROM document_read_pins p JOIN repository_object_references r
                     ON r.owner_kind='DOCUMENT_READER' AND r.owner_id=p.pin_id AND r.object_id=p.object_id
                    WHERE p.pin_id=:pin
                    """).setParameter("pin", pin).getSingleResult());
            assertThat(retained).containsExactly("CURRENT", reader, object, source.attempt(), true);
            for (int retry = 0; retry < 2; retry++) c.tx().inTransaction(em -> {
                assertThat(em.createNativeQuery("SELECT release_document_read_pin(:pin,:reader,:object)")
                        .setParameter("pin", pin).setParameter("reader", reader).setParameter("object", object).getSingleResult())
                        .isEqualTo(true);
            });
            assertThat(count(c, "document_read_pins")).isZero();
            long mirrors = c.tx().readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM repository_object_references WHERE owner_kind='DOCUMENT_READER' AND owner_id=:pin")
                    .setParameter("pin", pin).getSingleResult()).longValue());
            assertThat(mirrors).isZero();
        }
    }
}
