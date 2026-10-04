package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.*;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** SQL identity/retention evidence only. No provider or immutable-version qualification is simulated. */
@Testcontainers
class DocumentReuseAdmissionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static LedgerDatabase database;
    private static Tx tx;
    private static final long CORE_SIZE = 9007199254740993L;

    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
    }
    @AfterAll static void close() { if (database != null) database.close(); }
    private record Fixture(DriveRecord drive, String generation, ManagedBackendLedger.Profile profile, ManagedDocumentFixture source) {}

    @ParameterizedTest @ValueSource(ints = {1, 256, 257, 513})
    void provesExactSlotsWithBoundedStatementsAndInt64Sizes(int count) {
        var f = fixture(count, "v1");
        var plan = plan(f, member(f), List.of());
        assertThat(plan.members().getFirst().intent().getParts(0).getReuse().getObject().getSizeBytes()).isEqualTo(CORE_SIZE);
        validate(plan, 1 + (count + 255) / 256);
    }

    @Test void duplicateClaimsAcrossMembersShareOneProof() {
        var f = fixture(1, "v1");
        var first = member(f);
        var second = first.toBuilder().setMemberId("second").setDestination(DocumentRevisionCondition.newBuilder()
                .setAddress(address("other-destination")).setIfAbsent(true)).build();
        validate(plan(f, first, List.of(second)), 2);
    }

    @ParameterizedTest @ValueSource(ints = {256, 257})
    void batchesDistinctSourceBodiesAsWellAsClaims(int count) {
        var f = fixture(1, "v1");
        var member = member(f).toBuilder();
        for (int i = 1; i < count; i++) {
            var source = ManagedDocumentFixture.publish(tx, f.drive, f.generation, f.profile,
                    address("source-" + UUID.randomUUID()), DocumentSecurity.getDefaultInstance(), 2, 1, "v1");
            member.addParts(DocumentPublicationPart.newBuilder().setSlot(source.slots().get(1)).setReuse(PublicationReuse.newBuilder()
                    .setSource(DocumentRevisionCondition.newBuilder().setAddress(source.row().readManifest().getAddress())
                            .setExpectedMutationRevision(source.row().mutationRevision))
                    .setSourceSlot(source.slots().get(1)).setObject(source.identities().get(1))));
        }
        validate(plan(f, member.build(), List.of()), 2 * ((count + 255) / 256));
    }

    @Test void contradictoryIdentitiesForOneSourceSlotAreRefusedBeforeSql() {
        var f = fixture(1, "v1");
        var first = member(f);
        var second = first.toBuilder().setMemberId("second").setDestination(DocumentRevisionCondition.newBuilder()
                .setAddress(address("other-destination")).setIfAbsent(true));
        var part = second.getParts(0);
        second.setParts(0, part.toBuilder().setReuse(part.getReuse().toBuilder().setObject(part.getReuse().getObject().toBuilder()
                .setSha256("b".repeat(64)))));
        var plan = plan(f, first, List.of(second.build()));
        assertThatThrownBy(() -> DocumentReuseAdmission.prepare(plan)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Contradictory retained object identities");
    }

    @Test void policyOnlyRevisionChangeDoesNotRequireOriginalPublicationRevision() {
        var f = fixture(1, "v1");
        tx.inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security='{}'::jsonb WHERE node_id=:id")
                .setParameter("id", f.source.row().nodeId).executeUpdate(); });
        var current = new DocumentLedger(tx).findByNodeId(f.source.row().nodeId).orElseThrow();
        assertThat(current.mutationRevision).isGreaterThan(f.source.row().mutationRevision);
        var refreshed = new Fixture(f.drive, f.generation, f.profile, new ManagedDocumentFixture(current,
                f.source.attempt(), f.source.slots(), f.source.identities()));
        validate(plan(refreshed, member(refreshed), List.of()), 2);
    }

    @Test void originalIdentitySurvivesCurrentDriveConfigurationChange() {
        var f = fixture(1, "v1");
        tx.inTransaction(em -> { em.createNativeQuery("UPDATE drives SET bucket='replacement',prefix='changed' WHERE drive_id=:id")
                .setParameter("id", f.drive.driveId).executeUpdate(); });
        // Only tests the source proof. Destination placement is independently rechecked by admission.
        validate(plan(f, member(f), List.of()), 2);
    }

    @Test void absentProviderVersionIsComparedExactlyWithoutQualifyingTheProvider() {
        var f = fixture(1, null);
        var member = member(f);
        assertThat(member.getParts(0).getReuse().getObject().hasProviderVersion()).isFalse();
        validate(plan(f, member, List.of()), 2);
        var part = member.getParts(0);
        var inventedVersion = member.toBuilder().setParts(0, part.toBuilder().setReuse(part.getReuse().toBuilder()
                .setObject(part.getReuse().getObject().toBuilder().setProviderVersion("invented")))).build();
        assertThatThrownBy(() -> validate(plan(f, inventedVersion, List.of()), -1))
                .isInstanceOf(DocumentPartAttemptLedger.FenceException.class);
    }

    @Test void validForeignObjectCannotReplaceTheSelectedSourceSlot() {
        var f = fixture(1, "v1"); var other = fixture(1, "v1");
        var member = member(f); var part = member.getParts(0);
        var forged = member.toBuilder().setParts(0, part.toBuilder().setReuse(part.getReuse().toBuilder()
                .setObject(other.source.identities().getFirst()))).build();
        assertThatThrownBy(() -> validate(plan(f, forged, List.of()), -1))
                .isInstanceOf(DocumentPartAttemptLedger.FenceException.class);
    }

    @Test void retainedHistoricalIdentityCannotAuthorizeAnUnboundCurrentRow() {
        var f = fixture(1, "v1");
        var documents = new DocumentLedger(tx);
        documents.deleteByNodeId(f.source.row().nodeId);
        var legacy = f.source.row();
        legacy.partManifest = null; legacy.objectKey = "legacy/" + UUID.randomUUID();
        legacy.versionId = null; legacy.etag = "legacy";
        var restored = documents.save(legacy);
        var changed = new Fixture(f.drive, f.generation, f.profile, new ManagedDocumentFixture(restored,
                f.source.attempt(), f.source.slots(), f.source.identities()));
        assertThatThrownBy(() -> validate(plan(changed, member(changed), List.of()), -1))
                .isInstanceOf(DocumentPartAttemptLedger.FenceException.class);
        long history = tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM repository_object_references WHERE owner_kind='DOCUMENT_HISTORY' AND owner_id=:id")
                .setParameter("id", f.source.attempt()).getSingleResult()).longValue());
        assertThat(history).isEqualTo(1);
        // An explicit dependency on that same legacy row claims no physical object.
        var reference = member(changed).getParts(0).getReuse().getSource();
        var explicit = member(changed).toBuilder().clearParts().addSources(reference).addParts(DocumentPublicationPart.newBuilder()
                .setSlot(DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                .setUpload(PublicationUpload.newBuilder().setSizeBytes(1).setSha256("a".repeat(64)).setContentType("application/protobuf"))).build();
        validate(plan(changed, explicit, List.of()), 0);
    }

    @Test void sqlGuardsPreventDroppingRetentionOrReclaimingProvenSource() {
        var f = fixture(1, "v1"); UUID object = UUID.fromString(f.source.identities().getFirst().getObjectId());
        assertThatThrownBy(() -> tx.inTransaction(em -> { em.createNativeQuery("DELETE FROM repository_object_references WHERE object_id=:id")
                .setParameter("id", object).executeUpdate(); })).hasStackTraceContaining("cannot release a retained native owner");
        assertThatThrownBy(() -> tx.inTransaction(em -> { em.createNativeQuery("UPDATE repository_object_retention SET reclaiming=true WHERE object_id=:id")
                .setParameter("id", object).executeUpdate(); })).hasStackTraceContaining("Retained repository objects cannot be reclaimed");
        validate(plan(f, member(f), List.of()), 2);
    }

    private static void validate(DocumentUploadPlan.Prepared plan, int expectedCalls) {
        var prepared = DocumentReuseAdmission.prepare(plan);
        var authorization = DocumentAdmissionAuthorization.prepare(plan);
        var statistics = database.entityManagerFactory().unwrap(org.hibernate.SessionFactory.class).getStatistics();
        statistics.setStatisticsEnabled(true);
        try {
            tx.inTransaction(em -> {
                DocumentAdmissionAuthorization.lockAndAuthorize(em, new RepositoryCaller("operator", true), plan, authorization);
                long before = statistics.getPrepareStatementCount();
                DocumentReuseAdmission.requireBoundSources(em, prepared);
                if (expectedCalls >= 0) assertThat(statistics.getPrepareStatementCount() - before).isEqualTo(expectedCalls);
            });
        } finally { statistics.setStatisticsEnabled(false); }
    }

    private static DocumentUploadPlan.Prepared plan(Fixture f, DocumentPublicationMember member, List<DocumentPublicationMember> extra) {
        var command = new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1).setAccountId("account")
                .setOperationId(UUID.randomUUID().toString()).addMembers(member).addAllMembers(extra).build());
        boolean upload = member.getPartsList().stream().anyMatch(DocumentPublicationPart::hasUpload);
        return DocumentUploadPlan.prepare(command, Map.of(f.drive.driveId, DocumentUploadPlan.Placement.sample(f.drive, f.generation, f.profile)),
                upload ? Map.of(member.getMemberId(), UUID.randomUUID()) : Map.of());
    }

    private static DocumentPublicationMember member(Fixture f) {
        var source = DocumentRevisionCondition.newBuilder().setAddress(f.source.row().readManifest() == null
                ? address(f.source.row().docId) : f.source.row().readManifest().getAddress())
                .setExpectedMutationRevision(f.source.row().mutationRevision);
        var member = DocumentPublicationMember.newBuilder().setMemberId("member").setDriveId(f.drive.driveId.toString())
                .setDestination(DocumentRevisionCondition.newBuilder().setAddress(address("destination-" + UUID.randomUUID())).setIfAbsent(true))
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                .setOwnership(OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source").setSecurity(DocumentSecurity.getDefaultInstance()));
        for (int i = 0; i < f.source.slots().size(); i++) member.addParts(DocumentPublicationPart.newBuilder().setSlot(f.source.slots().get(i))
                .setReuse(PublicationReuse.newBuilder().setSource(source).setSourceSlot(f.source.slots().get(i)).setObject(f.source.identities().get(i))));
        return member.build();
    }
    private static Fixture fixture(int parts, String version) {
        var drive = new DriveRecord(); drive.driveId = UUID.randomUUID(); drive.accountId = "account"; drive.name = "source-" + drive.driveId;
        drive.driveType = "CUSTOM"; drive.provider = "test-location"; drive.bucket = "original"; drive.prefix = "prefix";
        new DriveLedger(tx).insert(drive);
        String generation = "source-" + UUID.randomUUID();
        var profile = new ManagedBackendLedger.Profile(new BackendIdentity("test-location", "test-location/v1", Map.of("endpoint", "synthetic")), "realm");
        new ManagedBackendLedger(tx).bind(generation, profile);
        var source = ManagedDocumentFixture.publish(tx, drive, generation, profile, address("source-" + UUID.randomUUID()),
                DocumentSecurity.getDefaultInstance(), parts, CORE_SIZE, version);
        return new Fixture(drive, generation, profile, source);
    }
    private static NodeAddress address(String doc) {
        return NodeAddress.newBuilder().setAccountId("account").setGraphId("graph").setGraphAddressId("node").setDocId(doc).build();
    }
}
