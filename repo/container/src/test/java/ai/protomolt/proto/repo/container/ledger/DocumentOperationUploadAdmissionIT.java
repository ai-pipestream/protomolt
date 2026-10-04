package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Typed SQL admission using synthetic declarations; no provider bytes or authorization are claimed. */
@Testcontainers
class DocumentOperationUploadAdmissionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static LedgerDatabase database;
    private static Tx tx;
    private static DriveLedger drives;
    private static RepositoryOperationLedger operations;
    private static DocumentOperationUploadAdmission admission;
    private static final Duration LEASE = Duration.ofMinutes(5);
    private static final String SHA = "a".repeat(64);

    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory()); drives = new DriveLedger(tx);
        operations = new RepositoryOperationLedger(tx); admission = new DocumentOperationUploadAdmission(tx, drives);
    }
    @AfterAll static void close() { if (database != null) database.close(); }

    private record Fixture(DocumentPublicationCommand command, RepositoryOperationLedger.Owner owner,
            DriveRecord drive, DocumentUploadPlan.Placement placement, UUID attempt) {
        Map<UUID, DocumentUploadPlan.Placement> placements() { return Map.of(drive.driveId, placement); }
        DocumentOperationUploadAdmission.Prepared prepare() {
            boolean upload = command.intent().getMembers(0).getPartsList().stream().anyMatch(p -> p.hasUpload());
            return DocumentOperationUploadAdmission.prepare(command, placements(), upload ? Map.of("member", attempt) : Map.of(), LEASE);
        }
    }

    @ParameterizedTest @ValueSource(ints = {1, 513})
    void typedAdmissionStoresOnlyNewSlotsWithExactOwnerAndBoundedBatchCalls(int chunks) {
        var f = fixture(chunks);
        var prepared = f.prepare();
        var statistics = database.entityManagerFactory().unwrap(org.hibernate.SessionFactory.class).getStatistics();
        statistics.setStatisticsEnabled(true); statistics.clear();
        java.util.List<DocumentPartAttemptLedger.Attempt> result;
        try {
            result = admission.admit(f.owner, prepared);
            assertThat(statistics.getTransactionCount()).isEqualTo(1);
            // owner+command+drive+profile+final lease/fence; then INSERT/reserve/seal/read and batches.
            assertThat(statistics.getPrepareStatementCount()).isEqualTo(6 + 4 + (chunks + 255) / 256 + 1);
        } finally { statistics.setStatisticsEnabled(false); }
        assertThat(result).hasSize(1);
        assertThat(result.getFirst().planKind()).isEqualTo("NEW_CONTENT");
        assertThat(result.getFirst().sampledRevision()).isEqualTo(9);
        var row = tx.readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT operation_principal,operation_id,operation_generation,member_id,drive_id,source_count
                FROM document_part_attempts WHERE attempt_id=:id
                """).setParameter("id", f.attempt).getSingleResult());
        assertThat(row).containsExactly("principal", f.owner.key().operationId(), 1L, "member", f.drive.driveId, 1);
        java.util.List<?> slots = tx.readOnly(em -> em.createNativeQuery("""
                SELECT ordinal,revision_ordinal,part,expected_size,object_key FROM document_part_attempt_objects
                WHERE attempt_id=:id ORDER BY ordinal
                """).setParameter("id", f.attempt).getResultList());
        assertThat(slots).hasSize(chunks);
        for (int i = 0; i < chunks; i++) {
            var values = (Object[]) slots.get(i);
            assertThat(values[0]).isEqualTo(i); assertThat(values[1]).isEqualTo(i + 1);
            assertThat(values[2]).isEqualTo(3); assertThat(values[3]).isEqualTo(9007199254740993L);
            assertThat((String) values[4]).endsWith("/part-" + (i + 1)).startsWith("original/documents/account/");
        }
        long unverified = tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM document_part_attempt_objects WHERE attempt_id=:id AND NOT verified")
                .setParameter("id", f.attempt).getSingleResult()).longValue());
        assertThat(unverified).isEqualTo(chunks);
    }

    @Test void zeroUploadsStillChecksPlacementAndCreatesNoAttempt() {
        var f = fixture(0);
        assertThat(admission.admit(f.owner, f.prepare())).isEmpty();
        assertThat(new DocumentPartAttemptLedger(tx).find(f.attempt)).isEmpty();
        tx.inTransaction(em -> { em.createNativeQuery("UPDATE drives SET prefix='changed' WHERE drive_id=:id")
                .setParameter("id", f.drive.driveId).executeUpdate(); });
        assertThatThrownBy(() -> admission.admit(f.owner, f.prepare())).hasMessageContaining("drive changed");
    }

    @Test void changedCanonicalCommandCannotUseAnExistingOwner() {
        var f = fixture(1);
        var changed = new DocumentPublicationCommand(f.command.intent().toBuilder().setMembers(0,
                f.command.intent().getMembers(0).toBuilder().putMetadata("purpose", "different")).build());
        var prepared = DocumentOperationUploadAdmission.prepare(changed, f.placements(), Map.of("member", f.attempt), LEASE);
        assertThatThrownBy(() -> admission.admit(f.owner, prepared))
                .isInstanceOf(RepositoryOperationLedger.CommandConflictException.class);
        assertThat(new DocumentPartAttemptLedger(tx).find(f.attempt)).isEmpty();
    }

    @ParameterizedTest @ValueSource(strings = {"drive", "profile", "missing", "owner"})
    void staleSelectionOrOwnerCannotCreateAttempt(String change) {
        var f = fixture(1);
        var placements = f.placements(); var owner = f.owner;
        if (change.equals("drive")) tx.inTransaction(em -> { em.createNativeQuery("UPDATE drives SET prefix='changed' WHERE drive_id=:id")
                .setParameter("id", f.drive.driveId).executeUpdate(); });
        if (change.equals("profile")) placements = Map.of(f.drive.driveId, DocumentUploadPlan.Placement.sample(f.drive,
                f.placement.generation(), new ManagedBackendLedger.Profile(f.placement.profile().identity(), "other-realm")));
        if (change.equals("missing")) placements = Map.of(f.drive.driveId, DocumentUploadPlan.Placement.sample(f.drive,
                "unregistered", f.placement.profile()));
        if (change.equals("owner")) owner = new RepositoryOperationLedger.Owner(owner.key(), owner.generation(), UUID.randomUUID(), owner.leaseUntil());
        var prepared = DocumentOperationUploadAdmission.prepare(f.command, placements, Map.of("member", f.attempt), LEASE);
        var selectedOwner = owner;
        assertThatThrownBy(() -> admission.admit(selectedOwner, prepared)).satisfies(failure -> {
            switch (change) {
                case "drive" -> assertThat(failure).hasMessageContaining("drive changed");
                case "profile" -> assertThat(failure).hasMessageContaining("profile differs");
                case "missing" -> assertThat(failure).hasMessageContaining("not registered");
                default -> assertThat(failure).isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
            }
        });
        assertThat(new DocumentPartAttemptLedger(tx).find(f.attempt)).isEmpty();
    }

    @Test void duplicateAttemptIsNotSilentlyAdopted() {
        var f = fixture(1); var prepared = f.prepare();
        var original = admission.admit(f.owner, prepared).getFirst();
        assertThatThrownBy(() -> admission.admit(f.owner, prepared)).hasStackTraceContaining("duplicate key");
        assertThat(new DocumentPartAttemptLedger(tx).find(f.attempt).orElseThrow()).isEqualTo(original);
    }

    @Test void laterMemberDatabaseFailureRollsBackEveryUploadAndReservation() {
        var f = fixture(1); UUID secondAttempt = UUID.randomUUID();
        var member = f.command.intent().getMembers(0);
        var intent = f.command.intent().toBuilder().setOperationId(UUID.randomUUID().toString()).addMembers(member.toBuilder()
                .setMemberId("second").setDestination(member.getDestination().toBuilder().setAddress(address("second")))).build();
        var command = new DocumentPublicationCommand(intent);
        var key = new RepositoryOperationLedger.Key("account", "principal", command.operationId());
        var owner = operations.admit(key, command, UUID.randomUUID(), LEASE).owner().orElseThrow();
        var prepared = DocumentOperationUploadAdmission.prepare(command, f.placements(), Map.of("member", f.attempt, "second", secondAttempt), LEASE);
        tx.inTransaction(em -> {
            em.createNativeQuery("CREATE FUNCTION fail_bound_admission() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.attempt_id='"
                    + secondAttempt + "' THEN RAISE EXCEPTION 'injected second member failure'; END IF; RETURN NEW; END; $$").executeUpdate();
            em.createNativeQuery("CREATE TRIGGER zz_fail_bound_admission AFTER INSERT ON document_part_attempt_objects FOR EACH ROW EXECUTE FUNCTION fail_bound_admission()")
                    .executeUpdate();
        });
        try {
            assertThatThrownBy(() -> admission.admit(owner, prepared)).hasStackTraceContaining("injected second member failure");
            for (UUID id : java.util.List.of(f.attempt, secondAttempt)) {
                assertThat(new DocumentPartAttemptLedger(tx).find(id)).isEmpty();
                long catalog = tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM repository_physical_locations WHERE source_id=:id")
                        .setParameter("id", id).getSingleResult()).longValue());
                long reserved = tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM document_part_key_reservations WHERE attempt_id=:id")
                        .setParameter("id", id).getSingleResult()).longValue());
                assertThat(catalog).isZero(); assertThat(reserved).isZero();
            }
        } finally { tx.inTransaction(em -> { em.createNativeQuery("DROP FUNCTION fail_bound_admission() CASCADE").executeUpdate(); }); }
    }

    @Test void earlierAttemptExpiryDuringLaterAdmissionRollsBackWholeBatch() {
        var f = fixture(1); UUID secondAttempt = UUID.randomUUID();
        var member = f.command.intent().getMembers(0);
        var command = new DocumentPublicationCommand(f.command.intent().toBuilder().setOperationId(UUID.randomUUID().toString())
                .addMembers(member.toBuilder().setMemberId("second").setDestination(member.getDestination().toBuilder()
                        .setAddress(address("second")))).build());
        var key = new RepositoryOperationLedger.Key("account", "principal", command.operationId());
        var owner = operations.admit(key, command, UUID.randomUUID(), LEASE).owner().orElseThrow();
        var prepared = DocumentOperationUploadAdmission.prepare(command, f.placements(), Map.of("member", f.attempt, "second", secondAttempt), Duration.ofSeconds(1));
        tx.inTransaction(em -> {
            em.createNativeQuery("CREATE FUNCTION delay_first_admission() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.attempt_id='"
                    + f.attempt + "' AND NEW.state='STAGING' THEN PERFORM pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM NEW.lease_until-clock_timestamp()))+0.02);"
                    + " END IF; RETURN NEW; END; $$").executeUpdate();
            em.createNativeQuery("CREATE TRIGGER zz_delay_first_admission AFTER UPDATE OF state ON document_part_attempts FOR EACH ROW EXECUTE FUNCTION delay_first_admission()")
                    .executeUpdate();
        });
        try {
            assertThatThrownBy(() -> admission.admit(owner, prepared)).hasMessageContaining("attempt expired before admission completed");
            assertThat(new DocumentPartAttemptLedger(tx).find(f.attempt)).isEmpty();
            assertThat(new DocumentPartAttemptLedger(tx).find(secondAttempt)).isEmpty();
        } finally { tx.inTransaction(em -> { em.createNativeQuery("DROP FUNCTION delay_first_admission() CASCADE").executeUpdate(); }); }
    }

    private static Fixture fixture(int chunks) {
        var drive = new DriveRecord(); drive.driveId = UUID.randomUUID(); drive.accountId = "account"; drive.name = "drive-" + drive.driveId;
        drive.driveType = "CUSTOM"; drive.provider = "test-location"; drive.bucket = "namespace"; drive.prefix = "original";
        drives.insert(drive);
        var profile = new ManagedBackendLedger.Profile(new BackendIdentity("test-location", "test-location/v1",
                Map.of("endpoint", "synthetic-sql-fixture")), "realm");
        String generation = "backend-" + UUID.randomUUID(); new ManagedBackendLedger(tx).bind(generation, profile);
        var slot = DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE).build();
        var member = DocumentPublicationMember.newBuilder().setMemberId("member").setDriveId(drive.driveId.toString())
                .setDestination(DocumentRevisionCondition.newBuilder().setAddress(address("destination")).setExpectedMutationRevision(9))
                .setOwnership(OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source").setSecurity(DocumentSecurity.getDefaultInstance()))
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                .addParts(DocumentPublicationPart.newBuilder().setSlot(slot).setReuse(PublicationReuse.newBuilder()
                        .setSource(DocumentRevisionCondition.newBuilder().setAddress(address("source")).setExpectedMutationRevision(7)).setSourceSlot(slot)
                        .setObject(PublicationObjectIdentity.newBuilder().setObjectId(UUID.randomUUID().toString())
                                .setBackendGeneration("original").setStorageRealm("original-realm").setNamespace("original-namespace")
                                .setObjectKey("original-key").setSizeBytes(1).setSha256(SHA).setContentType("application/protobuf"))));
        for (int i = 0; i < chunks; i++) member.addParts(DocumentPublicationPart.newBuilder()
                .setSlot(DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CHUNKS).setSubKey("chunk-" + i))
                .setUpload(PublicationUpload.newBuilder().setSizeBytes(9007199254740993L).setSha256(SHA).setContentType("application/protobuf")));
        var command = new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1).setAccountId("account")
                .setOperationId(UUID.randomUUID().toString()).addMembers(member).build());
        var key = new RepositoryOperationLedger.Key("account", "principal", command.operationId());
        var owner = operations.admit(key, command, UUID.randomUUID(), LEASE).owner().orElseThrow();
        return new Fixture(command, owner, drive, DocumentUploadPlan.Placement.sample(drive, generation, profile), UUID.randomUUID());
    }

    private static NodeAddress address(String id) {
        return NodeAddress.newBuilder().setDocId(id).setGraphAddressId("node").setGraphId("graph").setAccountId("account").build();
    }
}
