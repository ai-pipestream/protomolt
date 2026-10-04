package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.container.blob.DocumentIds;
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

/** Real SQL policy and staging checks; synthetic byte declarations are not verified provider content. */
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
    private static final RepositoryCaller ADMIN = new RepositoryCaller("principal", true);
    private static final RepositoryCaller SCOPED = new RepositoryCaller("principal", false, java.util.Set.of("account"), java.util.Set.of());
    private static final DocumentSecurity POLICY = DocumentSecurity.newBuilder()
            .addPermissions(AccessRule.newBuilder().setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_READ))
            .addPermissions(AccessRule.newBuilder().setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_WRITE)).build();

    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory()); drives = new DriveLedger(tx);
        operations = new RepositoryOperationLedger(tx); admission = new DocumentOperationUploadAdmission(tx, drives);
    }
    @AfterAll static void close() { if (database != null) database.close(); }

    private record Fixture(DocumentPublicationCommand command, RepositoryOperationLedger.Owner owner,
            DriveRecord drive, DocumentUploadPlan.Placement placement, UUID attempt,
            DocumentRecord destination, DocumentRecord source) {
        Map<UUID, DocumentUploadPlan.Placement> placements() { return Map.of(drive.driveId, placement); }
        DocumentOperationUploadAdmission.Prepared prepare() {
            boolean upload = command.intent().getMembers(0).getPartsList().stream().anyMatch(p -> p.hasUpload());
            return DocumentOperationUploadAdmission.prepare(command, placements(), upload ? Map.of("member", attempt) : Map.of(), LEASE);
        }
    }

    @ParameterizedTest @ValueSource(strings = {"missing", "unbound", "other-account", "other-principal", "admin-other-principal"})
    void invalidCallerCannotReachSql(String kind) {
        var f = fixture(1); var prepared = f.prepare();
        RepositoryCaller caller = switch (kind) {
            case "missing" -> null;
            case "unbound" -> new RepositoryCaller("principal", false);
            case "other-account" -> new RepositoryCaller("principal", false, java.util.Set.of("other"), java.util.Set.of());
            case "other-principal" -> new RepositoryCaller("other", false, java.util.Set.of("account"), java.util.Set.of());
            default -> new RepositoryCaller("other", true);
        };
        var statistics = database.entityManagerFactory().unwrap(org.hibernate.SessionFactory.class).getStatistics();
        statistics.setStatisticsEnabled(true); statistics.clear();
        try {
            assertThatThrownBy(() -> admission.admit(caller, f.owner, prepared)).isInstanceOfSatisfying(RepositoryException.class,
                    failure -> assertThat(failure.code()).isEqualTo(kind.equals("other-account")
                            ? RepositoryException.Code.NOT_FOUND : RepositoryException.Code.PERMISSION_DENIED));
            assertThat(statistics.getPrepareStatementCount()).isZero();
        } finally { statistics.setStatisticsEnabled(false); }
        assertNoAttempt(f.attempt);
    }

    @ParameterizedTest @ValueSource(strings = {"destination", "source"})
    void deniedPolicyMasksStaleRevisionAndDoesNotStage(String which) {
        var f = fixture(1);
        var row = which.equals("source") ? f.source : f.destination;
        var denied = POLICY.toBuilder().addPermissions(AccessRule.newBuilder().setIdentityType("public")
                .setIdentity("public").setAccess(Access.ACCESS_DENY)).build();
        setPolicy(row, denied);
        assertThatThrownBy(() -> admission.admit(SCOPED, f.owner, f.prepare()))
                .isInstanceOfSatisfying(RepositoryException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND);
                    assertThat(failure).hasMessage("Document is unavailable");
                });
        assertNoAttempt(f.attempt);
    }

    @ParameterizedTest @ValueSource(strings = {"destination", "source"})
    void changedAuthorizedRevisionCannotStage(String which) {
        var f = fixture(1);
        var row = which.equals("source") ? f.source : f.destination;
        tx.inTransaction(em -> { em.createNativeQuery("UPDATE documents SET filename='changed' WHERE node_id=:id")
                .setParameter("id", row.nodeId).executeUpdate(); });
        assertThatThrownBy(() -> admission.admit(SCOPED, f.owner, f.prepare()))
                .isInstanceOf(DocumentLedger.RevisionConflictException.class);
        assertNoAttempt(f.attempt);
    }

    @Test void scopedCallerCanStageAgainstCurrentReadAndWriteGrants() {
        var f = fixture(1);
        assertThat(admission.admit(SCOPED, f.owner, f.prepare())).hasSize(1);
    }

    @ParameterizedTest @ValueSource(strings = {"uuid", "generation", "realm", "namespace", "key", "version", "missing-version", "size", "sha", "type", "slot"})
    void forgedReuseClaimCannotStage(String field) {
        var f = fixture(1);
        var member = f.command.intent().getMembers(0).toBuilder();
        var reuse = member.getParts(0).getReuse().toBuilder();
        var identity = reuse.getObject().toBuilder();
        switch (field) {
            case "uuid" -> identity.setObjectId(UUID.randomUUID().toString());
            case "generation" -> identity.setBackendGeneration("other");
            case "realm" -> identity.setStorageRealm("other");
            case "namespace" -> identity.setNamespace("other");
            case "key" -> identity.setObjectKey("other");
            case "version" -> identity.setProviderVersion("other");
            case "missing-version" -> identity.clearProviderVersion();
            case "size" -> identity.setSizeBytes(2);
            case "sha" -> identity.setSha256("b".repeat(64));
            case "type" -> identity.setContentType("text/plain");
            default -> reuse.setSourceSlot(DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CHUNKS).setSubKey("missing"));
        }
        var requestedPart = member.getParts(0).toBuilder().setReuse(reuse.setObject(identity));
        if (field.equals("slot")) {
            // Keep target/source slots equal and retain a CORE, so this reaches
            // the database proof rather than failing command shape validation.
            member.addParts(member.getParts(0));
            requestedPart.setSlot(reuse.getSourceSlot());
        }
        member.setParts(0, requestedPart);
        var changed = rebind(f, member.build());
        assertThatThrownBy(() -> admission.admit(SCOPED, changed.owner, changed.prepare()))
                .isInstanceOf(DocumentPartAttemptLedger.FenceException.class).hasMessageContaining("retained current managed source binding");
        assertNoAttempt(changed.attempt);
    }

    @Test void deniedDestinationTakesPrecedenceOverAuthorizedButStaleSource() {
        var f = fixture(1);
        tx.inTransaction(em -> { em.createNativeQuery("UPDATE documents SET filename='changed' WHERE node_id=:id")
                .setParameter("id", f.source.nodeId).executeUpdate(); });
        setPolicy(f.destination, DocumentSecurity.getDefaultInstance());
        assertThatThrownBy(() -> admission.admit(SCOPED, f.owner, f.prepare()))
                .isInstanceOfSatisfying(RepositoryException.class,
                        failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
        assertNoAttempt(f.attempt);
    }

    @Test void scopedCreationRequiresAnExplicitFutureGrantButAdminIsExplicit() {
        var f = fixture(1);
        var member = f.command.intent().getMembers(0).toBuilder().setDestination(DocumentRevisionCondition.newBuilder()
                .setAddress(address("new-" + UUID.randomUUID())).setIfAbsent(true));
        var changed = rebind(f, member.build());
        assertThatThrownBy(() -> admission.admit(SCOPED, changed.owner, changed.prepare()))
                .isInstanceOfSatisfying(RepositoryException.class,
                        failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
        assertNoAttempt(changed.attempt);
        assertThat(admission.admit(ADMIN, changed.owner, changed.prepare())).hasSize(1);
    }

    @ParameterizedTest @ValueSource(strings = {"acl", "datasource", "drive", "deletion"})
    void scopedWritesCannotChangeProtectedOwnershipAndPlacement(String field) {
        var f = fixture(1);
        var member = f.command.intent().getMembers(0).toBuilder();
        switch (field) {
            case "acl" -> member.setOwnership(member.getOwnership().toBuilder().setSecurity(DocumentSecurity.getDefaultInstance()));
            case "datasource" -> member.setOwnership(member.getOwnership().toBuilder().setDatasourceId("different"));
            case "drive" -> tx.inTransaction(em -> { em.createNativeQuery("UPDATE documents SET drive_name='different' WHERE node_id=:id")
                    .setParameter("id", f.destination.nodeId).executeUpdate(); });
            default -> tx.inTransaction(em -> { em.createNativeQuery("UPDATE documents SET delete_source_blobs_on_settle=true, source_blob_delete_reason='original' WHERE node_id=:id")
                    .setParameter("id", f.destination.nodeId).executeUpdate(); });
        }
        var changed = rebind(f, member.build());
        assertThatThrownBy(() -> admission.admit(SCOPED, changed.owner, changed.prepare()))
                .isInstanceOfSatisfying(RepositoryException.class,
                        failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
        assertNoAttempt(changed.attempt);
    }

    @Test void selfCopyStillNeedsReadInAdditionToWrite() {
        var f = fixture(1);
        var writeOnly = DocumentSecurity.newBuilder().addPermissions(POLICY.getPermissions(1)).build();
        setPolicy(f.destination, writeOnly);
        var current = new DocumentLedger(tx).findByNodeId(f.destination.nodeId).orElseThrow();
        var member = f.command.intent().getMembers(0).toBuilder();
        member.setDestination(member.getDestination().toBuilder().setExpectedMutationRevision(current.mutationRevision));
        member.setOwnership(member.getOwnership().toBuilder().setSecurity(writeOnly));
        member.setParts(0, member.getParts(0).toBuilder().setReuse(member.getParts(0).getReuse().toBuilder().setSource(member.getDestination())));
        var changed = rebind(f, member.build());
        assertThatThrownBy(() -> admission.admit(SCOPED, changed.owner, changed.prepare()))
                .isInstanceOfSatisfying(RepositoryException.class,
                        failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
        assertNoAttempt(changed.attempt);
    }

    @Test void zeroUploadCommandStillRequiresSourceRead() {
        var f = fixture(0);
        setPolicy(f.source, DocumentSecurity.getDefaultInstance());
        assertThatThrownBy(() -> admission.admit(SCOPED, f.owner, f.prepare()))
                .isInstanceOfSatisfying(RepositoryException.class,
                        failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
        assertNoAttempt(f.attempt);
    }

    @Test void explicitDependencyNeedsReadEvenWhenNoPartsAreReused() {
        var f = fixture(1);
        var member = f.command.intent().getMembers(0).toBuilder();
        member.addSources(member.getParts(0).getReuse().getSource());
        member.setParts(0, DocumentPublicationPart.newBuilder().setSlot(member.getParts(0).getSlot())
                .setUpload(PublicationUpload.newBuilder().setSha256(SHA).setSizeBytes(1).setContentType("application/protobuf")));
        var changed = rebind(f, member.build());
        setPolicy(f.source, DocumentSecurity.getDefaultInstance());
        assertThatThrownBy(() -> admission.admit(SCOPED, changed.owner, changed.prepare()))
                .isInstanceOfSatisfying(RepositoryException.class,
                        failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
        assertNoAttempt(changed.attempt);
    }

    @Test void explicitAndReuseDependencyOnSameRevisionShareOneSourceFence() {
        var f = fixture(1);
        var member = f.command.intent().getMembers(0).toBuilder();
        member.addSources(member.getParts(0).getReuse().getSource());
        var changed = rebind(f, member.build());
        assertThat(admission.admit(SCOPED, changed.owner, changed.prepare())).hasSize(1);
        long sources = tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM document_part_attempt_sources WHERE attempt_id=:id")
                .setParameter("id", f.attempt).getSingleResult()).longValue());
        assertThat(sources).isEqualTo(1);
    }

    @Test void processAuthorityCannotConcealMalformedStoredPolicy() {
        var f = fixture(1);
        tx.inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security='[]'::jsonb WHERE node_id=:id")
                .setParameter("id", f.destination.nodeId).executeUpdate(); });
        assertThatThrownBy(() -> admission.admit(ADMIN, f.owner, f.prepare()))
                .isInstanceOfSatisfying(RepositoryException.class,
                        failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
        assertNoAttempt(f.attempt);
    }

    @Test void processAuthorityCannotAdmitMalformedProposedPublicIdentity() {
        var f = fixture(1);
        var bad = POLICY.toBuilder().setPermissions(0, POLICY.getPermissions(0).toBuilder().setIdentity("someone"));
        var member = f.command.intent().getMembers(0).toBuilder();
        member.setOwnership(member.getOwnership().toBuilder().setSecurity(bad));
        var changed = rebind(f, member.build());
        assertThatThrownBy(() -> admission.admit(ADMIN, changed.owner, changed.prepare()))
                .isInstanceOfSatisfying(RepositoryException.class,
                        failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
        assertNoAttempt(changed.attempt);
    }

    @Test void deniedSourceIsRejectedBeforeWaitingForDriveLock() throws Exception {
        var f = fixture(1);
        setPolicy(f.source, DocumentSecurity.getDefaultInstance());
        try (var holder = database.entityManagerFactory().createEntityManager();
             var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            holder.getTransaction().begin();
            holder.createNativeQuery("SELECT drive_id FROM drives WHERE drive_id=:id FOR UPDATE")
                    .setParameter("id", f.drive.driveId).getSingleResult();
            var prepared = f.prepare();
            var future = executor.submit(() -> admission.admit(SCOPED, f.owner, prepared));
            try {
                assertThatThrownBy(() -> future.get(5, java.util.concurrent.TimeUnit.SECONDS))
                        .hasCauseInstanceOf(RepositoryException.class).satisfies(failure ->
                                assertThat(((RepositoryException) failure.getCause()).code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
                assertNoAttempt(f.attempt);
            } finally { holder.getTransaction().rollback(); }
        }
    }

    @Test void policyRevocationCommittedDuringRowLockWaitPreventsAdmission() throws Exception {
        var f = fixture(1);
        try (var holder = database.entityManagerFactory().createEntityManager();
             var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            holder.getTransaction().begin();
            int holderPid = ((Number) holder.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue();
            holder.createNativeQuery("UPDATE documents SET security='{}'::jsonb WHERE node_id=:id")
                    .setParameter("id", f.source.nodeId).executeUpdate();
            var prepared = f.prepare();
            var future = executor.submit(() -> admission.admit(SCOPED, f.owner, prepared));
            try {
                long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                Integer contender = null;
                do {
                    var waiting = tx.readOnly(em -> em.createNativeQuery("""
                            SELECT pid FROM pg_stat_activity WHERE :holder=ANY(pg_blocking_pids(pid))
                                AND wait_event_type='Lock'
                            """).setParameter("holder", holderPid).getResultList());
                    if (!waiting.isEmpty()) { contender = ((Number) waiting.getFirst()).intValue(); break; }
                    Thread.sleep(10);
                } while (System.nanoTime() < deadline);
                assertThat(contender).as("admission backend waiting on the policy update").isNotNull();
                holder.getTransaction().commit();
                assertThatThrownBy(() -> future.get(10, java.util.concurrent.TimeUnit.SECONDS))
                        .hasCauseInstanceOf(RepositoryException.class).satisfies(failure ->
                                assertThat(((RepositoryException) failure.getCause()).code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
                assertNoAttempt(f.attempt);
            } finally { if (holder.getTransaction().isActive()) holder.getTransaction().rollback(); }
        }
    }

    private static Fixture rebind(Fixture f, DocumentPublicationMember member) {
        var command = new DocumentPublicationCommand(f.command.intent().toBuilder().setOperationId(UUID.randomUUID().toString())
                .setMembers(0, member).build());
        var key = new RepositoryOperationLedger.Key("account", "principal", command.operationId());
        var owner = operations.admit(key, command, UUID.randomUUID(), LEASE).owner().orElseThrow();
        return new Fixture(command, owner, f.drive, f.placement, f.attempt, f.destination, f.source);
    }

    @ParameterizedTest @ValueSource(strings = {"missing", "malformed", "inherited", "write-only", "purging", "destination-identity"})
    void sourcePolicyAndDestinationIdentityCannotBeBypassedByDeclaredReuse(String kind) {
        var f = fixture(1);
        switch (kind) {
            case "missing" -> tx.inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security=NULL WHERE node_id=:id")
                    .setParameter("id", f.source.nodeId).executeUpdate(); });
            case "malformed" -> tx.inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security='[]'::jsonb WHERE node_id=:id")
                    .setParameter("id", f.source.nodeId).executeUpdate(); });
            case "inherited" -> setPolicy(f.source, POLICY.toBuilder().setInheritanceEnabled(true).build());
            case "write-only" -> setPolicy(f.source, DocumentSecurity.newBuilder().addPermissions(POLICY.getPermissions(1)).build());
            case "purging" -> tx.inTransaction(em -> { em.createNativeQuery("UPDATE documents SET status='PENDING_PURGE' WHERE node_id=:id")
                    .setParameter("id", f.source.nodeId).executeUpdate(); });
            default -> tx.inTransaction(em -> { em.createNativeQuery("UPDATE documents SET doc_id='different' WHERE node_id=:id")
                    .setParameter("id", f.destination.nodeId).executeUpdate(); });
        }
        assertThatThrownBy(() -> admission.admit(SCOPED, f.owner, f.prepare()))
                .isInstanceOfSatisfying(RepositoryException.class, failure -> assertThat(failure.code()).isEqualTo(
                        kind.equals("malformed") || kind.equals("inherited") ? RepositoryException.Code.FAILED_PRECONDITION
                                : RepositoryException.Code.NOT_FOUND));
        assertNoAttempt(f.attempt);
    }

    private static void assertNoAttempt(UUID id) {
        assertThat(new DocumentPartAttemptLedger(tx).find(id)).isEmpty();
        long catalog = tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM repository_physical_locations WHERE source_id=:id")
                .setParameter("id", id).getSingleResult()).longValue());
        long reserved = tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM document_part_key_reservations WHERE attempt_id=:id")
                .setParameter("id", id).getSingleResult()).longValue());
        assertThat(catalog).isZero(); assertThat(reserved).isZero();
    }

    private static void setPolicy(DocumentRecord row, DocumentSecurity security) {
        row.writeSecurity(security);
        tx.inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security=CAST(:security AS jsonb) WHERE node_id=:id")
                .setParameter("security", row.security).setParameter("id", row.nodeId).executeUpdate(); });
    }

    @ParameterizedTest @ValueSource(ints = {1, 513})
    void typedAdmissionStoresOnlyNewSlotsWithExactOwnerAndBoundedBatchCalls(int chunks) {
        var f = fixture(chunks);
        var prepared = f.prepare();
        var statistics = database.entityManagerFactory().unwrap(org.hibernate.SessionFactory.class).getStatistics();
        statistics.setStatisticsEnabled(true); statistics.clear();
        java.util.List<DocumentPartAttemptLedger.Attempt> result;
        try {
            result = admission.admit(ADMIN, f.owner, prepared);
            assertThat(statistics.getTransactionCount()).isEqualTo(1);
            // owner+command+revision locks+source/claim proof+drive+profile+final checks; then attempt rows and batches.
            assertThat(statistics.getPrepareStatementCount()).isEqualTo(10 + 4 + (chunks + 255) / 256 + 3);
        } finally { statistics.setStatisticsEnabled(false); }
        assertThat(result).hasSize(1);
        assertThat(result.getFirst().planKind()).isEqualTo("NEW_CONTENT");
        assertThat(result.getFirst().sampledRevision()).isEqualTo(f.destination.mutationRevision);
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
        assertThat(selection(f)).containsExactly(f.attempt, f.drive.driveId, f.placement.generation(),
                "namespace", f.drive.prefix, chunks, f.destination.mutationRevision);
    }

    @Test void zeroUploadsStillChecksPlacementAndCreatesNoAttempt() {
        var f = fixture(0);
        assertThat(admission.admit(ADMIN, f.owner, f.prepare())).isEmpty();
        assertThat(new DocumentPartAttemptLedger(tx).find(f.attempt)).isEmpty();
        assertThat(selection(f)).containsExactly(null, f.drive.driveId, f.placement.generation(),
                "namespace", f.drive.prefix, 0, f.destination.mutationRevision);
        tx.inTransaction(em -> { em.createNativeQuery("UPDATE drives SET prefix='changed' WHERE drive_id=:id")
                .setParameter("id", f.drive.driveId).executeUpdate(); });
        assertThatThrownBy(() -> admission.admit(ADMIN, f.owner, f.prepare())).hasMessageContaining("drive changed");
        assertThat(selection(f)[4]).isEqualTo("original");
    }

    @Test void separateAdmissionsSharingOneSourceCanBothReachSelection() throws Exception {
        var first=fixture(0);
        var member=first.command.intent().getMembers(0);
        var command=new DocumentPublicationCommand(first.command.intent().toBuilder().setOperationId(UUID.randomUUID().toString())
                .setMembers(0,member.toBuilder().setDestination(member.getDestination().toBuilder()
                        .setAddress(address("concurrent-"+UUID.randomUUID())).setIfAbsent(true))).build());
        var owner=operations.admit(new RepositoryOperationLedger.Key("account","principal",command.operationId()),
                command,UUID.randomUUID(),LEASE).owner().orElseThrow();
        var second=DocumentOperationUploadAdmission.prepare(command,first.placements(),Map.of(),LEASE);
        try (var blocker=database.dataSource().getConnection();
             var executor=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            blocker.setAutoCommit(false);
            int blockerPid;
            try (var statement=blocker.createStatement()) {
                statement.execute("SELECT pg_advisory_xact_lock(7301,7302)");
                try (var rows=statement.executeQuery("SELECT pg_backend_pid()")) { rows.next(); blockerPid=rows.getInt(1); }
            }
            tx.inTransaction(em -> {
                em.createNativeQuery("""
                        CREATE FUNCTION hold_first_selection() RETURNS trigger LANGUAGE plpgsql AS $$
                        BEGIN IF NEW.operation_id='%s' THEN PERFORM pg_advisory_xact_lock(7301,7302); END IF;
                        RETURN NEW; END; $$
                        """.formatted(first.command.operationId())).executeUpdate();
                em.createNativeQuery("""
                        CREATE TRIGGER zz_hold_first_selection AFTER INSERT ON document_operation_selections
                        FOR EACH ROW EXECUTE FUNCTION hold_first_selection()
                        """).executeUpdate();
            });
            var pending=executor.submit(() -> admission.admit(ADMIN,first.owner,first.prepare()));
            try {
                long deadline=System.nanoTime()+Duration.ofSeconds(5).toNanos();
                boolean waiting=false;
                do {
                    waiting=tx.readOnly(em -> !em.createNativeQuery(
                            "SELECT pid FROM pg_stat_activity WHERE :blocker=ANY(pg_blocking_pids(pid))")
                            .setParameter("blocker",blockerPid).getResultList().isEmpty());
                    if (waiting) break;
                    Thread.sleep(10);
                } while(System.nanoTime()<deadline);
                assertThat(waiting).as("first admission held after selection insertion").isTrue();
                var independent=executor.submit(() -> admission.admit(ADMIN,owner,second));
                assertThat(independent.get(5,java.util.concurrent.TimeUnit.SECONDS)).isEmpty();
                assertThat(pending.isDone()).isFalse();
                blocker.rollback();
                assertThat(pending.get(5,java.util.concurrent.TimeUnit.SECONDS)).isEmpty();
            } finally {
                blocker.rollback();
                pending.get(10,java.util.concurrent.TimeUnit.SECONDS);
                tx.inTransaction(em -> { em.createNativeQuery("DROP FUNCTION hold_first_selection() CASCADE").executeUpdate(); });
            }
        }
    }

    private static Object[] selection(Fixture f) {
        return tx.readOnly(em -> (Object[])em.createNativeQuery("""
                SELECT attempt_id,drive_id,backend_generation,storage_namespace,drive_snapshot->>'prefix',
                    upload_count,sampled_revision FROM document_operation_selections
                WHERE account_id=:account AND principal=:principal AND operation_id=:operation
                    AND owner_generation=:generation AND member_id='member'
                """).setParameter("account",f.owner.key().account()).setParameter("principal",f.owner.key().principal())
                .setParameter("operation",f.owner.key().operationId()).setParameter("generation",f.owner.generation())
                .getSingleResult());
    }

    @Test void selectedPlacementDoesNotCopyFreeFormConfigurationOrCredentialReferences() {
        var f=fixture(0);
        f.drive.metadata="{\"test\":\"synthetic-sensitive-marker\",\"bulk\":\""+"x".repeat(100_000)+"\"}";
        f.drive.credentialsRef="synthetic-credential-reference";
        tx.inTransaction(em -> { em.createNativeQuery("""
                UPDATE drives SET metadata=CAST(:metadata AS jsonb),credentials_ref=:ref WHERE drive_id=:id
                """).setParameter("metadata",f.drive.metadata).setParameter("ref",f.drive.credentialsRef)
                .setParameter("id",f.drive.driveId).executeUpdate(); });
        var storedDrive=drives.findByName(f.drive.accountId,f.drive.name).orElseThrow();
        var sampled=DocumentUploadPlan.Placement.sample(storedDrive,f.placement.generation(),f.placement.profile());
        admission.admit(ADMIN,f.owner,DocumentOperationUploadAdmission.prepare(f.command,
                Map.of(f.drive.driveId,sampled),Map.of(),LEASE));
        var retained=tx.readOnly(em -> (Object[])em.createNativeQuery("""
                SELECT s.drive_snapshot::text,octet_length(s.drive_sha256),
                    s.drive_sha256=sha256(convert_to((to_jsonb(d)-'created_at')::text,'UTF8'))
                FROM document_operation_selections s JOIN drives d USING(drive_id) WHERE s.operation_id=:id
                """).setParameter("id",f.owner.key().operationId()).getSingleResult());
        assertThat((String)retained[0]).hasSizeLessThan(2048)
                .doesNotContain("synthetic-sensitive-marker","synthetic-credential-reference","bulk","credentials_ref","provider_config","metadata");
        assertThat(retained[1]).isEqualTo(32); assertThat(retained[2]).isEqualTo(true);
        tx.inTransaction(em -> { em.createNativeQuery("UPDATE drives SET metadata='{}' WHERE drive_id=:id")
                .setParameter("id",f.drive.driveId).executeUpdate(); });
        boolean stillMatches=tx.readOnly(em -> (Boolean)em.createNativeQuery("""
                SELECT s.drive_sha256=sha256(convert_to((to_jsonb(d)-'created_at')::text,'UTF8'))
                FROM document_operation_selections s JOIN drives d USING(drive_id) WHERE s.operation_id=:id
                """).setParameter("id",f.owner.key().operationId()).getSingleResult());
        assertThat(stillMatches).isFalse();
    }

    @Test void initialSelectionCannotBeReplacedByAnotherAdmissionOrMutated() {
        var f=fixture(1);
        admission.admit(ADMIN,f.owner,f.prepare());
        var replacement=UUID.randomUUID();
        var next=DocumentOperationUploadAdmission.prepare(f.command,f.placements(),Map.of("member",replacement),LEASE);
        assertThatThrownBy(() -> admission.admit(ADMIN,f.owner,next)).hasStackTraceContaining("duplicate key");
        assertNoAttempt(replacement);
        assertThat(selection(f)[0]).isEqualTo(f.attempt);
        for (String sql : java.util.List.of("UPDATE document_operation_selections SET attempt_id=NULL WHERE operation_id=:id",
                "DELETE FROM document_operation_selections WHERE operation_id=:id")) {
            assertThatThrownBy(() -> tx.inTransaction(em -> {
                RepositoryOperationLedger.fenceLiveOwner(em,f.owner);
                em.createNativeQuery(sql).setParameter("id",f.owner.key().operationId()).executeUpdate();
            })).hasStackTraceContaining("selection is immutable");
        }
    }

    @ParameterizedTest @ValueSource(strings={"owner", "member", "node", "revision", "count", "snapshot", "digest", "namespace"})
    void directSelectionForgeryFails(String field) {
        var f=fixture(1); admission.admit(ADMIN,f.owner,f.prepare());
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            if (!field.equals("owner")) RepositoryOperationLedger.fenceLiveOwner(em,f.owner);
            String member=field.equals("member") ? "'wrong'" : "member_id";
            String node=field.equals("node") ? "gen_random_uuid()" : "node_id";
            String revision=field.equals("revision") ? "sampled_revision+1" : "sampled_revision";
            String count=field.equals("count") ? "upload_count+1" : "upload_count";
            String snapshot=field.equals("snapshot") ? "'{}'::jsonb" : "drive_snapshot";
            String namespace=field.equals("namespace") ? "'other'" : "storage_namespace";
            String digest=field.equals("digest") ? "decode(repeat('00',32),'hex')" : "drive_sha256";
            em.createNativeQuery("""
                    INSERT INTO document_operation_selections(account_id,principal,operation_id,owner_generation,
                        member_id,node_id,sampled_revision,drive_id,drive_snapshot,drive_sha256,backend_generation,
                        storage_realm,storage_namespace,upload_count,attempt_id)
                    SELECT account_id,principal,operation_id,owner_generation,%s,%s,%s,drive_id,%s,%s,backend_generation,
                        storage_realm,%s,%s,attempt_id FROM document_operation_selections WHERE operation_id=:id
                    """.formatted(member,node,revision,snapshot,digest,namespace,count))
                    .setParameter("id",f.owner.key().operationId()).executeUpdate();
        })).hasStackTraceContaining(switch(field) {
            case "owner" -> "live owner write fence";
            case "snapshot", "digest", "namespace" -> "differs from sampled placement";
            default -> "exact live new-content attempt";
        });
        assertThat(selection(f)[0]).isEqualTo(f.attempt);
    }

    @Test void changedCanonicalCommandCannotUseAnExistingOwner() {
        var f = fixture(1);
        var changed = new DocumentPublicationCommand(f.command.intent().toBuilder().setMembers(0,
                f.command.intent().getMembers(0).toBuilder().putMetadata("purpose", "different")).build());
        var prepared = DocumentOperationUploadAdmission.prepare(changed, f.placements(), Map.of("member", f.attempt), LEASE);
        assertThatThrownBy(() -> admission.admit(ADMIN, f.owner, prepared))
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
        assertThatThrownBy(() -> admission.admit(ADMIN, selectedOwner, prepared)).satisfies(failure -> {
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
        var original = admission.admit(ADMIN, f.owner, prepared).getFirst();
        assertThatThrownBy(() -> admission.admit(ADMIN, f.owner, prepared)).hasStackTraceContaining("duplicate key");
        assertThat(new DocumentPartAttemptLedger(tx).find(f.attempt).orElseThrow()).isEqualTo(original);
    }

    @Test void laterMemberDatabaseFailureRollsBackEveryUploadAndReservation() {
        var f = fixture(1); UUID secondAttempt = UUID.randomUUID();
        var member = f.command.intent().getMembers(0);
        var intent = f.command.intent().toBuilder().setOperationId(UUID.randomUUID().toString()).addMembers(member.toBuilder()
                .setMemberId("second").setDestination(member.getDestination().toBuilder().setAddress(address("second")).setIfAbsent(true))).build();
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
            assertThatThrownBy(() -> admission.admit(ADMIN, owner, prepared)).hasStackTraceContaining("injected second member failure");
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
                        .setAddress(address("second")).setIfAbsent(true))).build());
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
            assertThatThrownBy(() -> admission.admit(ADMIN, owner, prepared)).hasMessageContaining("attempt expired before admission completed");
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
        var destinationAddress = address("destination-" + UUID.randomUUID());
        var sourceAddress = address("source-" + UUID.randomUUID());
        var destination = document(destinationAddress, drive.name);
        var retained = ManagedDocumentFixture.publish(tx, drive, generation, profile, sourceAddress, POLICY, 1, 1, "source-v1");
        var source = retained.row();
        var slot = DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE).build();
        var member = DocumentPublicationMember.newBuilder().setMemberId("member").setDriveId(drive.driveId.toString())
                .setDestination(DocumentRevisionCondition.newBuilder().setAddress(destinationAddress).setExpectedMutationRevision(destination.mutationRevision))
                .setOwnership(OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source").setSecurity(POLICY))
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                .addParts(DocumentPublicationPart.newBuilder().setSlot(slot).setReuse(PublicationReuse.newBuilder()
                        .setSource(DocumentRevisionCondition.newBuilder().setAddress(sourceAddress).setExpectedMutationRevision(source.mutationRevision)).setSourceSlot(slot)
                        .setObject(retained.identities().getFirst())));
        for (int i = 0; i < chunks; i++) member.addParts(DocumentPublicationPart.newBuilder()
                .setSlot(DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CHUNKS).setSubKey("chunk-" + i))
                .setUpload(PublicationUpload.newBuilder().setSizeBytes(9007199254740993L).setSha256(SHA).setContentType("application/protobuf")));
        var command = new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1).setAccountId("account")
                .setOperationId(UUID.randomUUID().toString()).addMembers(member).build());
        var key = new RepositoryOperationLedger.Key("account", "principal", command.operationId());
        var owner = operations.admit(key, command, UUID.randomUUID(), LEASE).owner().orElseThrow();
        return new Fixture(command, owner, drive, DocumentUploadPlan.Placement.sample(drive, generation, profile), UUID.randomUUID(), destination, source);
    }

    private static DocumentRecord document(NodeAddress address, String drive) {
        var row = new DocumentRecord(); row.nodeId = DocumentIds.nodeId(address);
        row.docId = address.getDocId(); row.accountId = address.getAccountId(); row.graphAddressId = address.getGraphAddressId();
        row.graphId = address.getGraphId(); row.rowKind = DocumentRowKind.PIPELINE; row.datasourceId = "source";
        row.driveName = drive; row.objectKey = "synthetic-legacy/" + row.nodeId; row.etag = "synthetic"; row.checksum = SHA; row.sizeBytes = 0L;
        row.writeSecurity(POLICY);
        return new DocumentLedger(tx).save(row);
    }

    private static NodeAddress address(String id) {
        return NodeAddress.newBuilder().setDocId(id).setGraphAddressId("node").setGraphId("graph").setAccountId("account").build();
    }
}
