package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
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

/** SQL proof tests with synthetic observations; not evidence of provider read-back. */
@Testcontainers
class DocumentSelectedAttemptIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static LedgerDatabase database;
    private static Tx tx;
    private static DocumentOperationUploadAdmission admission;
    private static DocumentSelectedAttemptLedger selected;
    private static final Duration LEASE = Duration.ofMinutes(5);
    private static final String SHA = "a".repeat(64);
    private static final RepositoryCaller ADMIN = new RepositoryCaller("principal", true);

    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
        admission = new DocumentOperationUploadAdmission(tx, new DriveLedger(tx));
        selected = new DocumentSelectedAttemptLedger(tx);
    }
    @AfterAll static void close() { if (database != null) database.close(); }

    private record Fixture(DocumentPublicationCommand command, RepositoryOperationLedger.Owner owner,
            Map<UUID, DocumentUploadPlan.Placement> placements, DocumentSelectedAttemptLedger.Selected selection,
            List<DocumentSelectedAttemptLedger.Observation> observations) {}

    @ParameterizedTest @ValueSource(ints={1,256})
    void boundedBatchVerifiesAllPartsWithConstantClientStatementCount(int parts) {
        var f = fixture(parts);
        var statistics = database.entityManagerFactory().unwrap(org.hibernate.SessionFactory.class).getStatistics();
        statistics.setStatisticsEnabled(true); statistics.clear();
        try {
            assertThat(selected.verifyBatch(f.owner, f.selection, f.observations).state()).isEqualTo("VERIFIED");
            assertThat(statistics.getTransactionCount()).isEqualTo(1);
            assertThat(statistics.getPrepareStatementCount()).isLessThanOrEqualTo(9);
        } finally { statistics.setStatisticsEnabled(false); }
        assertThat(selected.verifyBatch(f.owner, f.selection, f.observations).state()).isEqualTo("VERIFIED");
        assertThat(selected.renew(f.owner, List.of(f.selection), LEASE)).hasSize(1);
    }

    @Test void largePlanRequiresBoundedBatchesAndRejectsDuplicatesBeforeSql() {
        var f = fixture(257);
        var statistics = database.entityManagerFactory().unwrap(org.hibernate.SessionFactory.class).getStatistics();
        statistics.setStatisticsEnabled(true); statistics.clear();
        try {
            assertThatThrownBy(() -> selected.verifyBatch(f.owner, f.selection, f.observations))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("256");
            assertThatThrownBy(() -> selected.verifyBatch(f.owner, f.selection, List.of(f.observations.getFirst(),f.observations.getFirst())))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Duplicate");
            assertThat(statistics.getPrepareStatementCount()).isZero();
        } finally { statistics.setStatisticsEnabled(false); }
        assertThat(selected.verifyBatch(f.owner, f.selection, f.observations.subList(0,256)).state()).isEqualTo("STAGING");
        assertThat(selected.verifyBatch(f.owner, f.selection, f.observations.subList(256,257)).state()).isEqualTo("VERIFIED");
    }

    @ParameterizedTest @ValueSource(strings={"key","size","sha","type"})
    void mismatchedObservationRollsBackEarlierRows(String field) {
        var f = fixture(2); var original = f.observations.getLast();
        var changed = new DocumentSelectedAttemptLedger.Observation(field.equals("key") ? "missing" : original.key(),
                field.equals("size") ? 2 : original.size(),field.equals("sha") ? "b".repeat(64) : original.sha256(),
                field.equals("type") ? "text/plain" : original.contentType(),original.version(),original.etag());
        assertThatThrownBy(() -> selected.verifyBatch(f.owner,f.selection,List.of(f.observations.getFirst(),changed)))
                .hasMessageContaining("differ from admitted");
        assertThat(verified(f)).isZero();
    }

    @ParameterizedTest @ValueSource(strings={"version","etag"})
    void replayCannotChangeProviderIdentity(String field) {
        var f = fixture(1); selected.verifyBatch(f.owner,f.selection,f.observations);
        var original = f.observations.getFirst();
        var changed = new DocumentSelectedAttemptLedger.Observation(original.key(),original.size(),original.sha256(),original.contentType(),
                field.equals("version") ? "other" : original.version(),field.equals("etag") ? "other" : original.etag());
        assertThatThrownBy(() -> selected.verifyBatch(f.owner,f.selection,List.of(changed))).hasMessageContaining("differ from admitted");
        assertThat(selected.verifyBatch(f.owner,f.selection,f.observations).state()).isEqualTo("VERIFIED");
    }

    @ParameterizedTest @ValueSource(strings={"token","revision","member","attempt"})
    void wrongBindingCannotRenewOrVerify(String field) {
        var f = fixture(1); var s = f.selection;
        var wrong = new DocumentSelectedAttemptLedger.Selected(field.equals("member") ? "other" : s.member(),
                field.equals("revision") ? 2 : s.revision(),field.equals("attempt") ? UUID.randomUUID() : s.attempt(),
                field.equals("token") ? UUID.randomUUID() : s.token());
        assertThatThrownBy(() -> selected.verifyBatch(f.owner,wrong,f.observations)).hasMessageContaining("exact live selection");
        assertThatThrownBy(() -> selected.renew(f.owner,List.of(wrong),LEASE)).hasMessageContaining("exact live selection");
        assertThat(verified(f)).isZero();
    }

    @Test void displacedAttemptCannotRenewVerifyOrWriteDirectly() {
        var f = fixture(1); UUID replacement = UUID.randomUUID();
        admission.retry(ADMIN,f.owner,DocumentOperationUploadAdmission.prepare(f.command,f.placements,
                Map.of("member",replacement),LEASE),Map.of("member",new DocumentOperationSelection.Expected(1,f.selection.attempt())));
        assertThatThrownBy(() -> selected.renew(f.owner,List.of(f.selection),LEASE)).hasMessageContaining("exact live selection");
        assertThatThrownBy(() -> selected.verifyBatch(f.owner,f.selection,f.observations)).hasMessageContaining("exact live selection");
        for (String sql : List.of(
                "UPDATE document_part_attempts SET lease_until=lease_until+interval '1 minute' WHERE attempt_id=:id",
                "UPDATE document_part_attempt_objects SET verified=true WHERE attempt_id=:id",
                "UPDATE document_part_attempt_objects SET verified=verified WHERE attempt_id=:id")) {
            assertThatThrownBy(() -> tx.inTransaction(em -> {
                RepositoryOperationLedger.fenceLiveOwner(em,f.owner);
                em.createNativeQuery(sql).setParameter("id",f.selection.attempt()).executeUpdate();
            })).hasStackTraceContaining("current selected attempt");
        }
        assertThat(verified(f)).isZero();
    }

    @ParameterizedTest @ValueSource(strings={"attempt","object"})
    void sameTransactionReplacementInvalidatesEarlierSelectionProof(String target) {
        var f=fixture(1); UUID next=stageReplacement(f);
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em,f.owner);
            em.createNativeQuery("SELECT require_selected_document_attempt(a) FROM document_part_attempts a WHERE attempt_id=:id")
                    .setParameter("id",f.selection.attempt()).getSingleResult();
            DocumentOperationSelection.replace(em,f.owner,"member",new DocumentOperationSelection.Expected(1,f.selection.attempt()),next);
            em.createNativeQuery(target.equals("attempt")
                    ? "UPDATE document_part_attempts SET lease_until=lease_until+interval '1 minute' WHERE attempt_id=:id"
                    : "UPDATE document_part_attempt_objects SET verified=true WHERE attempt_id=:id")
                    .setParameter("id",f.selection.attempt()).executeUpdate();
        })).hasStackTraceContaining("current selected attempt");
        assertThat(selected.renew(f.owner,List.of(f.selection),LEASE)).hasSize(1);
        assertThat(verified(f)).isZero();
    }

    @Test void renewalOf64MembersHasBoundedClientStatements() {
        var f=fixture(1);
        var intent=f.command.intent().toBuilder().setOperationId(UUID.randomUUID().toString()).clearMembers();
        var attempts=new java.util.LinkedHashMap<String,UUID>();
        for (int i=0;i<64;i++) {
            String id="m"+i; attempts.put(id,UUID.randomUUID());
            var member=f.command.intent().getMembers(0);
            intent.addMembers(member.toBuilder().setMemberId(id).setDestination(member.getDestination().toBuilder()
                    .setAddress(member.getDestination().getAddress().toBuilder().setDocId(UUID.randomUUID().toString()))));
        }
        var command=new DocumentPublicationCommand(intent.build());
        var owner=new RepositoryOperationLedger(tx).admit(new RepositoryOperationLedger.Key("account","principal",command.operationId()),
                command,UUID.randomUUID(),LEASE).owner().orElseThrow();
        var admitted=admission.admit(ADMIN,owner,DocumentOperationUploadAdmission.prepare(command,f.placements,attempts,LEASE));
        var bindings=admitted.stream().map(attempt -> new DocumentSelectedAttemptLedger.Selected(
                attempts.entrySet().stream().filter(entry -> entry.getValue().equals(attempt.id())).findFirst().orElseThrow().getKey(),
                1,attempt.id(),attempt.token())).toList();
        var statistics=database.entityManagerFactory().unwrap(org.hibernate.SessionFactory.class).getStatistics();
        statistics.setStatisticsEnabled(true); statistics.clear();
        List<DocumentPartAttemptLedger.Attempt> renewed;
        try {
            renewed=selected.renew(owner,bindings,LEASE);
            assertThat(renewed).hasSize(64);
            assertThat(statistics.getTransactionCount()).isEqualTo(1);
            assertThat(statistics.getPrepareStatementCount()).isLessThanOrEqualTo(9);
        } finally { statistics.setStatisticsEnabled(false); }
        var invalid=new java.util.ArrayList<>(bindings);
        var last=invalid.getLast();
        invalid.set(invalid.size()-1,new DocumentSelectedAttemptLedger.Selected(last.member(),last.revision(),last.attempt(),UUID.randomUUID()));
        assertThatThrownBy(() -> selected.renew(owner,invalid,Duration.ofHours(1))).hasMessageContaining("exact live selection");
        var after=tx.inTransaction(em -> { return DocumentPartAttemptLedger.lockAll(em,bindings.stream().map(DocumentSelectedAttemptLedger.Selected::attempt).toList()); });
        assertThat(after).containsExactlyElementsOf(renewed);
    }

    @ParameterizedTest @ValueSource(strings={"renew","verify"})
    void cleanupWinningAnAttemptLockWaitPreventsSuccessfulWork(String action) throws Exception {
        var f=fixture(1,Duration.ofSeconds(3));
        try (var blocker=database.entityManagerFactory().createEntityManager();
             var executor=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            blocker.getTransaction().begin();
            try {
                int pid=((Number)blocker.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue();
                blocker.createNativeQuery("SELECT attempt_id FROM document_part_attempts WHERE attempt_id=:id FOR UPDATE")
                        .setParameter("id",f.selection.attempt()).getSingleResult();
                var pending=executor.submit(() -> {
                    if (action.equals("renew")) selected.renew(f.owner,List.of(f.selection),LEASE);
                    else selected.verifyBatch(f.owner,f.selection,f.observations);
                });
                long deadline=System.nanoTime()+Duration.ofSeconds(5).toNanos();
                boolean waiting=false;
                do {
                    waiting=tx.readOnly(em -> !em.createNativeQuery("SELECT pid FROM pg_stat_activity WHERE :blocker=ANY(pg_blocking_pids(pid))")
                            .setParameter("blocker",pid).getResultList().isEmpty());
                    if (waiting) break;
                    Thread.sleep(10);
                } while(System.nanoTime()<deadline);
                assertThat(waiting).isTrue();
                blocker.createNativeQuery("SELECT CAST(pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.02) AS text) FROM document_part_attempts WHERE attempt_id=:id")
                        .setParameter("id",f.selection.attempt()).getSingleResult();
                blocker.createNativeQuery("""
                        INSERT INTO document_part_attempt_cleanup(attempt_id,cleanup_token,claim_until,state)
                        VALUES(:id,gen_random_uuid(),clock_timestamp()+interval '5 minutes','DELETING')
                        """).setParameter("id",f.selection.attempt()).executeUpdate();
                blocker.getTransaction().commit();
                assertThatThrownBy(() -> pending.get(5,java.util.concurrent.TimeUnit.SECONDS)).hasStackTraceContaining("exact live selection");
            } finally { if (blocker.getTransaction().isActive()) blocker.getTransaction().rollback(); }
        }
        assertThat(verified(f)).isZero();
    }

    private static UUID stageReplacement(Fixture f) {
        UUID next=UUID.randomUUID();
        var member=DocumentUploadPlan.prepare(f.command,f.placements,Map.of("member",next)).members().getFirst();
        var encoded=DocumentAttemptPlanEncoding.prepare(member);
        tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em,f.owner);
            em.createNativeQuery("""
                    INSERT INTO document_part_attempts(attempt_id,node_id,account_id,sampled_revision,backend_generation,
                        storage_realm,storage_namespace,planned_count,source_count,lease_token,lease_until,state,
                        plan_kind,operation_principal,operation_id,operation_generation,member_id,drive_id)
                    SELECT :next,node_id,account_id,sampled_revision,backend_generation,storage_realm,storage_namespace,
                        planned_count,source_count,gen_random_uuid(),clock_timestamp()+interval '5 minutes','PLANNING',
                        plan_kind,operation_principal,operation_id,operation_generation,member_id,drive_id
                    FROM document_part_attempts WHERE attempt_id=:original
                    """).setParameter("next",next).setParameter("original",f.selection.attempt()).executeUpdate();
            var location=member.attempt().orElseThrow().location();
            DocumentPartAttemptLedger.insertEncodedRows(em,encoded,next,member.placement().profile().storageRealm(),location.namespace());
            em.createNativeQuery("UPDATE document_part_attempts SET state='STAGING' WHERE attempt_id=:id").setParameter("id",next).executeUpdate();
        });
        return next;
    }

    private static long verified(Fixture f) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM document_part_attempt_objects WHERE attempt_id=:id AND verified")
                .setParameter("id",f.selection.attempt()).getSingleResult()).longValue());
    }

    private static Fixture fixture(int parts) {
        return fixture(parts,LEASE);
    }

    private static Fixture fixture(int parts, Duration attemptLease) {
        var drive = new DriveRecord(); drive.driveId=UUID.randomUUID(); drive.accountId="account";
        drive.name="selected-"+drive.driveId; drive.driveType="CUSTOM"; drive.provider="test-location"; drive.bucket="namespace";
        new DriveLedger(tx).insert(drive);
        var profile = new ManagedBackendLedger.Profile(new BackendIdentity("test-location","test-location/v1",Map.of("endpoint","synthetic-sql")),"realm");
        String generation="selected-"+UUID.randomUUID(); new ManagedBackendLedger(tx).bind(generation,profile);
        var placements=Map.of(drive.driveId,DocumentUploadPlan.Placement.sample(drive,generation,profile));
        var member=DocumentPublicationMember.newBuilder().setMemberId("member").setDriveId(drive.driveId.toString())
                .setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true).setAddress(NodeAddress.newBuilder()
                        .setAccountId("account").setGraphId("graph").setGraphAddressId("node").setDocId(UUID.randomUUID().toString())))
                .setOwnership(OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source")
                        .setSecurity(DocumentSecurity.getDefaultInstance()))
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE);
        for (int i=0;i<parts;i++) member.addParts(DocumentPublicationPart.newBuilder()
                .setSlot(DocumentPublicationSlot.newBuilder().setPart(i==0 ? DocumentPart.DOCUMENT_PART_CORE : DocumentPart.DOCUMENT_PART_CHUNKS)
                        .setSubKey(i==0 ? "" : "chunk-"+i))
                .setUpload(PublicationUpload.newBuilder().setSizeBytes(1).setSha256(SHA).setContentType("application/protobuf")));
        var command=new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1).setAccountId("account")
                .setOperationId(UUID.randomUUID().toString()).addMembers(member).build());
        var owner=new RepositoryOperationLedger(tx).admit(new RepositoryOperationLedger.Key("account","principal",command.operationId()),
                command,UUID.randomUUID(),LEASE).owner().orElseThrow();
        UUID id=UUID.randomUUID();
        var attempt=admission.admit(ADMIN,owner,DocumentOperationUploadAdmission.prepare(command,placements,Map.of("member",id),attemptLease)).getFirst();
        var plan=DocumentUploadPlan.prepare(command,placements,Map.of("member",id));
        var observations=plan.members().getFirst().attempt().orElseThrow().uploads().stream().map(upload -> {
            var object=upload.object(); return new DocumentSelectedAttemptLedger.Observation(object.objectKey(),object.size(),object.sha256(),object.contentType(),null,null);
        }).toList();
        return new Fixture(command,owner,placements,new DocumentSelectedAttemptLedger.Selected("member",1,id,attempt.token()),observations);
    }
}
