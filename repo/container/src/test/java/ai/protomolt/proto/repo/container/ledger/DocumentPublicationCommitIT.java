package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.blob.s3.S3BackendIdentity;
import ai.protomolt.proto.repo.codec.*;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Real versioned provider writes, content admission, native SQL commit and authorized replay. */
@Testcontainers
class DocumentPublicationCommitIT {
    @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3=new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    private static LedgerDatabase database;
    private static Tx tx;
    private static OpenedBlobStore opened;
    private static ManagedBackendLedger.Profile profile;
    private static final String GENERATION="native-commit";
    private static final String NAMESPACE="native-commit";
    private static final Duration LEASE=Duration.ofMinutes(5);
    private static final RepositoryCaller ADMIN=new RepositoryCaller("principal",true);
    private static final DocumentRevisionAssembly.Limits LIMITS=new DocumentRevisionAssembly.Limits(1_000_000,100,100,100,100_000);

    @BeforeAll static void open() {
        database=new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(),POSTGRES.getUsername(),POSTGRES.getPassword()));
        tx=new Tx(database.entityManagerFactory());
        opened=BlobStores.discover().open("s3",Map.of("endpoint",S3.getEndpoint().toString(),"region",S3.getRegion(),
                "access-key",S3.getAccessKey(),"secret-key",S3.getSecretKey(),"path-style","true","conditional-writes","false"));
        opened.ensureNamespace(NAMESPACE);
        try (var admin=software.amazon.awssdk.services.s3.S3Client.builder().endpointOverride(S3.getEndpoint())
                .region(software.amazon.awssdk.regions.Region.of(S3.getRegion())).forcePathStyle(true)
                .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                        software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(S3.getAccessKey(),S3.getSecretKey()))).build()) {
            admin.putBucketVersioning(b -> b.bucket(NAMESPACE).versioningConfiguration(v ->
                    v.status(software.amazon.awssdk.services.s3.model.BucketVersioningStatus.ENABLED)));
        }
        profile=new ManagedBackendLedger.Profile(S3BackendIdentity.of(S3.getEndpoint().toString(),S3.getRegion(),true),"native-commit-realm");
        new ManagedBackendLedger(tx).bind(GENERATION,profile);
    }
    @AfterAll static void close() throws Exception { if(opened!=null) opened.close(); if(database!=null) database.close(); }

    private record Fixture(DocumentPublicationCommand command,RepositoryOperationLedger.Owner owner,
            DocumentOperationUploadAdmission.Prepared prepared,Map<DocumentUploadPayloads.Key,PartObject> bodies) {}
    private record Checked(Map<String,DocumentCommandContent> content,Map<String,DocumentSelectedAttemptLedger.Selected> selected) {}

    @Test void commitsTwoRealDocumentsWithOpaqueAnyAndReplaysExactOutcome() {
        var fixture=fixture(2);
        var checked=stage(fixture);
        var result=publisher().commit(ADMIN,fixture.owner,fixture.prepared,checked.content,checked.selected,()->{});
        assertThat(result.getMembersCount()).isEqualTo(2);
        assertThat(new DocumentPublicationReplay(tx).observe(ADMIN,fixture.command).result()).contains(result);
        for (var revision:result.getMembersList()) {
            var node=ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(revision.getAddress());
            var row=new DocumentLedger(tx).findByNodeId(node).orElseThrow();
            assertThat(row.objectKey).isNull();
            var retained=new DocumentPublicationLedger(tx).findForRead(row).orElseThrow();
            assertThat(retained.revisionId().toString()).isEqualTo(revision.getRevisionId());
            for (var bound:retained.boundParts()) {
                var part=bound.part();
                var actual=opened.store().getBounded(bound.binding().namespace(),part.key(),part.providerVersion(),Math.toIntExact(part.size()));
                assertThat(actual.versionId()).isEqualTo(part.providerVersion());
                assertThat(DocumentPartCodec.sha256Hex(actual.data())).isEqualTo(part.sha256());
                var expected=fixture.bodies.entrySet().stream().filter(e -> e.getKey().member().equals(revision.getMemberId())
                        && e.getValue().part()==part.part() && e.getValue().subKey().equals(part.subKey())).findFirst().orElseThrow().getValue();
                assertThat(actual.data()).isEqualTo(expected.bytes());
            }
            assertThat(row.readManifest().getPartsList()).allSatisfy(part -> {
                assertThat(part.getWrittenBy().getModuleId()).isEqualTo("producer");
                assertThat(part.getState()).isEqualTo(PartState.PART_STATE_PRESENT);
            });
            var observation=tx.readOnly(em -> (byte[])em.createNativeQuery(
                    "SELECT structured_resolution FROM document_revision_commits WHERE revision_id=:id")
                    .setParameter("id",UUID.fromString(revision.getRevisionId())).getSingleResult());
            try {
                assertThat(RepositoryAnyResolution.parseFrom(observation).getNotAttempted()).isTrue();
            } catch (com.google.protobuf.InvalidProtocolBufferException failure) { throw new AssertionError(failure); }
        }
    }

    @Test void secondMemberSqlFailureRollsBackEntireBatchAndCanRetry() {
        var fixture=fixture(2);
        var checked=stage(fixture);
        tx.inTransaction(em -> {
            em.createNativeQuery("""
                    CREATE FUNCTION test_reject_second_native_member() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                      IF NEW.member_ordinal=1 THEN RAISE EXCEPTION 'injected second member failure'; END IF;
                      RETURN NEW;
                    END $$
                    """).executeUpdate();
            em.createNativeQuery("CREATE TRIGGER test_reject_second_native_member BEFORE INSERT ON document_revision_commits "
                    +"FOR EACH ROW EXECUTE FUNCTION test_reject_second_native_member()").executeUpdate();
        });
        try {
            assertThatThrownBy(()->publisher().commit(ADMIN,fixture.owner,fixture.prepared,checked.content,checked.selected,()->{}))
                    .hasStackTraceContaining("injected second member failure");
            for (var member:fixture.command.intent().getMembersList())
                assertThat(new DocumentLedger(tx).findByNodeId(ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(member.getDestination().getAddress()))).isEmpty();
            assertThat(new DocumentPublicationReplay(tx).observe(ADMIN,fixture.command).state()).isEqualTo(DocumentPublicationReplay.State.PENDING);
            assertThat(tx.<Long>readOnly(em -> ((Number)em.createNativeQuery(
                    "SELECT count(*) FROM document_revision_commits WHERE operation_id=:id")
                    .setParameter("id",fixture.command.operationId()).getSingleResult()).longValue())).isZero();
        } finally {
            tx.inTransaction(em -> {
                em.createNativeQuery("DROP TRIGGER test_reject_second_native_member ON document_revision_commits").executeUpdate();
                em.createNativeQuery("DROP FUNCTION test_reject_second_native_member()").executeUpdate();
            });
        }
        var result=publisher().commit(ADMIN,fixture.owner,fixture.prepared,checked.content,checked.selected,()->{});
        assertThat(new DocumentPublicationReplay(tx).observe(ADMIN,fixture.command).result()).contains(result);
    }

    private static DocumentPublicationCommit publisher() { return new DocumentPublicationCommit(tx,new DriveLedger(tx),false,false); }

    @Test void callerFailureAfterCommitReplaysWithoutRepeatingPublication() {
        var fixture=fixture(2);
        var checked=stage(fixture);
        var committed=new java.util.concurrent.atomic.AtomicReference<DocumentPublicationResult>();
        // Inject failure at the caller's result-delivery boundary after the real SQL commit.
        // This exercises durable recovery, not a claimed network transport implementation.
        assertThatThrownBy(()->{
            committed.set(publisher().commit(ADMIN,fixture.owner,fixture.prepared,checked.content,checked.selected,()->{}));
            throw new java.io.IOException("caller lost the committed response");
        }).isInstanceOf(java.io.IOException.class);
        assertThat(new DocumentPublicationReplay(tx).observe(ADMIN,fixture.command).result()).contains(committed.get());
        assertThatThrownBy(()->publisher().commit(ADMIN,fixture.owner,fixture.prepared,checked.content,checked.selected,()->{}))
                .hasStackTraceContaining("Repository operation is terminal");
        assertThat(tx.<Long>readOnly(em -> ((Number)em.createNativeQuery(
                "SELECT count(*) FROM document_revision_commits WHERE operation_id=:id")
                .setParameter("id",fixture.command.operationId()).getSingleResult()).longValue())).isEqualTo(2);
        assertThat(tx.<Long>readOnly(em -> ((Number)em.createNativeQuery(
                "SELECT count(*) FROM repository_operation_success WHERE operation_id=:id")
                .setParameter("id",fixture.command.operationId()).getSingleResult()).longValue())).isEqualTo(1);
    }

    @Test void policyRevokedAfterStagingPreventsPublicationBeforeRevisionDisclosure() {
        var grant=DocumentSecurity.newBuilder()
                .addPermissions(AccessRule.newBuilder().setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_READ))
                .addPermissions(AccessRule.newBuilder().setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_WRITE)).build();
        var first=fixture(1,1,grant);
        var initial=stage(first);
        var original=publisher().commit(ADMIN,first.owner,first.prepared,initial.content,initial.selected,()->{});
        var revision=original.getMembers(0);
        var member=first.command.intent().getMembers(0).toBuilder().setDestination(
                first.command.intent().getMembers(0).getDestination().toBuilder().clearIfAbsent().setExpectedMutationRevision(revision.getMutationRevision()));
        var command=new DocumentPublicationCommand(first.command.intent().toBuilder().setOperationId(UUID.randomUUID().toString()).setMembers(0,member).build());
        var placements=new HashMap<UUID,DocumentUploadPlan.Placement>();
        first.prepared.members().forEach(m -> placements.put(m.placement().drive().id(),m.placement()));
        var owner=new RepositoryOperationLedger(tx).admit(new RepositoryOperationLedger.Key("account","principal",command.operationId()),command,UUID.randomUUID(),LEASE).owner().orElseThrow();
        var prepared=DocumentOperationUploadAdmission.prepare(command,placements,Map.of(member.getMemberId(),UUID.randomUUID()),LEASE);
        var ready=stage(new Fixture(command,owner,prepared,first.bodies));
        var node=ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(revision.getAddress());
        tx.inTransaction(em -> {
            var row=em.find(DocumentRecord.class,node,jakarta.persistence.LockModeType.PESSIMISTIC_WRITE);
            row.writeSecurity(DocumentSecurity.newBuilder().addPermissions(AccessRule.newBuilder()
                    .setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_DENY)).build());
        });
        var caller=new RepositoryCaller("principal",false,java.util.Set.of("account"),java.util.Set.of());
        assertThatThrownBy(()->publisher().commit(caller,owner,prepared,ready.content,ready.selected,()->{}))
                .isInstanceOf(ai.protomolt.proto.repo.spi.RepositoryException.class)
                .satisfies(failure -> assertThat(((ai.protomolt.proto.repo.spi.RepositoryException)failure).code())
                        .isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.NOT_FOUND));
        assertThatThrownBy(()->publisher().commit(ADMIN,owner,prepared,ready.content,ready.selected,()->{}))
                .isInstanceOf(DocumentLedger.RevisionConflictException.class);
        assertThat(new DocumentPublicationReplay(tx).observe(ADMIN,command).state()).isEqualTo(DocumentPublicationReplay.State.PENDING);
        assertThat(new DocumentPublicationReplay(tx).observe(ADMIN,first.command).result()).contains(original);
    }

    @Test void cancellationDuringFirstMemberWriteRollsBackAndAllowsRetry() throws Exception {
        var fixture=fixture(2);
        var checked=stage(fixture);
        int lockKey=java.util.concurrent.ThreadLocalRandom.current().nextInt(1,Integer.MAX_VALUE);
        tx.inTransaction(em -> {
            em.createNativeQuery("""
                    CREATE FUNCTION test_pause_native_member() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                      IF NEW.member_ordinal=0 THEN PERFORM pg_advisory_xact_lock(%d); END IF;
                      RETURN NEW;
                    END $$
                    """.formatted(lockKey)).executeUpdate();
            em.createNativeQuery("CREATE TRIGGER test_pause_native_member BEFORE INSERT ON document_revision_commits "
                    +"FOR EACH ROW EXECUTE FUNCTION test_pause_native_member()").executeUpdate();
        });
        var cancelled=new java.util.concurrent.atomic.AtomicBoolean();
        try (var executor=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
                var blocker=database.entityManagerFactory().createEntityManager()) {
            blocker.getTransaction().begin();
            blocker.createNativeQuery("SELECT pg_advisory_xact_lock(:key)").setParameter("key",lockKey).getSingleResult();
            var pending=executor.submit(()->publisher().commit(ADMIN,fixture.owner,fixture.prepared,checked.content,checked.selected,
                    ()->{if(cancelled.get()) throw new java.util.concurrent.CancellationException("cancelled during publication");}));
            try {
                boolean waiting=false;
                long deadline=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
                do {
                    waiting=tx.readOnly(em -> ((Number)em.createNativeQuery(
                            "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND objid=:key AND NOT granted")
                            .setParameter("key",lockKey).getSingleResult()).longValue()>0);
                    if (waiting || pending.isDone()) break;
                    Thread.sleep(10);
                } while (System.nanoTime()<deadline);
                assertThat(waiting).as("publisher reached the first member's real SQL write").isTrue();
                cancelled.set(true);
            } finally { blocker.getTransaction().rollback(); }
            assertThatThrownBy(()->pending.get(10,java.util.concurrent.TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(java.util.concurrent.CancellationException.class);
        } finally {
            tx.inTransaction(em -> {
                em.createNativeQuery("DROP TRIGGER test_pause_native_member ON document_revision_commits").executeUpdate();
                em.createNativeQuery("DROP FUNCTION test_pause_native_member()").executeUpdate();
            });
        }
        for (var member:fixture.command.intent().getMembersList())
            assertThat(new DocumentLedger(tx).findByNodeId(ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(member.getDestination().getAddress()))).isEmpty();
        assertThat(new DocumentPublicationReplay(tx).observe(ADMIN,fixture.command).state()).isEqualTo(DocumentPublicationReplay.State.PENDING);
        var result=publisher().commit(ADMIN,fixture.owner,fixture.prepared,checked.content,checked.selected,()->{});
        assertThat(new DocumentPublicationReplay(tx).observe(ADMIN,fixture.command).result()).contains(result);
    }

    @Test void replacementSelectionRequiresRebuiltPreparedPlan() {
        var first=fixture(1);
        var initial=stage(first);
        var selected=initial.selected.values().iterator().next();
        var placements=new HashMap<UUID,DocumentUploadPlan.Placement>();
        first.prepared.members().forEach(m -> placements.put(m.placement().drive().id(),m.placement()));
        var replacement=DocumentOperationUploadAdmission.prepare(first.command,placements,Map.of(selected.member(),UUID.randomUUID()),LEASE);
        var next=new Fixture(first.command,first.owner,replacement,first.bodies);
        Checked ready;
        try (var coordinator=new DocumentUploadCoordinator(tx,new DriveLedger(tx),new PayloadBudget(2_000_000),
                (generation,retained)->new DocumentUploadCoordinator.Backend(profile.identity(),opened),4,Duration.ofMillis(25),
                new SqlTimeouts(Duration.ofSeconds(2),Duration.ofSeconds(5)))) {
            var staged=coordinator.retry(ADMIN,first.owner,replacement,first.bodies,Map.of(),()->{},
                    Map.of(selected.member(),new DocumentOperationSelection.Expected(selected.revision(),selected.attempt())));
            var replacementSelections=new HashMap<String,DocumentSelectedAttemptLedger.Selected>();
            staged.members().forEach(m -> replacementSelections.put(m.selection().member(),m.selection()));
            // The command and exact checked bytes are unchanged; only their physical attempt changes.
            ready=new Checked(initial.content,Map.copyOf(replacementSelections));
        }
        assertThat(ready.selected.get(selected.member()).attempt()).isNotEqualTo(selected.attempt());
        assertThatThrownBy(()->publisher().commit(ADMIN,first.owner,first.prepared,ready.content,ready.selected,()->{}))
                .isInstanceOf(DocumentPartAttemptLedger.FenceException.class);
        assertThat(new DocumentPublicationReplay(tx).observe(ADMIN,first.command).state()).isEqualTo(DocumentPublicationReplay.State.PENDING);
        var result=publisher().commit(ADMIN,next.owner,next.prepared,ready.content,ready.selected,()->{});
        assertThat(new DocumentPublicationReplay(tx).observe(ADMIN,first.command).result()).contains(result);
    }

    @Test void hostTypedRequirementAndCancellationCannotPublishOpaqueContent() {
        var fixture=fixture(1);
        var checked=stage(fixture);
        var typed=new DocumentPublicationCommit(tx,new DriveLedger(tx),true,false);
        assertThatThrownBy(()->typed.commit(ADMIN,fixture.owner,fixture.prepared,checked.content,checked.selected,()->{}))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(()->publisher().commit(ADMIN,fixture.owner,fixture.prepared,checked.content,checked.selected,
                ()->{throw new java.util.concurrent.CancellationException("caller cancelled");}))
                .isInstanceOf(java.util.concurrent.CancellationException.class);
        try {
            Thread.currentThread().interrupt();
            assertThatThrownBy(()->publisher().commit(ADMIN,fixture.owner,fixture.prepared,checked.content,checked.selected,()->{}))
                    .isInstanceOf(java.util.concurrent.CancellationException.class);
        } finally { Thread.interrupted(); }
        assertThat(new DocumentPublicationReplay(tx).observe(ADMIN,fixture.command).state()).isEqualTo(DocumentPublicationReplay.State.PENDING);
        var result=publisher().commit(ADMIN,fixture.owner,fixture.prepared,checked.content,checked.selected,()->{});
        assertThat(new DocumentPublicationReplay(tx).observe(ADMIN,fixture.command).result()).contains(result);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    void publishesNewRevisionWithRetainedCoreAndOptionalFreshParts(boolean mixed) {
        publishRetainedRevision(mixed,1);
    }

    @Test void projectsRetainedProvenanceAcrossSqlBatchBoundary() {
        publishRetainedRevision(false,40);
    }

    private static void publishRetainedRevision(boolean mixed,int chunks) {
        var first=fixture(1,chunks);
        var checked=stage(first);
        var original=publisher().commit(ADMIN,first.owner,first.prepared,checked.content,checked.selected,()->{});
        var revision=original.getMembers(0);
        var node=ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(revision.getAddress());
        var prior=new DocumentLedger(tx).findByNodeId(node).orElseThrow();
        var publication=new DocumentPublicationLedger(tx).findForRead(prior).orElseThrow();
        var member=first.command.intent().getMembers(0).toBuilder();
        var condition=member.getDestination().toBuilder().clearIfAbsent().setExpectedMutationRevision(prior.mutationRevision).build();
        member.setDestination(condition);
        var retainedBytes=new HashMap<DocumentUploadPayloads.Key,ByteString>();
        var freshBodies=new HashMap<DocumentUploadPayloads.Key,PartObject>();
        assertThat(member.getPartsCount()).isGreaterThan(1);
        for (int i=0;i<member.getPartsCount();i++) {
            var declaration=member.getParts(i);
            var key=new DocumentUploadPayloads.Key(member.getMemberId(),i);
            if (mixed && declaration.getSlot().getPart()!=DocumentPart.DOCUMENT_PART_CORE) {
                freshBodies.put(key,first.bodies.get(key));
                continue;
            }
            var bound=publication.boundParts().stream().filter(p -> p.part().part()==declaration.getSlot().getPart()
                    && p.part().subKey().equals(declaration.getSlot().getSubKey())).findFirst().orElseThrow();
            var part=bound.part();
            var objectId=tx.readOnly(em -> em.createNativeQuery("SELECT object_id FROM document_revision_parts WHERE revision_id=:revision AND part=:part AND sub_key=:key")
                    .setParameter("revision",publication.revisionId()).setParameter("part",part.part().getNumber()).setParameter("key",part.subKey()).getSingleResult().toString());
            var identity=PublicationObjectIdentity.newBuilder().setObjectId(objectId).setBackendGeneration(bound.binding().generation())
                    .setStorageRealm(bound.binding().profile().storageRealm()).setNamespace(bound.binding().namespace()).setObjectKey(part.key())
                    .setProviderVersion(part.providerVersion()).setSizeBytes(part.size()).setSha256(part.sha256()).setContentType(part.contentType());
            member.setParts(i,declaration.toBuilder().clearUpload().setReuse(PublicationReuse.newBuilder()
                    .setSource(condition).setSourceSlot(declaration.getSlot()).setObject(identity)));
            var actual=opened.store().getBounded(bound.binding().namespace(),part.key(),part.providerVersion(),Math.toIntExact(part.size()));
            assertThat(DocumentPartCodec.sha256Hex(actual.data())).isEqualTo(part.sha256());
            retainedBytes.put(key,ByteString.copyFrom(actual.data()));
        }
        var command=new DocumentPublicationCommand(first.command.intent().toBuilder().setOperationId(UUID.randomUUID().toString()).setMembers(0,member).build());
        var placements=new HashMap<UUID,DocumentUploadPlan.Placement>();
        first.prepared.members().forEach(m -> placements.put(m.placement().drive().id(),m.placement()));
        var owner=new RepositoryOperationLedger(tx).admit(new RepositoryOperationLedger.Key("account","principal",command.operationId()),command,UUID.randomUUID(),LEASE).owner().orElseThrow();
        var prepared=DocumentOperationUploadAdmission.prepare(command,placements,mixed ? Map.of(member.getMemberId(),UUID.randomUUID()) : Map.of(),LEASE);
        var next=new Fixture(command,owner,prepared,Map.copyOf(freshBodies));
        var nextChecked=stage(next,Map.copyOf(retainedBytes));
        assertThat(nextChecked.selected.size()).isEqualTo(mixed ? 1 : 0);
        var result=publisher().commit(ADMIN,owner,prepared,nextChecked.content,nextChecked.selected,()->{});
        var current=new DocumentLedger(tx).findByNodeId(node).orElseThrow();
        assertThat(current.readManifest().getDocVersion()).isEqualTo(prior.readManifest().getDocVersion()+1);
        var priorCore=prior.readManifest().getPartsList().stream().filter(p -> p.getPart()==DocumentPart.DOCUMENT_PART_CORE).findFirst().orElseThrow();
        var currentCore=current.readManifest().getPartsList().stream().filter(p -> p.getPart()==DocumentPart.DOCUMENT_PART_CORE).findFirst().orElseThrow();
        assertThat(currentCore).isEqualTo(priorCore);
        var currentPublication=new DocumentPublicationLedger(tx).findForRead(current).orElseThrow();
        for (var part:currentPublication.parts()) {
            var old=publication.parts().stream().filter(p -> p.part()==part.part() && p.subKey().equals(part.subKey())).findFirst().orElseThrow();
            if (mixed && part.part()!=DocumentPart.DOCUMENT_PART_CORE) {
                assertThat(part.key()).isNotEqualTo(old.key());
                assertThat(part.providerVersion()).isNotEqualTo(old.providerVersion());
            } else {
                assertThat(part.key()).isEqualTo(old.key());
                assertThat(part.providerVersion()).isEqualTo(old.providerVersion());
            }
        }
        assertThat(new DocumentPublicationReplay(tx).observe(ADMIN,first.command).result()).contains(original);
        assertThat(new DocumentPublicationReplay(tx).observe(ADMIN,command).result()).contains(result);
    }

    private static Checked stage(Fixture fixture) {
        return stage(fixture,Map.of());
    }

    private static Checked stage(Fixture fixture,Map<DocumentUploadPayloads.Key,ByteString> retainedBytes) {
        try (var coordinator=new DocumentUploadCoordinator(tx,new DriveLedger(tx),new PayloadBudget(2_000_000),
                (generation,retained)->new DocumentUploadCoordinator.Backend(profile.identity(),opened),4,Duration.ofMillis(25),
                new SqlTimeouts(Duration.ofSeconds(2),Duration.ofSeconds(5)))) {
            return coordinator.stageAndPrepare(ADMIN,fixture.owner,fixture.prepared,fixture.bodies,Map.of(),()->{},(staged,view,active)->{
                var content=new HashMap<String,DocumentCommandContent>();
                var selected=new HashMap<String,DocumentSelectedAttemptLedger.Selected>();
                for (var member:fixture.command.intent().getMembersList()) {
                    // Small test-owned copies outlive the borrowed preparation view.
                    var bytes=new HashMap<Integer,ByteString>();
                    for (int i=0;i<member.getPartsCount();i++) {
                        var key=new DocumentUploadPayloads.Key(member.getMemberId(),i);
                        bytes.put(i,member.getParts(i).hasUpload() ? ByteString.copyFrom(view.bytes(key)) : retainedBytes.get(key));
                    }
                    try { content.put(member.getMemberId(),DocumentCommandContent.check(fixture.command,member.getMemberId(),bytes,false,LIMITS,active)); }
                    catch (com.google.protobuf.InvalidProtocolBufferException failure) { throw new IllegalArgumentException(failure); }
                }
                for (var member:staged.members()) selected.put(member.selection().member(),member.selection());
                return new Checked(Map.copyOf(content),Map.copyOf(selected));
            });
        }
    }

    private static Fixture fixture(int count) {
        return fixture(count,1);
    }

    private static Fixture fixture(int count,int chunks) {
        return fixture(count,chunks,DocumentSecurity.getDefaultInstance());
    }

    private static Fixture fixture(int count,int chunks,DocumentSecurity policy) {
        var drive=new DriveRecord(); drive.driveId=UUID.randomUUID(); drive.accountId="account"; drive.name="native-"+drive.driveId;
        drive.driveType="CUSTOM"; drive.provider="s3"; drive.bucket=NAMESPACE; new DriveLedger(tx).insert(drive);
        var placements=Map.of(drive.driveId,DocumentUploadPlan.Placement.sample(drive,GENERATION,profile));
        var intent=DocumentPublicationIntent.newBuilder().setEncodingVersion(1).setAccountId("account").setOperationId(UUID.randomUUID().toString());
        var bodies=new HashMap<DocumentUploadPayloads.Key,PartObject>();
        var attempts=new HashMap<String,UUID>();
        for (int index=0;index<count;index++) {
            String id="member-"+index, docId=UUID.randomUUID().toString();
            var ownership=OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source").setSecurity(policy).build();
            var metadata=SearchMetadata.newBuilder();
            for (int chunk=0;chunk<chunks;chunk++) metadata.addSemanticResults(SemanticProcessingResult.newBuilder().setResultId("original-"+chunk));
            var document=Document.newBuilder().setDocId(docId).setOwnership(ownership)
                    .setSearchMetadata(metadata)
                    .setStructuredData(Any.newBuilder()
                    .setTypeUrl("archive.example/unavailable.Record").setValue(ByteString.copyFrom(new byte[]{0,(byte)255,1}))).build();
            var member=DocumentPublicationMember.newBuilder().setMemberId(id).setDriveId(drive.driveId.toString()).setOwnership(ownership)
                    .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE)
                    .setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true).setAddress(NodeAddress.newBuilder()
                            .setAccountId("account").setGraphId("graph").setGraphAddressId("node").setDocId(docId)));
            var fragments=DocumentPartCodec.split(document,PartLayouts.document());
            for (int i=0;i<fragments.size();i++) {
                var part=fragments.get(i); bodies.put(new DocumentUploadPayloads.Key(id,i),part);
                member.addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder().setPart(part.part()).setSubKey(part.subKey()))
                        .setUpload(PublicationUpload.newBuilder().setSizeBytes(part.bytes().length).setSha256(part.sha256()).setContentType("application/protobuf")
                                .setWrittenBy(WriteProvenance.newBuilder().setModuleId("producer"))));
            }
            intent.addMembers(member); attempts.put(id,UUID.randomUUID());
        }
        var command=new DocumentPublicationCommand(intent.build());
        var owner=new RepositoryOperationLedger(tx).admit(new RepositoryOperationLedger.Key("account","principal",command.operationId()),command,UUID.randomUUID(),LEASE).owner().orElseThrow();
        return new Fixture(command,owner,DocumentOperationUploadAdmission.prepare(command,placements,attempts,LEASE),Map.copyOf(bodies));
    }
}
