package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.blob.s3.S3BackendIdentity;
import ai.protomolt.proto.repo.codec.*;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Real adapter transfers composed explicitly with SQL gates, not a public operation coordinator. */
@Testcontainers
class DocumentSelectedTransferIT {
    @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3=new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    private static LedgerDatabase database;
    private static Tx tx;
    private static OpenedBlobStore opened;
    private static ManagedBackendLedger.Profile profile;
    private static DocumentOperationUploadAdmission admission;
    private static DocumentSelectedAttemptLedger selected;
    private static final String GENERATION="selected-transfer";
    private static final String NAMESPACE="selected-transfer";
    private static final Duration LEASE=Duration.ofMinutes(5);
    private static final RepositoryCaller ADMIN=new RepositoryCaller("principal",true);

    @BeforeAll static void open() {
        database=new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(),POSTGRES.getUsername(),POSTGRES.getPassword()));
        tx=new Tx(database.entityManagerFactory()); admission=new DocumentOperationUploadAdmission(tx,new DriveLedger(tx));
        selected=new DocumentSelectedAttemptLedger(tx);
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
        profile=new ManagedBackendLedger.Profile(S3BackendIdentity.of(S3.getEndpoint().toString(),S3.getRegion(),true),"selected-transfer-realm");
        new ManagedBackendLedger(tx).bind(GENERATION,profile);
    }
    @AfterAll static void close() throws Exception { if(opened!=null) opened.close(); if(database!=null) database.close(); }

    private record Fixture(DocumentPublicationCommand command,RepositoryOperationLedger.Owner owner,
            Map<UUID,DocumentUploadPlan.Placement> placements,DocumentSelectedAttemptLedger.Selected selection,
            DocumentPartAttemptLedger.PlannedObject planned,byte[] body) {}

    @Test void nonCorePartTransfersWithoutConstructingALegacyFullPlan() {
        var f=fixture(LEASE);
        var verified=transfer(f,opened.store());
        assertThat(verified.version()).isNotBlank();
        assertThat(verified.planned().part()).isNotEqualTo(DocumentPart.DOCUMENT_PART_CORE);
        assertThat(record(f,verified).state()).isEqualTo("STAGING"); // CORE has not been uploaded.
        assertThat(verifiedCount(f)).isEqualTo(1);
        assertThat(opened.store().getBounded(NAMESPACE,f.planned.objectKey(),verified.version(),f.body.length).data()).containsExactly(f.body);
    }

    @Test void replacementCommitsWhilePutIsHeldAndOldCompletionIsRejected() throws Exception {
        var f=fixture(Duration.ofSeconds(2));
        var committed=new CountDownLatch(1); var release=new CountDownLatch(1);
        var store=intercept((method,args,invoke) -> {
            Object result=invoke.call();
            if(method.equals("put")) { committed.countDown(); if(!release.await(15,TimeUnit.SECONDS)) throw new IllegalStateException("PUT gate timed out"); }
            return result;
        });
        try (var executor=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var pending=executor.submit(() -> record(f,transfer(f,store)));
            try {
                assertThat(committed.await(5,TimeUnit.SECONDS)).isTrue();
                UUID next=UUID.randomUUID();
                var replacement=executor.submit(() -> admission.retry(ADMIN,f.owner,
                        DocumentOperationUploadAdmission.prepare(f.command,f.placements,Map.of("member",next),LEASE),
                        Map.of("member",new DocumentOperationSelection.Expected(1,f.selection.attempt()))));
                assertThat(replacement.get(5,TimeUnit.SECONDS)).extracting(DocumentPartAttemptLedger.Attempt::id).containsExactly(next);
                assertThat(pending.isDone()).isFalse();
                release.countDown();
                assertThatThrownBy(() -> pending.get(5,TimeUnit.SECONDS)).hasStackTraceContaining("exact live selection");
            } finally { release.countDown(); }
        }
        assertThat(verifiedCount(f)).isZero();
        expire(f); recover(f);
    }

    @Test void lostPutAcknowledgementDoesNotInventVerificationOrRetry() {
        var f=fixture(Duration.ofSeconds(1));
        var puts=new java.util.concurrent.atomic.AtomicInteger(); var reads=new java.util.concurrent.atomic.AtomicInteger();
        var fault=new IllegalStateException("Injected lost PUT acknowledgement after adapter commit");
        var store=intercept((method,args,invoke) -> {
            Object result=invoke.call();
            if(method.equals("put")) { puts.incrementAndGet(); throw fault; }
            if(method.equals("getBounded")) reads.incrementAndGet();
            return result;
        });
        assertThatThrownBy(() -> transfer(f,store)).isInstanceOf(DocumentPartTransfer.Failure.class).hasCause(fault);
        assertThat(puts.get()).isEqualTo(1); assertThat(reads.get()).isZero(); assertThat(verifiedCount(f)).isZero();
        assertThat(opened.store().get(NAMESPACE,f.planned.objectKey()).data()).containsExactly(f.body);
        expire(f); recover(f);
    }

    @Test void latePutAfterAbsenceObservationIsRejectedAndReclaimedOnNextPass() throws Exception {
        var f=fixture(Duration.ofSeconds(1));
        var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
        var store=intercept((method,args,invoke) -> {
            if(method.equals("put")) { entered.countDown(); if(!release.await(15,TimeUnit.SECONDS)) throw new IllegalStateException("PUT gate timed out"); }
            return invoke.call();
        });
        try (var executor=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var pending=executor.submit(() -> record(f,transfer(f,store)));
            try {
                assertThat(entered.await(5,TimeUnit.SECONDS)).isTrue();
                expire(f); recover(f);
                assertThat(pending.isDone()).isFalse();
                release.countDown();
                assertThatThrownBy(() -> pending.get(5,TimeUnit.SECONDS)).hasStackTraceContaining("exact live selection");
            } finally { release.countDown(); }
        }
        assertThat(opened.store().get(NAMESPACE,f.planned.objectKey()).data()).containsExactly(f.body);
        assertThat(new DocumentAttemptCleanupLedger(tx).candidates(Duration.ZERO,100,GENERATION)).contains(f.selection.attempt());
        recover(f);
        assertThat(verifiedCount(f)).isZero();
    }

    private static DocumentPartTransfer.Verified transfer(Fixture f,BlobStore store) {
        // The fixture's private codec output was matched to the admitted declaration.
        return DocumentPartTransfer.upload(store,NAMESPACE,f.planned,f.body,Map.of(),() -> {},() -> {});
    }
    private static DocumentPartAttemptLedger.Attempt record(Fixture f,DocumentPartTransfer.Verified verified) {
        var p=verified.planned();
        return selected.verifyBatch(f.owner,f.selection,List.of(new DocumentSelectedAttemptLedger.Observation(
                p.objectKey(),p.size(),p.sha256(),p.contentType(),verified.version(),verified.etag())));
    }
    private static long verifiedCount(Fixture f) {
        return tx.readOnly(em -> ((Number)em.createNativeQuery("SELECT count(*) FROM document_part_attempt_objects WHERE attempt_id=:id AND verified")
                .setParameter("id",f.selection.attempt()).getSingleResult()).longValue());
    }
    private static void expire(Fixture f) {
        tx.readOnly(em -> em.createNativeQuery("SELECT CAST(pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.02) AS text) FROM document_part_attempts WHERE attempt_id=:id")
                .setParameter("id",f.selection.attempt()).getSingleResult());
    }
    private static void recover(Fixture f) {
        var recovery=new DocumentAttemptRecovery(new DocumentAttemptCleanupLedger(tx),(generation,retained) -> {
            assertThat(generation).isEqualTo(GENERATION); assertThat(retained).isEqualTo(profile);
            return opened.reclaimer();
        });
        var result=recovery.recover(f.selection.attempt(),Duration.ofSeconds(10));
        assertThat(result.failure()).isNull(); assertThat(result.outcome()).isEqualTo(DocumentAttemptRecovery.Outcome.ABSENT);
        assertThatThrownBy(() -> opened.store().get(NAMESPACE,f.planned.objectKey())).isInstanceOf(BlobStore.BlobNotFoundException.class);
    }
    @FunctionalInterface private interface Interceptor {
        Object call(String method,Object[] args,java.util.concurrent.Callable<Object> invoke) throws Exception;
    }
    private static BlobStore intercept(Interceptor interceptor) {
        return (BlobStore)java.lang.reflect.Proxy.newProxyInstance(BlobStore.class.getClassLoader(),new Class<?>[]{BlobStore.class},
                (proxy,method,args) -> interceptor.call(method.getName(),args,() -> {
                    try { return method.invoke(opened.store(),args); }
                    catch(java.lang.reflect.InvocationTargetException failure) {
                        if(failure.getCause() instanceof Exception exception) throw exception;
                        throw (Error)failure.getCause();
                    }
                }));
    }
    private static Fixture fixture(Duration attemptLease) {
        String docId=UUID.randomUUID().toString();
        var payloads=DocumentPartCodec.split(Document.newBuilder().setDocId(docId)
                .setSearchMetadata(SearchMetadata.newBuilder().addSemanticResults(SemanticProcessingResult.newBuilder().setResultId("result"))).build(),PartLayouts.document());
        assertThat(payloads).hasSizeGreaterThan(1);
        var drive=new DriveRecord(); drive.driveId=UUID.randomUUID(); drive.accountId="account"; drive.name="transfer-"+drive.driveId;
        drive.driveType="CUSTOM"; drive.provider="s3"; drive.bucket=NAMESPACE; new DriveLedger(tx).insert(drive);
        var placements=Map.of(drive.driveId,DocumentUploadPlan.Placement.sample(drive,GENERATION,profile));
        var member=DocumentPublicationMember.newBuilder().setMemberId("member").setDriveId(drive.driveId.toString())
                .setDestination(DocumentRevisionCondition.newBuilder().setIfAbsent(true).setAddress(NodeAddress.newBuilder()
                        .setAccountId("account").setGraphId("graph").setGraphAddressId("node").setDocId(docId)))
                .setOwnership(OwnershipContext.newBuilder().setAccountId("account").setDatasourceId("source").setSecurity(DocumentSecurity.getDefaultInstance()))
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE);
        for(var part:payloads) member.addParts(DocumentPublicationPart.newBuilder()
                .setSlot(DocumentPublicationSlot.newBuilder().setPart(part.part()).setSubKey(part.subKey()))
                .setUpload(PublicationUpload.newBuilder().setSizeBytes(part.bytes().length).setSha256(part.sha256()).setContentType("application/protobuf")));
        var command=new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1).setAccountId("account")
                .setOperationId(UUID.randomUUID().toString()).addMembers(member).build());
        var owner=new RepositoryOperationLedger(tx).admit(new RepositoryOperationLedger.Key("account","principal",command.operationId()),command,UUID.randomUUID(),LEASE).owner().orElseThrow();
        UUID id=UUID.randomUUID();
        var attempt=admission.admit(ADMIN,owner,DocumentOperationUploadAdmission.prepare(command,placements,Map.of("member",id),attemptLease)).getFirst();
        var plan=DocumentUploadPlan.prepare(command,placements,Map.of("member",id));
        return new Fixture(command,owner,placements,new DocumentSelectedAttemptLedger.Selected("member",1,id,attempt.token()),
                plan.members().getFirst().attempt().orElseThrow().uploads().getLast().object(),payloads.getLast().bytes().clone());
    }
}
