package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.s3.*;
import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.codec.*;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.zaxxer.hikari.*;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import static ai.protomolt.proto.repo.container.ledger.DocumentSuccessorLatePutIT.*;
import static org.assertj.core.api.Assertions.*;

/** Test-only process recovery driver. SQL discovery here is not a production recovery API. */
public final class DocumentPublicationProcessWorker {
    static final RepositoryCaller ADMIN = new RepositoryCaller("principal", true);
    static final RepositoryReadControl NONE = RepositoryReadControl.NONE;
    public static void main(String[] args) throws Exception {
        assertThat(args).hasSize(3);
        assertThat(args[0]).isIn("write", "recover");
        var command = new DocumentPublicationCommand(DocumentPublicationIntent.parseFrom(Files.readAllBytes(Path.of(args[1]))));
        assertThat(command.intent().getMembersList()).hasSize(1);
        var declared = command.intent().getMembers(0).getPartsList();
        assertThat(declared).hasSize(1);
        assertThat(declared.getFirst().hasUpload()).isTrue();
        var bytes = Files.readAllBytes(Path.of(args[2]));
        var digest = HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes));
        assertThat(digest).isEqualTo(declared.getFirst().getUpload().getSha256());
        var slot = declared.getFirst().getSlot();
        var body = new PartObject(slot.getPart(), slot.getSubKey(), bytes, digest);
        var env = System.getenv();
        var config = new HikariConfig();
        config.setJdbcUrl(env.get("TEST_DB_URL")); config.setUsername(env.get("TEST_DB_USER"));
        config.setPassword(env.get("TEST_DB_PASSWORD")); config.setSchema(env.get("TEST_DB_SCHEMA")); config.setMaximumPoolSize(6);
        try (var pool = new HikariDataSource(config);
             var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                     Map.of("hibernate.connection.datasource", pool, "hibernate.hbm2ddl.auto", "validate"));
             var sdk = client(); var opened = opened(sdk, args[0].equals("write"))) {
            var tx = new Tx(emf);
            var profile = new ManagedBackendLedger(tx).find(GENERATION).orElseThrow();
            var drive = new DriveLedger(tx).findById(UUID.fromString(command.intent().getMembers(0).getDriveId())).orElseThrow();
            var input = new Input(command, Map.of(drive.driveId, DocumentUploadPlan.Placement.sample(drive, GENERATION, profile)), body);
            try (var host = new Host(tx, profile, new AtomicReference<>(opened), Duration.ofSeconds(10))) {
                if (args[0].equals("write")) {
                    execute(host, input, NONE);
                    throw new AssertionError("Writer must be killed while the real PUT is held");
                }
                recover(tx, host, input, opened);
            }
        }
        System.out.println("PROCESS_RECOVERY_OK");
    }

    private static void recover(Tx tx, Host host, Input input, OpenedBlobStore backend) throws Exception {
        var command = input.command();
        var key = new RepositoryOperationLedger.Key("account", "principal", command.operationId());
        var oldAttempt = tx.readOnly(em -> (UUID) em.createNativeQuery("""
                SELECT attempt_id FROM document_part_attempts WHERE operation_id=:id
                """).setParameter("id", command.operationId()).getSingleResult());
        var oldObject = tx.readOnly(em -> (String) em.createNativeQuery(
                "SELECT object_key FROM document_part_attempt_objects WHERE attempt_id=:id")
                .setParameter("id", oldAttempt).getSingleResult());
        assertThat(backend.store().get(BUCKET, oldObject).data()).containsExactly(input.body().bytes());
        tx.readOnly(em -> em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM
                  (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.1)
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.operation_id=:id
                """).setParameter("id", key.operationId()).getSingleResult());
        // Production discovery returns private identities without adopting or renewing them.
        var observed = new RepositoryCoordinatorRecoveryDiscovery(tx, new SqlTimeouts(Duration.ofSeconds(1),Duration.ofSeconds(5)))
                .inspect(ADMIN,key,command.sha256(),NONE);
        assertThat(observed.status()).isEqualTo(RepositoryCoordinatorRecoveryDiscovery.Status.EXPIRED_BOUND);
        var found = observed.candidate().orElseThrow();
        long epoch = found.predecessor().epoch();
        assertThat(epoch).isEqualTo(1);
        var proposal = new RepositoryCoordinatorReservation.ExpiredUnquiesced(found.predecessor(), UUID.randomUUID(),
                host.sessions.coordinatorIdentity(), Duration.ofMinutes(5), found.owner());
        RepositoryCoordinatorExpiration.reserve(tx, ADMIN, proposal, NONE);
        var claim = tx.inTransaction(em -> { return RepositoryExecutionClaimLedger.lockLive(em, key, command.sha256(), epoch+1, proposal.successorToken()); });
        var budget = new PayloadBudget(64_000_000);
        try (var loaded = new DocumentPublicationPreparationJournal(tx, budget).load(ADMIN, claim, 0, NONE).orElseThrow()) {
            var plan = RepositorySuccessorInstall.prepare(proposal, loaded.record(), Duration.ofMinutes(5),
                    Map.of("a", DocumentPublicationCandidate.Mode.OPAQUE));
            RepositorySuccessorInstall.install(tx, host.budget, ADMIN, plan, NONE);
            host.sessions.activateSuccessor(ADMIN, ADMIN, plan, NONE);
            var result = execute(host, input, NONE);
            var nextAttempt = plan.next().seeds().attempts().get("a");
            assertThat(nextAttempt).isNotEqualTo(oldAttempt);
            var object = tx.readOnly(em -> (Object[]) em.createNativeQuery(
                    "SELECT object_key,provider_version FROM document_part_attempt_objects WHERE attempt_id=:id AND verified")
                    .setParameter("id", nextAttempt).getSingleResult());
            assertThat(object[0]).isNotEqualTo(oldObject); assertThat((String) object[1]).isNotBlank();
            assertThat(backend.store().getBounded(BUCKET, (String) object[0], (String) object[1], input.body().bytes().length).data())
                    .containsExactly(input.body().bytes());
            assertThat(tx.<Integer>readOnly(em -> ((Number) em.createNativeQuery("""
                    SELECT count(*) FROM document_revision_parts p JOIN document_part_attempt_objects o ON p.object_id=o.physical_object_id
                    WHERE p.revision_id=:revision AND o.attempt_id=:attempt
                    """).setParameter("revision", UUID.fromString(result.getMembers(0).getRevisionId()))
                    .setParameter("attempt", nextAttempt).getSingleResult()).intValue())).isEqualTo(1);
            assertThat(tx.<Integer>readOnly(em -> ((Number) em.createNativeQuery("""
                    SELECT count(*) FROM document_revision_parts p JOIN document_part_attempt_objects o ON p.object_id=o.physical_object_id
                    WHERE o.attempt_id=:attempt
                    """).setParameter("attempt", oldAttempt).getSingleResult()).intValue())).isZero();
            int calls = ((ObservedStore) backend.store()).calls().get();
            assertThat(host.sessions.execute(ADMIN, command, Map.of(), Map.of(), Map.of(), Map.of(), Optional.empty(),
                    (member, occurrence) -> { throw new AssertionError("Receipt replay cannot resolve schemas"); }, NONE)).isEqualTo(result);
            assertThat(((ObservedStore) backend.store()).calls().get()).isEqualTo(calls);
        }
        assertThat(budget.reservedBytes()).isZero();
        for (String table : List.of("repository_coordinator_drains", "repository_coordinator_local_drains")) {
            assertThat(tx.<Integer>readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE operation_id=:id AND claim_epoch=1")
                    .setParameter("id", key.operationId()).getSingleResult()).intValue())).isZero();
        }
    }

    static S3Client client() {
        var env = System.getenv();
        return S3Client.builder().endpointOverride(URI.create(env.get("TEST_S3_ENDPOINT"))).region(Region.of(env.get("TEST_S3_REGION")))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(env.get("TEST_S3_ACCESS"), env.get("TEST_S3_SECRET"))))
                .forcePathStyle(true).overrideConfiguration(c -> c.apiCallTimeout(Duration.ofSeconds(15))).build();
    }
    private static OpenedBlobStore opened(S3Client sdk, boolean hold) {
        BlobStore store = new ObservedStore(new S3BlobStore(sdk), hold, new java.util.concurrent.atomic.AtomicInteger());
        return new OpenedBlobStore(store, () -> {}, Set.of(BlobCapability.NON_EXPIRING_WRITES, BlobCapability.BOUNDED_READ,
                BlobCapability.PHYSICAL_RECLAMATION), new S3NamespaceProvisioner(sdk), new S3ObjectReclaimer(sdk));
    }
    /** Delegates actual provider operations; holds only the return from a completed real PUT. */
    private record ObservedStore(BlobStore delegate, boolean held, java.util.concurrent.atomic.AtomicInteger calls) implements BlobStore {
        private PutResult hold(PutResult result) {
            if (!held) return result;
            assertThat(result.versionId()).isNotBlank();
            System.out.println("REAL_PUT_HELD"); System.out.flush();
            try { new CountDownLatch(1).await(); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException("Unexpected writer interruption", e); }
            throw new AssertionError("Latch cannot release");
        }
        public PutResult put(PutSpec spec, byte[] body) { calls.incrementAndGet(); return hold(delegate.put(spec, body)); }
        public PutResult put(PutSpec spec, InputStream body, long size) { calls.incrementAndGet(); return hold(delegate.put(spec, body, size)); }
        public GetResult get(String bucket, String key, String version) { calls.incrementAndGet(); return delegate.get(bucket, key, version); }
        public GetResult getBounded(String bucket, String key, String version, int limit) { calls.incrementAndGet(); return delegate.getBounded(bucket, key, version, limit); }
        public void copy(String a, String b, String c, String d) { calls.incrementAndGet(); delegate.copy(a,b,c,d); }
        public boolean delete(String bucket, String key) { calls.incrementAndGet(); return delegate.delete(bucket,key); }
        public BatchDeleteResult deleteAll(String bucket, List<String> keys) { calls.incrementAndGet(); return delegate.deleteAll(bucket,keys); }
        public List<ListedObject> list(String bucket, String prefix) { calls.incrementAndGet(); return delegate.list(bucket,prefix); }
        public void headBucket(String bucket) { calls.incrementAndGet(); delegate.headBucket(bucket); }
        public void headObject(String bucket, String key) { calls.incrementAndGet(); delegate.headObject(bucket,key); }
    }
}
