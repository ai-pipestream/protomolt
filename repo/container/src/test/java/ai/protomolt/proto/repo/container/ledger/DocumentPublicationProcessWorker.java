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
        boolean scoped = args[0].startsWith("scoped-");
        String mode = scoped ? args[0].substring(7) : args[0];
        assertThat(mode).isIn("write", "recover", "reserve", "install", "initial-before", "initial-after", "publish", "recover-initial", "recover-activated");
        var command = new DocumentPublicationCommand(DocumentPublicationIntent.parseFrom(Files.readAllBytes(Path.of(args[1]))));
        assertThat(command.intent().getMembersList()).hasSize(1);
        var caller = scoped ? scopedCaller(command) : ADMIN;
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
                     Map.of("hibernate.connection.datasource", admissionGate(pool,mode,command.operationId()), "hibernate.hbm2ddl.auto", "validate"));
             var sdk = client(); var opened = opened(sdk, mode.equals("write"))) {
            var tx = new Tx(emf);
            var profile = new ManagedBackendLedger(tx).find(GENERATION).orElseThrow();
            var drive = new DriveLedger(tx).findById(UUID.fromString(command.intent().getMembers(0).getDriveId())).orElseThrow();
            var input = new Input(command, Map.of(drive.driveId, DocumentUploadPlan.Placement.sample(drive, GENERATION, profile)), body);
            try (var host = new Host(tx, profile, new AtomicReference<>(opened), Duration.ofSeconds(10))) {
                if (mode.equals("write") || mode.startsWith("initial-")) {
                    execute(host, input, caller);
                    throw new AssertionError("Writer must be killed while the real PUT is held");
                }
                if (mode.equals("publish")) {
                    var result=execute(host,input,caller);
                    int calls=((ObservedStore)opened.store()).calls().get();
                    assertThat(host.sessions.execute(caller,command,Map.of(),Map.of(),Map.of(),Map.of(),Optional.empty(),
                            (member,occurrence) -> {throw new AssertionError("Replay cannot resolve schema");},NONE)).isEqualTo(result);
                    assertThat(((ObservedStore)opened.store()).calls().get()).isEqualTo(calls);
                } else recover(tx, host, input, opened, mode, caller, Path.of(args[2]).resolveSibling("activation-release"));
            }
        }
        System.out.println("PROCESS_RECOVERY_OK");
    }

    static RepositoryCaller scopedCaller(DocumentPublicationCommand command) {
        return new RepositoryCaller("principal", false, Set.of(command.intent().getAccountId()), Set.of(),
                Optional.of(new RepositoryCredentialBinding("process-recovery-test", command.operationId(), 1)));
    }

    private static DocumentPublicationResult execute(Host host, Input input, RepositoryCaller caller) throws Exception {
        return host.sessions.execute(caller, input.command(), input.placements(),
                Map.of(new DocumentUploadPayloads.Key("a", 0), input.body()), Map.of(),
                Map.of("a", DocumentPublicationCandidate.Mode.OPAQUE), Optional.empty(),
                (member, occurrence) -> { throw new AssertionError("Opaque fixture cannot resolve schemas"); }, NONE);
    }

    private static void recover(Tx tx, Host host, Input input, OpenedBlobStore backend, String mode, RepositoryCaller caller, Path activationRelease) throws Exception {
        var command = input.command();
        var key = new RepositoryOperationLedger.Key("account", "principal", command.operationId());
        boolean initial=mode.equals("recover-initial");
        if(initial) assertThat(tx.<Number>readOnly(em -> (Number) em.createNativeQuery(
                "SELECT count(*) FROM document_part_attempts WHERE operation_id=:id")
                .setParameter("id",command.operationId()).getSingleResult()).longValue()).isZero();
        var oldAttempt = initial ? null : tx.readOnly(em -> (UUID) em.createNativeQuery("""
                SELECT attempt_id FROM document_part_attempts WHERE operation_id=:id
                """).setParameter("id", command.operationId()).getSingleResult());
        var oldObject = initial ? null : tx.readOnly(em -> (String) em.createNativeQuery(
                "SELECT object_key FROM document_part_attempt_objects WHERE attempt_id=:id")
                .setParameter("id", oldAttempt).getSingleResult());
        if(!initial) assertThat(backend.store().get(BUCKET, oldObject).data()).containsExactly(input.body().bytes());
        tx.readOnly(em -> em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM
                  (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.1)
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.operation_id=:id
                """).setParameter("id", key.operationId()).getSingleResult());
        // Production discovery returns private identities without adopting or renewing them.
        var observed = new RepositoryCoordinatorRecoveryDiscovery(tx, new SqlTimeouts(Duration.ofSeconds(1),Duration.ofSeconds(5)))
                .inspect(ADMIN,key,command.sha256(),NONE);
        if(initial) {
            assertThat(observed.status()).isEqualTo(RepositoryCoordinatorRecoveryDiscovery.Status.EXPIRED_BOUND);
            assertThat(observed.candidate().orElseThrow().predecessor().epoch()).isEqualTo(1);
            assertThat(observed.candidate().orElseThrow().owner().generation()).isEqualTo(1);
        }
        RepositoryCoordinatorReservation.Proposal proposal;
        if (observed.status() == RepositoryCoordinatorRecoveryDiscovery.Status.EXPIRED_BOUND) {
            var found = observed.candidate().orElseThrow();
            proposal = new RepositoryCoordinatorReservation.ExpiredUnquiesced(found.predecessor(), UUID.randomUUID(),
                    host.sessions.coordinatorIdentity(), Duration.ofSeconds(10), found.owner());
            RepositoryCoordinatorExpiration.reserve(tx, ADMIN,
                    (RepositoryCoordinatorReservation.ExpiredUnquiesced) proposal, NONE);
        } else {
            var found = observed.unactivated().orElseThrow();
            proposal = new RepositoryCoordinatorReservation.SupersededUnactivated(found.predecessor(), UUID.randomUUID(),
                    host.sessions.coordinatorIdentity(), Duration.ofSeconds(10), found.owner(),
                    found.preparationSha256(), found.installation());
            RepositoryCoordinatorSupersession.reserve(tx, ADMIN,
                    (RepositoryCoordinatorReservation.SupersededUnactivated) proposal, NONE);
        }
        if (mode.equals("reserve")) holdReplacement();
        var budget = new PayloadBudget(64_000_000);
        try (var loaded = new RepositoryReservedPreparation(tx, budget,
                new SqlTimeouts(Duration.ofSeconds(1), Duration.ofSeconds(5)))
                .load(ADMIN, caller, proposal, RepositoryCoordinatorReservation.owner(proposal).orElseThrow(), NONE)) {
            var plan = RepositorySuccessorInstall.prepare(proposal, loaded.record(), Duration.ofSeconds(10),
                    Map.of("a", DocumentPublicationCandidate.Mode.OPAQUE));
            RepositorySuccessorInstall.install(tx, host.budget, ADMIN, plan, NONE);
            if (mode.equals("install")) holdReplacement();
            host.sessions.activateSuccessor(ADMIN, caller, plan, NONE);
            if (mode.equals("recover-activated")) {
                System.out.println("SUCCESSOR_ACTIVATED"); System.out.flush();
                long deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
                while (!Files.exists(activationRelease) && System.nanoTime() < deadline) Thread.sleep(10);
                assertThat(Files.exists(activationRelease)).as("parent released activation barrier").isTrue();
            }
            var result = execute(host, input, caller);
            var nextAttempt = plan.next().seeds().attempts().get("a");
            assertThat(nextAttempt).isNotEqualTo(loaded.record().seeds().attempts().get("a"));
            assertThat(plan.next().seeds().uploadTokens().get("a")).isNotEqualTo(loaded.record().seeds().uploadTokens().get("a"));
            if(!initial) assertThat(nextAttempt).isNotEqualTo(oldAttempt);
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
            if(!initial) assertThat(tx.<Integer>readOnly(em -> ((Number) em.createNativeQuery("""
                    SELECT count(*) FROM document_revision_parts p JOIN document_part_attempt_objects o ON p.object_id=o.physical_object_id
                    WHERE o.attempt_id=:attempt
                    """).setParameter("attempt", oldAttempt).getSingleResult()).intValue())).isZero();
            int calls = ((ObservedStore) backend.store()).calls().get();
            assertThat(host.sessions.execute(caller, command, Map.of(), Map.of(), Map.of(), Map.of(), Optional.empty(),
                    (member, occurrence) -> { throw new AssertionError("Receipt replay cannot resolve schemas"); }, NONE)).isEqualTo(result);
            assertThat(((ObservedStore) backend.store()).calls().get()).isEqualTo(calls);
        }
        assertThat(budget.reservedBytes()).isZero();
        for (String table : List.of("repository_coordinator_drains", "repository_coordinator_local_drains")) {
            assertThat(tx.<Integer>readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE operation_id=:id AND claim_epoch=1")
                    .setParameter("id", key.operationId()).getSingleResult()).intValue())).isZero();
        }
    }

    /** Hold an actual owner transaction at the JDBC commit boundary, before any provider work. */
    private static javax.sql.DataSource admissionGate(javax.sql.DataSource source,String mode,UUID operation) {
        if(!mode.startsWith("initial-")) return source;
        var ready=new java.util.concurrent.atomic.AtomicBoolean();
        var after=DocumentJdbcFaults.afterCommit(source,() -> {
            if(mode.equals("initial-after") && ready.compareAndSet(true,false)) holdInitial();
        });
        return DocumentJdbcFaults.beforeCommit(after,connection -> {
            try(var query=connection.prepareStatement("""
                    SELECT count(*) FROM repository_execution_claims c
                    JOIN repository_coordinator_bindings b USING(account_id,principal,operation_id)
                    JOIN repository_publication_preparations p USING(account_id,principal,operation_id)
                    JOIN repository_publication_modes m USING(account_id,principal,operation_id)
                    JOIN repository_operations r USING(account_id,principal,operation_id)
                    JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                    WHERE c.operation_id=? AND c.claim_epoch=1 AND o.owner_generation=1
                    """)) {
                query.setObject(1,operation);
                try(var rows=query.executeQuery()) {
                    rows.next();
                    if(rows.getInt(1)==1) {
                        ready.set(true);
                        if(mode.equals("initial-before")) holdInitial();
                    }
                }
            }
        });
    }

    private static void holdInitial() throws java.sql.SQLException {
        System.out.println("INITIAL_COMMIT_HELD");System.out.flush();
        try {new CountDownLatch(1).await();}
        catch(InterruptedException e) {Thread.currentThread().interrupt();throw new java.sql.SQLException("Initial commit gate interrupted",e);}
        throw new AssertionError("Initial writer must be killed");
    }

    private static void holdReplacement() throws InterruptedException {
        System.out.println("REPLACEMENT_HELD"); System.out.flush();
        new CountDownLatch(1).await();
        throw new AssertionError("Replacement must be killed before activation");
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
