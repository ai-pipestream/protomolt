package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.registry.SchemaRegistryStore;
import ai.protomolt.proto.repo.admission.*;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.codec.*;
import ai.protomolt.proto.repo.container.ledger.*;
import ai.protomolt.proto.repo.schema.registry.RegistrySchemaResolver;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.schema.registry.git.GitSchemaRegistryStore;
import com.google.protobuf.*;
import java.lang.reflect.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/** Actual managed service, versioned provider and held Git registry load in the production-JAR host. */
public final class ManagedJournaledDrainProbe {
    private static final RepositoryCaller ADMIN = new RepositoryCaller("schema-owner", true);
    public static void run(Path bundle) throws Exception {
        run(bundle,0);
        run(bundle,1);
        run(bundle,2);
    }

    private static void run(Path bundle, int scenario) throws Exception {
        boolean recoveryEnabled=scenario!=0;
        var recoveryOperation=new java.util.concurrent.atomic.AtomicReference<DocumentPublicationCommand>();
        var recoveryCalls=new java.util.concurrent.atomic.AtomicInteger();
        var mutateAtCall=new java.util.concurrent.atomic.AtomicInteger(Integer.MAX_VALUE);
        var mutateInput=new java.util.concurrent.atomic.AtomicReference<Runnable>(() -> {});
        var mutated=new java.util.concurrent.atomic.AtomicBoolean();
        String generation = "journaled-schema-" + UUID.randomUUID();
        var config = new RepoServiceConfig(0, new LedgerConfig(System.getenv("PROTOMOLT_TEST_JDBC"),
                System.getenv("PROTOMOLT_TEST_USER"), System.getenv("PROTOMOLT_TEST_PASSWORD")),
                System.getenv("PROTOMOLT_TEST_S3_ENDPOINT"), System.getenv("PROTOMOLT_TEST_S3_REGION"),
                System.getenv("PROTOMOLT_TEST_S3_ACCESS"), System.getenv("PROTOMOLT_TEST_S3_SECRET"),
                "journaled-schema", 0, null, null, null, null, 0, 0L)
                .withManagedStorage(new ManagedStoragePolicy(generation, "schema-realm", true));
        var probeDirectory = Path.of(ManagedJournaledDrainProbe.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getParent();
        var directory = Files.createTempDirectory(probeDirectory, "managed-drain-schema-");
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var definition = definition(StringValue.getDescriptor());
        try (var store = GitSchemaRegistryStore.builder().repositoryDir(directory).build();
             var executor = Executors.newVirtualThreadPerTaskExecutor();
             var database = new LedgerDatabase(config.ledger())) {
            store.putDescriptorSet(definition.metadata().getArtifactSha256(), definition.descriptors());
            SchemaRegistryStore held = (SchemaRegistryStore) Proxy.newProxyInstance(SchemaRegistryStore.class.getClassLoader(),
                    new Class<?>[]{SchemaRegistryStore.class}, (proxy, method, args) -> {
                        if (method.getName().equals("descriptorSet")) {
                            entered.countDown();
                            if (!release.await(20, TimeUnit.SECONDS)) throw new IllegalStateException("registry gate timeout");
                        }
                        try { return method.invoke(store, args); }
                        catch (InvocationTargetException failure) { throw failure.getCause(); }
                    });
            var resolver = new RegistrySchemaResolver(held,
                    new DocumentSchemaArtifactCache.Limits(8_000_000, 16, 4_000_000), 4, 16);
            var access = new ManagedSchemaAccess() {
                public DocumentSchemaAdmission.Resolution open(RepositoryCaller caller, DocumentPublicationMember member,
                        RepositoryReadControl control) {
                    require(caller == ADMIN, "schema scope carries actual caller");
                    return resolver.open(occurrence -> new RegistrySchemaResolver.Selected(definition.metadata(), definition.source()), control::check);
                }
                public void close() { resolver.close(); }
                public boolean awaitIdle(Duration timeout) throws InterruptedException { return resolver.awaitLoads(timeout); }
            };
            var options = new ManagedPublicationOptions(bundle, Duration.ofMinutes(5), Duration.ofSeconds(5),
                    (account, principal, operation) -> {
                        require(principal.equals(ADMIN.principalName()), "exact drain principal");
                        return ADMIN;
                    });
            if (recoveryEnabled) options=options.withRecovery((account,principal,operation) -> {
                        var expected=recoveryOperation.get();
                        if (expected!=null && operation.equals(expected.operationId())
                                && account.equals(expected.intent().getAccountId()) && principal.equals(ADMIN.principalName())) {
                            if (recoveryCalls.incrementAndGet()==mutateAtCall.get()) {
                                mutateInput.get().run();
                                mutated.set(true);
                            }
                            return ADMIN;
                        }
                        throw new AssertionError("Fresh publication or terminal replay requested recovery authority");
                    });
            if (scenario==1) options=options.withTransport(new ManagedPublicationOptions.Transport(auth -> {
                require(auth.caller().name().equals(ADMIN.principalName()) && auth.caller().unrestricted()
                        && auth.binding().isEmpty(),"fixture binding preserves authenticated identity");
                return ADMIN;
            },32L*1024*1024,4));
            var host = RepoServices.build(config, BridgeEngine.standard(), null, access, options);
            var tx = new Tx(database.entityManagerFactory());
            try (var hosted=scenario==1 ? hostedPublication(host) : null) {
                if (scenario!=1) require(host.services().stream().noneMatch(service -> service instanceof DocumentPublicationGrpcService),
                        "library composition does not implicitly mount publication transport");
                var terminal = prepare(host, tx, generation, false);
                var completed = executeTransport(host, tx, terminal);
                require(completed.getMembersCount() == 1, "terminal control published");
                int largest=request(terminal).getPayloadsList().stream().mapToInt(payload -> payload.getContent().size()).max().orElseThrow();
                require(largest>1,"fixture has a nonempty bounded upload");
                var boundedReceipt=host.documentPublication().repository((caller,command,control) -> {
                    throw new AssertionError("Object-bound refusal or terminal replay selected storage");
                },largest);
                require(boundedReceipt.publishDocument(ADMIN,request(terminal),RepositoryReadControl.NONE)
                        .getCommitted().equals(completed),"exact object boundary preserves terminal replay");
                var tooSmall=host.documentPublication().repository((caller,command,control) -> {
                    throw new AssertionError("Oversized upload reached storage selection");
                },largest-1);
                for (boolean replay:List.of(false,true)) {
                    var oversized=request(terminal).toBuilder();
                    if (!replay) oversized.getIntentBuilder().setOperationId(UUID.randomUUID().toString());
                    try {
                        tooSmall.publishDocument(ADMIN,oversized.build(),RepositoryReadControl.NONE);
                        throw new AssertionError("Configured object bound accepted oversized upload");
                    } catch (IllegalArgumentException expected) {
                        require(expected.getMessage().contains("configured object limit"),"object-bound refusal");
                    }
                    if (!replay) {
                        UUID operation=UUID.fromString(oversized.getIntent().getOperationId());
                        require(count(tx,"repository_operation_owners",operation)==0,"object cap precedes owner admission");
                        require(count(tx,"repository_execution_claims",operation)==0,"object cap precedes claim admission");
                    }
                }
                var receiptOnly=host.documentPublication().repository((caller,command,control) -> {
                    throw new AssertionError("Terminal receipt selected host storage or schemas");
                });
                require(receiptOnly.publishDocument(ADMIN,request(terminal),RepositoryReadControl.NONE).getCommitted().equals(completed),
                        "shared facade returns exact terminal receipt without host selection");
                try {
                    receiptOnly.publishDocument(ADMIN,request(terminal).toBuilder().clearPayloads().build(),RepositoryReadControl.NONE);
                    throw new AssertionError("Terminal replay accepted incomplete uploads");
                } catch (IllegalArgumentException expected) { /* Shared input validation precedes receipt lookup. */ }
                try {
                    receiptOnly.publishDocument(ADMIN,request(terminal).toBuilder().setModes(0,
                            request(terminal).getModes(0).toBuilder().setMode(DocumentPublicationMode.DOCUMENT_PUBLICATION_MODE_TYPED)).build(),
                            RepositoryReadControl.NONE);
                    throw new AssertionError("Terminal replay accepted different modes");
                } catch (RepositoryException expected) {
                    require(expected.code()==RepositoryException.Code.FAILED_PRECONDITION,"terminal mode conflict");
                }
                require(host.publishDocument(ADMIN, terminal.command, Map.of(), Map.of(), Map.of(), Map.of(),
                        Optional.empty(), RepositoryReadControl.NONE).equals(completed), "terminal receipt replay");
                require(entered.getCount() == 1, "opaque terminal control did not resolve a schema");
                var foreign=new DriveRecord(); foreign.driveId=UUID.randomUUID(); foreign.accountId="foreign-"+UUID.randomUUID();
                foreign.name="private-drive"; foreign.driveType="PIPELINE"; foreign.bucket="private-bucket";
                foreign.provider="unsupported-private-provider";
                host.driveLedger().insert(foreign);
                for (var driveId:List.of(foreign.driveId,UUID.randomUUID())) {
                    var denied=request(terminal).toBuilder().setIntent(terminal.command.intent().toBuilder()
                            .setOperationId(UUID.randomUUID().toString()).setMembers(0,terminal.command.intent().getMembers(0)
                                    .toBuilder().setDriveId(driveId.toString()))).build();
                    try {
                        host.publicationRepository().publishDocument(ADMIN,denied,RepositoryReadControl.NONE);
                        throw new AssertionError("Publication selected a missing or foreign drive");
                    } catch (RepositoryException expected) {
                        require(expected.code()==RepositoryException.Code.NOT_FOUND,"foreign drive stays private before backend checks");
                    }
                    require(count(tx,"repository_operation_owners",UUID.fromString(denied.getIntent().getOperationId()))==0,
                            "invalid selection acquired no operation owner");
                }
                var work = prepare(host, tx, generation, true);
                if (scenario==2) {
                    recoveryOperation.set(work.command);
                    mutateInput.set(() -> work.bodies.values().iterator().next().bytes()[0]^=1);
                    release.countDown();
                    recoverExpired(host,tx,generation,bundle,work,() -> mutateAtCall.set(recoveryCalls.get()+2));
                    require(recoveryCalls.get()>0,"managed recovery requested exact process authority");
                    require(mutated.get(),"caller bytes changed after snapshot before recovery reservation");
                    host.close(Duration.ofSeconds(5));
                    require(resolver.cachedBytes()==0,"recovery host released schema cache");
                    System.out.println("MANAGED_EXPIRED_PUBLICATION_RECOVERY_OK");
                    return;
                }
                if (recoveryEnabled) {
                    var accepted=executor.submit(() -> hosted.client().publishDocument(request(work)).getCommitted());
                    require(entered.await(10,TimeUnit.SECONDS),"enabled host entered real schema lookup");
                    try { host.close(Duration.ofMillis(100)); throw new AssertionError("accepted publication was not retained"); }
                    catch (IllegalStateException expected) {
                        require(expected instanceof RepositoryDrainTimeoutException timeout
                                && timeout.phase()==RepositoryDrainTimeoutException.Phase.PUBLICATION_RPC,
                                "shutdown waits for accepted call");
                    }
                    require(!hosted.server().isShutdown(),"shutdown timeout retains publication listener");
                    try (var connection=host.ledgerDataSource().getConnection()) {
                        require(connection.isValid(1),"shutdown timeout retains repository SQL");
                    }
                    expectStatus(io.grpc.Status.Code.UNAVAILABLE,() -> hosted.client().publishDocument(request(terminal)));
                    require(!accepted.isDone() && !access.awaitIdle(Duration.ZERO),"schema worker remains accepted after close");
                    release.countDown();
                    var result=accepted.get(15,TimeUnit.SECONDS);
                    require(result.getMembersCount()==1,"accepted typed publication finished after close");
                    require(count(tx,"repository_operation_success",work.command.operationId())==1,"one durable publication outcome");
                    long versions=tx.readOnly(em -> ((Number)em.createNativeQuery("""
                            SELECT count(*) FROM document_operation_selections s
                            JOIN document_part_attempt_objects o ON o.attempt_id=s.attempt_id
                            WHERE s.operation_id=:id AND o.verified AND o.provider_version IS NOT NULL
                            """).setParameter("id",work.command.operationId()).getSingleResult()).longValue());
                    require(versions>0,"published revision selected verified provider versions");
                    host.close(Duration.ofSeconds(5));
                    require(hosted.server().isTerminated(),"successful drain closes publication listener");
                    require(count(tx,"repository_coordinator_local_drains",work.command.operationId())==0,
                            "terminal operation requires no drain marker");
                    require(resolver.cachedBytes()==0,"enabled host released schema cache");
                    try { host.ledgerDataSource().getConnection(); throw new AssertionError("closed enabled host still lends SQL"); }
                    catch (java.sql.SQLException expected) { /* Final drain releases the service-owned pool. */ }
                    System.out.println("MANAGED_RECOVERY_ACCEPTED_PUBLICATION_DRAIN_OK");
                    System.out.println("MANAGED_PUBLICATION_HOST_DRAIN_OK");
                    return;
                }
                var second = prepare(host, tx, generation, true);
                var cancelled = new java.util.concurrent.atomic.AtomicBoolean();
                var control = new RepositoryReadControl() {
                    public boolean isCancelled() { return cancelled.get(); }
                    public long remainingNanos() { return Long.MAX_VALUE; }
                };
                var operation = executor.submit(() -> host.publishDocument(ADMIN, work.command, work.placements, work.bodies,
                        Map.of(), work.modes, Optional.of(definition(Document.getDescriptor())), control));
                require(entered.await(10, TimeUnit.SECONDS), "real descriptor load entered");
                var secondOperation = executor.submit(() -> host.publishDocument(ADMIN, second.command, second.placements, second.bodies,
                        Map.of(), second.modes, Optional.of(definition(Document.getDescriptor())), control));
                long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                while (count(tx, "repository_operation_owners", second.command.operationId()) != 1) {
                    if (System.nanoTime() >= deadline) throw new AssertionError("second operation never admitted");
                    Thread.sleep(10);
                }
                cancelled.set(true);
                try { host.close(Duration.ofMillis(100)); throw new AssertionError("held schema worker reported complete shutdown"); }
                catch (IllegalStateException expected) {
                    if (!expected.getMessage().equals("Native publication resources still active; shared resources retained")) throw expected;
                }
                try { operation.get(5, TimeUnit.SECONDS); throw new AssertionError("closed schema admission returned publication"); }
                catch (ExecutionException expected) { require(expected.getCause() instanceof RepositoryException failure
                        && failure.code() == RepositoryException.Code.CANCELLED, "original publication cancellation retained"); }
                try { secondOperation.get(5, TimeUnit.SECONDS); throw new AssertionError("second cancelled operation returned publication"); }
                catch (ExecutionException expected) { require(expected.getCause() instanceof RepositoryException failure
                        && failure.code() == RepositoryException.Code.CANCELLED, "second publication cancellation retained"); }
                require(!access.awaitIdle(Duration.ZERO), "abandoned registry load is still active");
                require(count(tx, "repository_coordinator_drains", work.command.operationId()) == 1, "admission closure persisted");
                require(count(tx, "repository_coordinator_local_drains", work.command.operationId()) == 0, "no attestation with live schema worker");
                require(count(tx, "repository_coordinator_drains", second.command.operationId()) == 1, "second admission closure persisted");
                require(count(tx, "repository_coordinator_local_drains", second.command.operationId()) == 0, "second attestation awaits schema worker");
                try (var connection = host.ledgerDataSource().getConnection(); var statement = connection.createStatement();
                     var result = statement.executeQuery("SELECT 1")) { require(result.next(), "SQL retained while schema worker lives"); }
                release.countDown();
                require(access.awaitIdle(Duration.ofSeconds(5)), "registry worker drained");
                String ids = "'" + work.command.operationId() + "','" + second.command.operationId() + "'";
                tx.inTransaction(em -> {
                    em.createNativeQuery("""
                            CREATE FUNCTION test_managed_drain_batch() RETURNS trigger LANGUAGE plpgsql AS $$
                            BEGIN
                              IF NEW.operation_id IN (%s) AND EXISTS(SELECT 1 FROM repository_coordinator_local_drains WHERE operation_id IN (%s))
                              THEN RAISE EXCEPTION 'deliberate second local drain failure'; END IF;
                              RETURN NEW;
                            END $$
                            """.formatted(ids, ids)).executeUpdate();
                    em.createNativeQuery("CREATE TRIGGER test_managed_drain_batch BEFORE INSERT ON repository_coordinator_local_drains FOR EACH ROW EXECUTE FUNCTION test_managed_drain_batch()")
                            .executeUpdate();
                });
                java.util.List<?> committed;
                try {
                    try { host.close(Duration.ofSeconds(5)); throw new AssertionError("partial attestation reported complete shutdown"); }
                    catch (RuntimeException failure) {
                        boolean injected = false;
                        for (Throwable cause = failure; cause != null; cause = cause.getCause())
                            if (cause.getMessage() != null && cause.getMessage().contains("deliberate second local drain failure")) injected = true;
                        if (!injected) throw failure;
                    }
                    committed = tx.readOnly(em -> em.createNativeQuery("SELECT operation_id,recorded_at FROM repository_coordinator_local_drains WHERE operation_id IN (" + ids + ")").getResultList());
                    require(committed.size() == 1, "first local drain remains durable after second fails");
                    try (var connection = host.ledgerDataSource().getConnection(); var statement = connection.createStatement();
                         var result = statement.executeQuery("SELECT 1")) { require(result.next(), "partial attestation retains SQL"); }
                } finally {
                    tx.inTransaction(em -> {
                        em.createNativeQuery("DROP TRIGGER test_managed_drain_batch ON repository_coordinator_local_drains").executeUpdate();
                        em.createNativeQuery("DROP FUNCTION test_managed_drain_batch()").executeUpdate();
                    });
                }
                host.close(Duration.ofSeconds(5));
                require(count(tx, "repository_coordinator_local_drains", work.command.operationId()) == 1, "complete host drain attested");
                require(count(tx, "repository_coordinator_local_drains", second.command.operationId()) == 1, "second drain attested after retry");
                var first = (Object[]) committed.getFirst();
                Object timestamp = tx.readOnly(em -> em.createNativeQuery("SELECT recorded_at FROM repository_coordinator_local_drains WHERE operation_id=:id")
                        .setParameter("id", first[0]).getSingleResult());
                require(timestamp.equals(first[1]), "retry confirms the original first marker");
                require(count(tx, "repository_coordinator_drains", terminal.command.operationId()) == 0,
                        "completed session excluded from drain snapshot");
                require(count(tx, "repository_coordinator_local_drains", terminal.command.operationId()) == 0,
                        "completed session needs no local-drain marker");
                require(resolver.cachedBytes() == 0, "schema cache released");
                try { host.ledgerDataSource().getConnection(); throw new AssertionError("closed host still lends SQL"); }
                catch (java.sql.SQLException expected) { /* Host now releases its own pool. */ }
                require(store.descriptorSet(definition.metadata().getArtifactSha256()).isPresent(), "borrowed registry remains available");
            } finally { release.countDown(); host.close(); }
        }
        System.out.println("MANAGED_JOURNALED_SCHEMA_DRAIN_OK");
    }

    private record HostedPublication(io.grpc.Server server, io.grpc.ManagedChannel channel,
            DocumentPublicationServiceGrpc.DocumentPublicationServiceBlockingStub client) implements AutoCloseable {
        @Override public void close() throws Exception {
            channel.shutdownNow();
            require(channel.awaitTermination(10,TimeUnit.SECONDS),"managed publication client stopped");
        }
    }

    private static HostedPublication hostedPublication(RepoServices host) {
        String name=io.grpc.inprocess.InProcessServerBuilder.generateName();
        try { host.startInProcess(name); throw new AssertionError("publication listener accepted missing operator token"); }
        catch (IllegalArgumentException expected) { /* Refusal happens before listener creation. */ }
        require(host.services().stream().anyMatch(service -> service instanceof DocumentPublicationGrpcService),
                "publication mounted independently of historical reads");
        require(host.services().stream().noneMatch(service -> service instanceof DocumentHistoryGrpcService),
                "history remains disabled");
        String token="publication-fixture-"+UUID.randomUUID();
        var server=host.startInProcess(name,"operator-fixture-"+UUID.randomUUID(), supplied -> supplied.equals(token)
                ? Optional.of(new ai.protomolt.proto.actions.Caller(ADMIN.principalName(),Set.of(),true)) : Optional.empty());
        var channel=io.grpc.inprocess.InProcessChannelBuilder.forName(name).build();
        var plain=DocumentPublicationServiceGrpc.newBlockingStub(channel).withDeadlineAfter(30,TimeUnit.SECONDS);
        expectStatus(io.grpc.Status.Code.UNAUTHENTICATED,() -> plain.publishDocument(PublishDocumentRequest.getDefaultInstance()));
        var headers=new io.grpc.Metadata(); headers.put(io.grpc.Metadata.Key.of("api_token",io.grpc.Metadata.ASCII_STRING_MARSHALLER),token);
        return new HostedPublication(server,channel,plain.withInterceptors(io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(headers)));
    }
    private static void recoverExpired(RepoServices host, Tx tx, String generation, Path bundle, Work work,
            Runnable armMutation) throws Exception {
        var profile=new ManagedBackendLedger(tx).find(generation).orElseThrow();
        var budget=new ai.protomolt.proto.repo.blob.spi.PayloadBudget(64_000_000);
        var timeouts=new SqlTimeouts(Duration.ofSeconds(2),Duration.ofSeconds(5));
        var ledger=new DocumentReadLedger(tx,UUID.randomUUID());
        var opened=BlobStores.discover().open("s3",Map.ofEntries(
                Map.entry("endpoint", System.getenv("PROTOMOLT_TEST_S3_ENDPOINT")),
                Map.entry("region", System.getenv("PROTOMOLT_TEST_S3_REGION")),
                Map.entry("path-style", "true"),
                Map.entry("conditional-writes", "true"),
                Map.entry("access-key", System.getenv("PROTOMOLT_TEST_S3_ACCESS")),
                Map.entry("secret-key", System.getenv("PROTOMOLT_TEST_S3_SECRET")),
                Map.entry("credentials-mode", "static"),
                Map.entry("api-call-timeout-ms", "300000"),
                Map.entry("api-attempt-timeout-ms", "60000"),
                Map.entry("connection-timeout-ms", "10000"),
                Map.entry("socket-timeout-ms", "60000")));
        var reader=new ai.protomolt.proto.repo.engine.DocumentPartReader((original,selected) -> {
            require(original.equals(generation) && selected.equals(profile),"predecessor exact read backend");
            return opened.store();
        },2,8_000_000,budget);
        var predecessor=DocumentPublicationRuntime.managedJournaled(tx,new DriveLedger(tx),ledger,reader,budget,
                (original,selected) -> {
                    require(original.equals(generation) && selected.equals(profile),"predecessor exact upload backend");
                    return new DocumentPublicationRuntime.Backend(profile.identity(),opened);
                },new DocumentRevisionAssembly.Limits(8_000_000,100,100,10000,1_000_000),timeouts,
                2,Duration.ofMillis(25),Duration.ofSeconds(10),2,4_000_000,10,false,
                new DocumentPublicationRuntime.Assessments(bundle,Duration.ofMinutes(5),Duration.ofSeconds(5)),
                (account,principal,operation) -> ADMIN,new DocumentPublicationRuntime.ExternalWorkers() {
                    public void closeAdmission() { }
                    public boolean awaitIdle(Duration timeout) { return true; }
                });
        Throwable primaryFailure=null;
        try {
            var interrupted=new IllegalStateException("predecessor descriptor selection interrupted");
            try {
                predecessor.execute(ADMIN,work.command,work.placements,work.bodies,Map.of(),work.modes,
                        Optional.of(definition(Document.getDescriptor())),(caller,member,occurrence) -> { throw interrupted; },
                        RepositoryReadControl.NONE);
                throw new AssertionError("predecessor unexpectedly published");
            } catch (IllegalStateException expected) { require(expected==interrupted,"original schema failure preserved"); }
            require(count(tx,"repository_publication_assessment_starts",work.command.operationId())==0,
                    "schema failure preceded assessment creation");
            require(count(tx,"repository_operation_success",work.command.operationId())==0,"predecessor did not publish");
            require(count(tx,"repository_coordinator_drains",work.command.operationId())==0,"predecessor not gracefully drained");
            long verified=tx.readOnly(em -> ((Number)em.createNativeQuery("""
                    SELECT count(*) FROM document_operation_selections s JOIN document_part_attempt_objects o ON o.attempt_id=s.attempt_id
                    WHERE s.operation_id=:id AND o.verified AND o.provider_version IS NOT NULL
                    """).setParameter("id",work.command.operationId()).getSingleResult()).longValue());
            require(verified>0,"predecessor performed real versioned uploads");
            tx.readOnly(em -> em.createNativeQuery("""
                    SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM
                     (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.1)
                    FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                    WHERE c.operation_id=:id
                    """).setParameter("id",work.command.operationId()).getSingleResult());
            var corrupt=new HashMap<>(work.bodies);
            var first=corrupt.entrySet().iterator().next();
            var body=first.getValue();
            byte[] changed=body.bytes().clone();
            changed[0]^=1;
            corrupt.put(first.getKey(),new PartObject(body.part(),body.subKey(),changed,body.sha256()));
            try {
                execute(host,ADMIN,new Work(work.command,work.placements,corrupt,work.modes));
                throw new AssertionError("Corrupt resubmission advanced recovery");
            } catch (IllegalArgumentException expected) {
                require(expected.getMessage().contains("Recovery payload checksum"),"private copy checksum rejected");
            }
            require(count(tx,"repository_coordinator_reservations",work.command.operationId())==0
                    && count(tx,"repository_successor_installs",work.command.operationId())==0
                    && count(tx,"repository_successor_executions",work.command.operationId())==0,
                    "corrupt resubmission did not change durable ownership");
            // The next authority lookup discovers state; the following one occurs
            // after private preflight and immediately before reservation.
            armMutation.run();
            var result=execute(host,ADMIN,work);
            require(result.getMembersCount()==1,"managed successor published");
            for (String table : List.of("repository_coordinator_reservations","repository_successor_installs",
                    "repository_successor_executions","repository_operation_success"))
                require(count(tx,table,work.command.operationId())==1,"exact single recovery transition: "+table);
            require(host.publishDocument(ADMIN,work.command,Map.of(),Map.of(),Map.of(),Map.of(),Optional.empty(),
                    RepositoryReadControl.NONE).equals(result),"managed successor receipt replay");
        } catch (Exception | Error failure) {
            primaryFailure=failure;
            throw failure;
        } finally {
            // Keep the independently opened client alive until the old owner proves its own drain.
            try {
                require(predecessor.shutdownStep(Duration.ofSeconds(5)),"predecessor drains through reviewed recovery chain");
                opened.close();
                require(count(tx,"repository_coordinator_drains",work.command.operationId())==0
                        && count(tx,"repository_coordinator_local_drains",work.command.operationId())==0,
                        "fenced predecessor did not assert graceful quiescence");
            } catch (Exception | Error cleanup) {
                if (primaryFailure==null) throw cleanup;
                if (cleanup!=primaryFailure) primaryFailure.addSuppressed(cleanup);
            }
        }
    }

    private static long count(Tx tx, String table, UUID operation) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE operation_id=:id")
                .setParameter("id", operation).getSingleResult()).longValue());
    }
    private static void require(boolean condition, String message) { if (!condition) throw new AssertionError(message); }
    private record Work(DocumentPublicationCommand command, Map<UUID, DocumentPublicationRuntime.Placement> placements,
            Map<DocumentPublicationRuntime.PayloadKey, PartObject> bodies, Map<String, DocumentPublicationRuntime.Mode> modes) {}

    private static DocumentPublicationResult execute(RepoServices host, RepositoryCaller caller, Work work) throws Exception {
        return host.publishDocument(caller, work.command, work.placements, work.bodies, Map.of(), work.modes,
                work.modes.get("document") == DocumentPublicationRuntime.Mode.TYPED ? Optional.of(definition(Document.getDescriptor())) : Optional.empty(),
                RepositoryReadControl.NONE);
    }

    /** Fixture authentication over a real transport; this does not qualify production API-key setup. */
    private static DocumentPublicationResult executeTransport(RepoServices host, Tx tx, Work work) throws Exception {
        var budget=new ai.protomolt.proto.repo.blob.spi.PayloadBudget(32L*1024*1024);
        var repository=host.publicationRepository();
        var credential=new RepositoryCredentialBinding("transport-fixture",UUID.randomUUID(),1);
        credentialAdministration(tx,credential,"register");
        var preserveCredential=new java.util.concurrent.atomic.AtomicBoolean();
        var fault=new java.util.concurrent.atomic.AtomicReference<String>("");
        var entered=new CountDownLatch(1);
        var cancelled=new CountDownLatch(1);
        var release=new CountDownLatch(1);
        var received=new java.util.concurrent.atomic.AtomicReference<RepositoryCaller>();
        var repositoryCalls=new java.util.concurrent.atomic.AtomicInteger();
        var corruptWire=new java.util.concurrent.atomic.AtomicBoolean();
        // Faults surround the real repository, never synthesize a successful outcome.
        DocumentPublicationRepository controlled=(caller,input,control) -> {
            repositoryCalls.incrementAndGet();
            received.set(caller);
            String mode=fault.get();
            if (mode.equals("wait")) {
                entered.countDown();
                try {
                    long limit=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
                    while (!control.isCancelled() && System.nanoTime()<limit) Thread.sleep(5);
                    require(control.isCancelled(),"transport propagated cancellation to active producer");
                    cancelled.countDown();
                    require(release.await(10,TimeUnit.SECONDS),"test released cancelled producer");
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt(); throw new IllegalStateException(interrupted);
                }
                control.check();
            }
            var result=repository.publishDocument(caller,input,control);
            return switch (mode) {
                case "committed" -> result.toBuilder().setCommitted(result.getCommitted().toBuilder().setAccountId("wrong-account")).build();
                case "rejected" -> PublishDocumentResponse.newBuilder().setRejected(DocumentPublicationRejection.getDefaultInstance()).build();
                case "unexpected" -> throw new IllegalStateException("private-provider-location");
                case "conflict" -> throw new RepositoryException(RepositoryException.Code.CONFLICT,"controlled conflict after replay");
                case "unsupported" -> throw new RepositoryException(RepositoryException.Code.UNSUPPORTED,"controlled unsupported operation after replay");
                default -> result;
            };
        };
        var service=new DocumentPublicationGrpcService(controlled, auth -> new RepositoryCaller(
                auth.caller().name(),auth.caller().unrestricted(),auth.caller().unrestricted() ? Set.of() : Set.of(work.command.intent().getAccountId()),
                Set.of(),preserveCredential.get() ? auth.binding().map(binding -> new RepositoryCredentialBinding(
                        binding.issuer(),binding.credentialId(),binding.generation())) : Optional.empty()),budget,1);
        var identity=io.grpc.Metadata.Key.of("test-publication-identity",io.grpc.Metadata.ASCII_STRING_MARSHALLER);
        io.grpc.ServerInterceptor authentication=new io.grpc.ServerInterceptor() {
            @SuppressWarnings("unchecked")
            @Override public <Q,S> io.grpc.ServerCall.Listener<Q> interceptCall(io.grpc.ServerCall<Q,S> call,
                    io.grpc.Metadata headers,io.grpc.ServerCallHandler<Q,S> next) {
                var value=headers.get(identity);
                var context=io.grpc.Context.current();
                if (value!=null) {
                    var caller=new ai.protomolt.proto.actions.Caller(ADMIN.principalName(),Set.of(),!value.equals("bound"));
                    var auth=new ai.protomolt.proto.authz.AuthenticatedCaller(caller,value.equals("bound")
                            ? Optional.of(new ai.protomolt.proto.authz.CredentialBinding(credential.issuer(),credential.credentialId(),credential.generation())) : Optional.empty());
                    context=context.withValue(ai.protomolt.proto.authz.grpc.CallerContexts.CALLER,caller)
                            .withValue(ai.protomolt.proto.authz.grpc.CallerContexts.AUTHENTICATED_CALLER,auth);
                }
                var delivery=new io.grpc.ForwardingServerCall.SimpleForwardingServerCall<Q,S>(call) {
                    @Override public void sendMessage(S message) {
                        if (corruptWire.get() && message instanceof PublishDocumentResponse response && response.hasCommitted())
                            message=(S)response.toBuilder().setCommitted(response.getCommitted().toBuilder().setAccountId("wire-corruption")).build();
                        super.sendMessage(message);
                    }
                };
                return io.grpc.Contexts.interceptCall(context,delivery,headers,next);
            }
        };
        String name=io.grpc.inprocess.InProcessServerBuilder.generateName();
        try (var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var server=io.grpc.inprocess.InProcessServerBuilder.forName(name).executor(executor)
                    .maxInboundMessageSize(DocumentPublicationInput.MAX_ENVELOPE_BYTES)
                    .intercept(authentication).addService(service).build().start();
            var channel=io.grpc.inprocess.InProcessChannelBuilder.forName(name).build();
            try {
                var plain=DocumentPublicationServiceGrpc.newBlockingStub(channel).withDeadlineAfter(30,TimeUnit.SECONDS);
                expectStatus(io.grpc.Status.Code.UNAUTHENTICATED,() -> plain.publishDocument(request(work)));
                var boundHeaders=new io.grpc.Metadata(); boundHeaders.put(identity,"bound");
                var bound=plain.withInterceptors(io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(boundHeaders));
                expectStatus(io.grpc.Status.Code.PERMISSION_DENIED,() -> bound.publishDocument(request(work)));
                var headers=new io.grpc.Metadata(); headers.put(identity,"process");
                var authenticated=plain.withInterceptors(io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(headers));
                var clientBudget=new ai.protomolt.proto.repo.blob.spi.PayloadBudget(32L*1024*1024);
                var remote=new ai.protomolt.proto.repo.publication.grpc.RemoteDocumentPublicationRepository(ADMIN,
                        DocumentPublicationServiceGrpc.newFutureStub(channel).withInterceptors(
                                io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(headers)),clientBudget,Duration.ofSeconds(30),1);
                int beforeClientChecks=repositoryCalls.get();
                expectRepositoryCode(RepositoryException.Code.PERMISSION_DENIED,() -> remote.publishDocument(
                        new RepositoryCaller("different",true),request(work),RepositoryReadControl.NONE));
                expectRepositoryCode(RepositoryException.Code.CANCELLED,() -> remote.publishDocument(ADMIN,request(work),new RepositoryReadControl() {
                    public boolean isCancelled() { return true; }
                    public long remainingNanos() { return Long.MAX_VALUE; }
                }));
                var expired=new ai.protomolt.proto.repo.publication.grpc.RemoteDocumentPublicationRepository(ADMIN,
                        DocumentPublicationServiceGrpc.newFutureStub(channel).withDeadlineAfter(-1,TimeUnit.SECONDS),
                        clientBudget,Duration.ofSeconds(30),1);
                expectRepositoryCode(RepositoryException.Code.DEADLINE_EXCEEDED,
                        () -> expired.publishDocument(ADMIN,request(work),RepositoryReadControl.NONE));
                require(repositoryCalls.get()==beforeClientChecks,"client identity and cancellation checks precede network calls");
                expectStatus(io.grpc.Status.Code.INVALID_ARGUMENT,() -> authenticated.publishDocument(PublishDocumentRequest.getDefaultInstance()));
                var response=remote.publishDocument(ADMIN,request(work),RepositoryReadControl.NONE);
                require(response.hasCommitted(),"transport published a committed receipt");
                require(repository.publishDocument(ADMIN,request(work),RepositoryReadControl.NONE).equals(response),
                        "library replay equals fresh transport receipt");
                require(authenticated.publishDocument(request(work)).equals(response),"transport replay equals library receipt");
                require(remote.publishDocument(ADMIN,request(work),RepositoryReadControl.NONE).equals(response),"remote SPI replay equals library receipt");
                expectStatus(io.grpc.Status.Code.INVALID_ARGUMENT,() -> authenticated.publishDocument(request(work).toBuilder().clearPayloads().build()));
                awaitBudgetRelease(budget);
                corruptWire.set(true);
                try { remote.publishDocument(ADMIN,request(work),RepositoryReadControl.NONE); throw new AssertionError("remote client accepted corrupt wire receipt"); }
                catch (RepositoryException invalid) {
                    require(invalid.code()==RepositoryException.Code.DATA_LOSS && invalid.getMessage().equals("Invalid remote publication receipt"),
                            "client independently checks receipt correspondence after server validation");
                } finally { corruptWire.set(false); }
                awaitBudgetRelease(budget);
                for (var mapping : Map.of("conflict",RepositoryException.Code.CONFLICT,"unsupported",RepositoryException.Code.UNSUPPORTED).entrySet()) {
                    fault.set(mapping.getKey());
                    expectRepositoryCode(mapping.getValue(),() -> remote.publishDocument(ADMIN,request(work),RepositoryReadControl.NONE));
                    awaitBudgetRelease(budget);
                }
                fault.set("");
                preserveCredential.set(true);
                var scopedCaller=new RepositoryCaller(ADMIN.principalName(),false,Set.of(work.command.intent().getAccountId()),Set.of(),Optional.of(credential));
                var scopedRemote=new ai.protomolt.proto.repo.publication.grpc.RemoteDocumentPublicationRepository(scopedCaller,
                        DocumentPublicationServiceGrpc.newFutureStub(channel).withInterceptors(
                                io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(boundHeaders)),clientBudget,Duration.ofSeconds(30),1);
                require(scopedRemote.publishDocument(scopedCaller,request(work),RepositoryReadControl.NONE).equals(response),"scoped authenticated replay preserves receipt");
                require(received.get().credentialBinding().equals(Optional.of(credential)),"SPI received exact scoped credential");
                awaitBudgetRelease(budget);
                credentialAdministration(tx,credential,"revoke");
                expectRepositoryCode(RepositoryException.Code.UNAUTHENTICATED,
                        () -> scopedRemote.publishDocument(scopedCaller,request(work),RepositoryReadControl.NONE));
                awaitBudgetRelease(budget);
                for (String mode : List.of("committed","rejected")) {
                    fault.set(mode);
                    expectStatus(io.grpc.Status.Code.DATA_LOSS,() -> authenticated.publishDocument(request(work)));
                    awaitBudgetRelease(budget);
                }
                fault.set("unexpected");
                try { authenticated.publishDocument(request(work)); throw new AssertionError("Expected sanitized failure"); }
                catch (io.grpc.StatusRuntimeException failure) {
                    require(failure.getStatus().getCode()==io.grpc.Status.Code.INTERNAL
                            && "Publication failed".equals(failure.getStatus().getDescription()),"unexpected failure is sanitized");
                }
                awaitBudgetRelease(budget);
                fault.set("wait");
                var cancelClient=new java.util.concurrent.atomic.AtomicBoolean();
                var future=executor.submit(() -> remote.publishDocument(ADMIN,request(work),new RepositoryReadControl() {
                    public boolean isCancelled() { return cancelClient.get(); }
                    public long remainingNanos() { return Long.MAX_VALUE; }
                }));
                try {
                    require(entered.await(10,TimeUnit.SECONDS),"producer entered before cancellation");
                    cancelClient.set(true);
                    try { future.get(10,TimeUnit.SECONDS); throw new AssertionError("client cancellation returned a receipt"); }
                    catch (ExecutionException failure) {
                        require(failure.getCause() instanceof RepositoryException cancelledCall
                                && cancelledCall.code()==RepositoryException.Code.CANCELLED,"client cancellation returns repository status");
                    }
                    require(cancelled.await(10,TimeUnit.SECONDS),"producer observed cancellation");
                    require(budget.reservedBytes()>0,"active cancelled producer retains its byte reservation");
                    expectStatus(io.grpc.Status.Code.RESOURCE_EXHAUSTED,() -> authenticated.publishDocument(request(work)));
                } finally { release.countDown(); }
                awaitBudgetRelease(budget);
                fault.set("");
                require(authenticated.publishDocument(request(work)).equals(response),"cancelled call released slot for exact replay");
                require(remote.publishDocument(ADMIN,request(work),RepositoryReadControl.NONE).equals(response),"remote client can replay after cancellation");
                require(clientBudget.reservedBytes()==0,"client released input and response reservations");
                awaitBudgetRelease(budget);
                verifyNetworkParser(service,authentication,headers,request(work),response,repositoryCalls);
                awaitBudgetRelease(budget);
                System.out.println("MANAGED_PUBLICATION_TRANSPORT_PARITY_OK");
                return response.getCommitted();
            } finally {
                release.countDown();
                channel.shutdownNow(); server.shutdownNow();
                require(channel.awaitTermination(10,TimeUnit.SECONDS),"publication channel stopped");
                require(server.awaitTermination(10,TimeUnit.SECONDS),"publication server stopped");
            }
        }
    }

    private static void verifyNetworkParser(DocumentPublicationGrpcService service, io.grpc.ServerInterceptor authentication,
            io.grpc.Metadata headers, PublishDocumentRequest request, PublishDocumentResponse receipt,
            java.util.concurrent.atomic.AtomicInteger repositoryCalls) throws Exception {
        try (var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var server=io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
                    .forAddress(new java.net.InetSocketAddress("127.0.0.1",0)).executor(executor)
                    .maxInboundMessageSize(DocumentPublicationInput.MAX_ENVELOPE_BYTES)
                    .intercept(authentication).addService(service).build().start();
            var channel=io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder.forAddress("127.0.0.1",server.getPort())
                    .usePlaintext().build();
            try {
                var client=DocumentPublicationServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10,TimeUnit.SECONDS)
                        .withInterceptors(io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(headers));
                require(client.publishDocument(request).equals(receipt),"Netty replay equals original receipt");
                int before=repositoryCalls.get();
                var oversized=request.toBuilder().setPayloads(0,request.getPayloads(0).toBuilder()
                        .setContent(ByteString.copyFrom(new byte[DocumentPublicationInput.MAX_ENVELOPE_BYTES+1]))).build();
                expectStatus(io.grpc.Status.Code.RESOURCE_EXHAUSTED,() -> client.publishDocument(oversized));
                require(repositoryCalls.get()==before,"oversized wire request never reached repository");
            } finally {
                channel.shutdownNow(); server.shutdownNow();
                require(channel.awaitTermination(10,TimeUnit.SECONDS),"Netty publication channel stopped");
                require(server.awaitTermination(10,TimeUnit.SECONDS),"Netty publication server stopped");
            }
        }
    }

    private static void awaitBudgetRelease(ai.protomolt.proto.repo.blob.spi.PayloadBudget budget) throws Exception {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
        while (budget.reservedBytes()!=0 && System.nanoTime()<deadline) Thread.sleep(10);
        require(budget.reservedBytes()==0,"transport released delivery reservations after completion");
    }

    /** Invoke the internal authority for fixture setup without creating a public provisioning API. */
    private static void credentialAdministration(Tx tx, RepositoryCredentialBinding credential, String action) throws Exception {
        var type=Class.forName("ai.protomolt.proto.repo.container.ledger.RepositoryCredentialAuthorities");
        var constructor=type.getDeclaredConstructor(Tx.class); constructor.setAccessible(true);
        var method=type.getDeclaredMethod(action,RepositoryCaller.class,RepositoryCredentialBinding.class,String.class);
        method.setAccessible(true);
        method.invoke(constructor.newInstance(tx),ADMIN,credential,ADMIN.principalName());
    }

    private static void expectStatus(io.grpc.Status.Code expected, Runnable action) {
        try { action.run(); throw new AssertionError("Expected gRPC status "+expected); }
        catch (io.grpc.StatusRuntimeException failure) {
            require(failure.getStatus().getCode()==expected,"Expected "+expected+", got "+failure.getStatus());
        }
    }

    private static void expectRepositoryCode(RepositoryException.Code expected, Runnable action) {
        try { action.run(); throw new AssertionError("Expected repository status "+expected); }
        catch (RepositoryException failure) { require(failure.code()==expected,"Expected "+expected+", got "+failure.code()); }
    }

    private static DocumentPublicationResult executeFacade(RepoServices host, RepositoryCaller caller, Work work) {
        var repository=host.publicationRepository();
        var response=repository.publishDocument(caller,request(work),RepositoryReadControl.NONE);
        require(response.hasCommitted(),"facade published a committed receipt");
        return response.getCommitted();
    }

    private static PublishDocumentRequest request(Work work) {
        var request=PublishDocumentRequest.newBuilder().setIntent(work.command.intent());
        work.modes.forEach((member,mode) -> request.addModes(DocumentPublicationMemberMode.newBuilder().setMemberId(member)
                .setMode(mode==DocumentPublicationRuntime.Mode.TYPED ? DocumentPublicationMode.DOCUMENT_PUBLICATION_MODE_TYPED
                        : DocumentPublicationMode.DOCUMENT_PUBLICATION_MODE_OPAQUE)));
        work.bodies.forEach((key,body) -> request.addPayloads(DocumentPublicationPayload.newBuilder().setMemberId(key.member())
                .setRevisionOrdinal(key.revisionOrdinal()).setContent(ByteString.copyFrom(body.bytes()))));
        return request.build();
    }

    private static Work prepare(RepoServices host, Tx tx, String generation, boolean typed) throws Exception {
        String account = "account-" + UUID.randomUUID();
        String namespace = "schema-" + UUID.randomUUID();
        try (var backing = BlobStores.discover().open("s3", Map.ofEntries(
                Map.entry("endpoint", System.getenv("PROTOMOLT_TEST_S3_ENDPOINT")),
                Map.entry("region", System.getenv("PROTOMOLT_TEST_S3_REGION")),
                Map.entry("path-style", "true"),
                Map.entry("conditional-writes", "true"),
                Map.entry("access-key", System.getenv("PROTOMOLT_TEST_S3_ACCESS")),
                Map.entry("secret-key", System.getenv("PROTOMOLT_TEST_S3_SECRET")),
                Map.entry("credentials-mode", "static"),
                Map.entry("api-call-timeout-ms", "300000"),
                Map.entry("api-attempt-timeout-ms", "60000"),
                Map.entry("connection-timeout-ms", "10000"),
                Map.entry("socket-timeout-ms", "60000")))) { backing.ensureNamespace(namespace); }
        try (var client=software.amazon.awssdk.services.s3.S3Client.builder()
                .endpointOverride(java.net.URI.create(System.getenv("PROTOMOLT_TEST_S3_ENDPOINT")))
                .region(software.amazon.awssdk.regions.Region.of(System.getenv("PROTOMOLT_TEST_S3_REGION")))
                .forcePathStyle(true)
                .httpClientBuilder(software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient.builder())
                .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                        software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(
                                System.getenv("PROTOMOLT_TEST_S3_ACCESS"),System.getenv("PROTOMOLT_TEST_S3_SECRET")))).build()) {
            client.putBucketVersioning(request -> request.bucket(namespace).versioningConfiguration(
                    configuration -> configuration.status(software.amazon.awssdk.services.s3.model.BucketVersioningStatus.ENABLED)));
        }
        var drive = new DriveRecord();
        drive.driveId = UUID.randomUUID(); drive.accountId = account; drive.name = "schema"; drive.driveType = "PIPELINE"; drive.bucket = namespace;
        host.driveLedger().insert(drive);
        var profile = new ManagedBackendLedger(tx).find(generation).orElseThrow();
        var policy = DocumentAdmissionPolicy.of(DocumentSchemaPolicy.newBuilder()
                .setEncodingVersion(1).setAccountId(account).setMode(typed ? DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_TYPED_REQUIRED
                        : DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_OPAQUE_ALLOWED).setAnyResolvedSchema(true)
                .setValidationProfile("protomolt-retained-schema-admission/v1")
                .setLimits(DocumentSchemaPolicyLimits.newBuilder().setMaxFragments(32).setMaxFragmentBytes(4_000_000)
                        .setMaxRoots(100).setMaxEvidenceBytes(4_000_000).setMaxBindings(20).setMaxRetainedBytes(16_000_000)
                        .setMaxDecodedBytes(1_000_000)).build(), () -> {});
        // Trusted fixture administration through the real guarded policy catalog.
        // The managed service does not yet expose a policy-administration API.
        tx.inTransaction(em -> {
            em.createNativeQuery("SELECT lock_document_schema_policy_account(:account,true)").setParameter("account", account).getSingleResult();
            em.createNativeQuery("""
                    INSERT INTO document_schema_policies(account_id,policy_sha256,policy_codec,policy_version,policy_bytes)
                    VALUES(:account,:sha,:codec,:version,:bytes)
                    """).setParameter("account", account).setParameter("sha", HexFormat.of().parseHex(policy.sha256()))
                    .setParameter("codec", DocumentAdmissionPolicy.CODEC).setParameter("version", DocumentAdmissionPolicy.VERSION)
                    .setParameter("bytes", policy.bytes().toByteArray()).executeUpdate();
            em.createNativeQuery("""
                    INSERT INTO document_schema_policy_current(account_id,policy_sha256,policy_revision)
                    VALUES(:account,:sha,1)
                    """).setParameter("account", account).setParameter("sha", HexFormat.of().parseHex(policy.sha256())).executeUpdate();
        });
        var security = DocumentSecurity.newBuilder().addPermissions(AccessRule.newBuilder().setIdentityType("public").setIdentity("public").setAccess(ai.protomolt.proto.repo.v1.Access.ACCESS_READ))
                .addPermissions(AccessRule.newBuilder().setIdentityType("public").setIdentity("public").setAccess(ai.protomolt.proto.repo.v1.Access.ACCESS_WRITE)).build();
        var ownership = OwnershipContext.newBuilder().setAccountId(account).setDatasourceId("source").setSecurity(security).build();
        var documentBuilder = Document.newBuilder().setDocId("document").setOwnership(ownership);
        if (typed) documentBuilder.setStructuredData(Any.pack(StringValue.of("managed registry payload"), "type.test"));
        var document = documentBuilder.build();
        var member = DocumentPublicationMember.newBuilder().setMemberId("document").setDriveId(drive.driveId.toString()).setOwnership(ownership)
                .setRowKind(DocumentPublicationRowKind.DOCUMENT_PUBLICATION_ROW_KIND_PIPELINE).setDestination(DocumentRevisionCondition.newBuilder()
                        .setIfAbsent(true).setAddress(NodeAddress.newBuilder().setAccountId(account).setDocId("document").setGraphId("graph").setGraphAddressId("node")));
        var bodies = new HashMap<DocumentPublicationRuntime.PayloadKey, PartObject>();
        for (var part : DocumentPartCodec.split(document, PartLayouts.document())) {
            bodies.put(new DocumentPublicationRuntime.PayloadKey("document", member.getPartsCount()), part);
            member.addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder().setPart(part.part()).setSubKey(part.subKey()))
                    .setUpload(PublicationUpload.newBuilder().setSizeBytes(part.bytes().length).setSha256(DocumentPartCodec.sha256Hex(part.bytes())).setContentType("application/protobuf")));
        }
        var command = new DocumentPublicationCommand(DocumentPublicationIntent.newBuilder().setEncodingVersion(1).setAccountId(account)
                .setOperationId(UUID.randomUUID().toString()).addMembers(member).build());
        return new Work(command, Map.of(drive.driveId, new DocumentPublicationRuntime.Placement(drive, generation, profile)), bodies,
                Map.of("document", typed ? DocumentPublicationRuntime.Mode.TYPED : DocumentPublicationRuntime.Mode.OPAQUE));
    }

    private static DocumentSchemaAdmission.Definition definition(Descriptors.Descriptor type) {
        var closure = DescriptorFingerprints.closure(type); var bytes = closure.toByteString();
        var metadata = RepositorySchemaAsset.newBuilder().setTypeUrl("type.test/" + type.getFullName())
                .setArtifactSha256(DocumentPartCodec.sha256Hex(bytes.toByteArray()))
                .setSchema(PublicationSchemaCondition.newBuilder().setTypeName(type.getFullName()).setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure)))
                .setCompilation(SchemaCompilationProvenance.newBuilder().setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                        .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN).setUnknownCompilerReason("fixture compiler unknown")
                        .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("test-runtime").setVersion("1"))).build();
        return new DocumentSchemaAdmission.Definition(metadata, bytes, Optional.empty());
    }
}
