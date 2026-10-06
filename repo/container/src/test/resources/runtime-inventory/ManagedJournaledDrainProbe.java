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
            var journaled = new ManagedDocumentServices.Journaled(
                    new DocumentPublicationRuntime.Assessments(bundle, Duration.ofMinutes(5), Duration.ofSeconds(5)),
                    (account, principal, operation) -> {
                        require(principal.equals(ADMIN.principalName()), "exact drain principal");
                        return ADMIN;
                    });
            var host = new RepoServices(config, BridgeEngine.standard(), BlobStores.discover(), null, access, null, journaled);
            var tx = new Tx(database.entityManagerFactory());
            try {
                var terminal = prepare(host, tx, generation, false);
                var completed = execute(host, ADMIN, terminal);
                require(completed.getMembersCount() == 1, "terminal control published");
                require(host.publishDocument(ADMIN, terminal.command, Map.of(), Map.of(), Map.of(), Map.of(),
                        Optional.empty(), RepositoryReadControl.NONE).equals(completed), "terminal receipt replay");
                require(entered.getCount() == 1, "opaque terminal control did not resolve a schema");
                var work = prepare(host, tx, generation, true);
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

    private static Work prepare(RepoServices host, Tx tx, String generation, boolean typed) throws Exception {
        String account = "account-" + UUID.randomUUID();
        String namespace = "schema-" + UUID.randomUUID();
        try (var backing = BlobStores.discover().open("s3", Map.of("endpoint", System.getenv("PROTOMOLT_TEST_S3_ENDPOINT"),
                "region", System.getenv("PROTOMOLT_TEST_S3_REGION"), "path-style", "true", "conditional-writes", "true",
                "access-key", System.getenv("PROTOMOLT_TEST_S3_ACCESS"), "secret-key", System.getenv("PROTOMOLT_TEST_S3_SECRET")))) { backing.ensureNamespace(namespace); }
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
