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
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
@org.junit.jupiter.api.parallel.Execution(org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD)
class ManagedSchemaHostIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer STORAGE = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    @TempDir Path directory;
    private static final RepositoryCaller ADMIN = new RepositoryCaller("schema-owner", true);

    private static RepoServiceConfig config(String id) {
        return new RepoServiceConfig(0, new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                STORAGE.getEndpoint().toString(), STORAGE.getRegion(), STORAGE.getAccessKey(), STORAGE.getSecretKey(),
                "schema-host", 0, null, null, null, null, 0, 0L)
                .withManagedStorage(new ManagedStoragePolicy(id, "schema-realm", true));
    }

    private static final class Access implements ManagedSchemaAccess {
        final RegistrySchemaResolver resolver;
        final DocumentSchemaAdmission.Definition definition;
        final List<RepositoryCaller> callers = new CopyOnWriteArrayList<>();
        final AtomicInteger closes = new AtomicInteger();
        volatile boolean allowed = true;
        Access(SchemaRegistryStore store, DocumentSchemaAdmission.Definition definition) {
            this.definition = definition;
            resolver = new RegistrySchemaResolver(store, new DocumentSchemaArtifactCache.Limits(8_000_000, 16, 4_000_000), 4, 16);
        }
        public DocumentSchemaAdmission.Resolution open(RepositoryCaller caller, DocumentPublicationMember member, RepositoryReadControl control) {
            return resolver.open(occurrence -> {
                assertThat(member.getOwnership().getAccountId()).isNotBlank();
                assertThat(occurrence.typeUrl()).isEqualTo(definition.metadata().getTypeUrl());
                if (!allowed) throw new SecurityException("Schema grant revoked");
                callers.add(caller);
                return new RegistrySchemaResolver.Selected(definition.metadata(), definition.source());
            }, control::check);
        }
        public void close() { closes.incrementAndGet(); resolver.close(); }
        public boolean awaitIdle(Duration timeout) throws InterruptedException { return resolver.awaitLoads(timeout); }
    }

    @Test void nativeTypedOpaqueAndReplayUseOptionalHostScopesAndPreserveScopedCaller() throws Exception {
        var definition = definition(StringValue.getDescriptor());
        String generation = "schema-" + UUID.randomUUID();
        var config = config(generation);
        try (var store = GitSchemaRegistryStore.builder().repositoryDir(directory).build()) {
            store.putDescriptorSet(definition.metadata().getArtifactSha256(), definition.descriptors());
            var access = new Access(store, definition);
            try (var host = RepoServices.build(config, BridgeEngine.standard(), null, access);
                 var database = new LedgerDatabase(config.ledger())) {
                var tx = new Tx(database.entityManagerFactory());
                for (boolean typed : new boolean[] {true, false}) {
                    var work = prepare(host, tx, generation, typed);
                    var result = execute(host, ADMIN, work);
                    assertThat(result.getMembersCount()).isEqualTo(1);
                    int selections = access.callers.size();
                    assertThat(selections).isEqualTo(typed ? 1 : 2);
                    assertThat(host.publishDocument(ADMIN, work.command, Map.of(), Map.of(), Map.of(), Map.of(),
                            Optional.empty(), RepositoryReadControl.NONE)).isEqualTo(result);
                    assertThat(access.callers).hasSize(selections);
                    if (typed) {
                        assertThat(access.callers.getFirst()).isSameAs(ADMIN);
                        var member = work.command.intent().getMembers(0);
                        var command = new DocumentPublicationCommand(work.command.intent().toBuilder()
                                .setOperationId(UUID.randomUUID().toString()).setMembers(0, member.toBuilder().setDestination(
                                        member.getDestination().toBuilder().clearIfAbsent()
                                                .setExpectedMutationRevision(result.getMembers(0).getMutationRevision()))).build());
                        var scoped = new RepositoryCaller("schema-owner", false, Set.of(member.getOwnership().getAccountId()), Set.of());
                        access.allowed = false;
                        var update = new Work(command, work.placements, work.bodies, work.modes);
                        assertThatThrownBy(() -> execute(host, scoped, update)).isInstanceOf(SecurityException.class)
                                .hasMessage("Schema grant revoked");
                        access.allowed = true;
                        execute(host, scoped, new Work(command, work.placements, work.bodies, work.modes));
                        assertThat(access.callers.getLast()).isSameAs(scoped);
                        assertThat(access.callers.getLast().processAuthority()).isFalse();
                    }
                }
                assertThat(host.services()).noneMatch(service -> service.getClass().getSimpleName().contains("Publication"));
            }
            assertThat(access.closes.get()).isPositive();
            assertThat(access.resolver.cachedBytes()).isZero();
            assertThat(store.descriptorSet(definition.metadata().getArtifactSha256())).contains(definition.descriptors());
        }
    }

    @Test void shutdownRetainsSharedResourcesUntilAbandonedRegistryReadCompletes() throws Exception {
        var definition = definition(StringValue.getDescriptor());
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var store = GitSchemaRegistryStore.builder().repositoryDir(directory).build();
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            store.putDescriptorSet(definition.metadata().getArtifactSha256(), definition.descriptors());
            SchemaRegistryStore held = (SchemaRegistryStore) Proxy.newProxyInstance(SchemaRegistryStore.class.getClassLoader(),
                    new Class<?>[] {SchemaRegistryStore.class}, (proxy, method, args) -> {
                        if (method.getName().equals("descriptorSet")) {
                            entered.countDown();
                            if (!release.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("fixture provider release timed out");
                        }
                        try { return method.invoke(store, args); }
                        catch (InvocationTargetException failure) { throw failure.getCause(); }
                    });
            var access = new Access(held, definition);
            String generation = "held-" + UUID.randomUUID();
            var config = config(generation);
            var host = RepoServices.build(config, BridgeEngine.standard(), null, access);
            try (var database = new LedgerDatabase(config.ledger())) {
                var work = prepare(host, new Tx(database.entityManagerFactory()), generation, true);
                var operation = executor.submit(() -> execute(host, ADMIN, work));
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(() -> host.close(Duration.ofMillis(100))).isInstanceOf(IllegalStateException.class);
                assertThatThrownBy(() -> operation.get(5, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
                assertThat(access.awaitIdle(Duration.ZERO)).isFalse();
                try (var connection = host.ledgerDataSource().getConnection(); var statement = connection.createStatement();
                     var result = statement.executeQuery("SELECT 1")) { assertThat(result.next()).isTrue(); }
                assertThatThrownBy(() -> execute(host, ADMIN, work)).isInstanceOf(IllegalStateException.class);
                release.countDown();
                assertThat(access.awaitIdle(Duration.ofSeconds(5))).isTrue();
                host.close();
                assertThat(access.resolver.cachedBytes()).isZero();
                assertThatThrownBy(() -> host.ledgerDataSource().getConnection()).isInstanceOf(java.sql.SQLException.class);
                assertThat(store.descriptorSet(definition.metadata().getArtifactSha256())).contains(definition.descriptors());
            } finally { release.countDown(); host.close(); }
        }
    }

    @Test void rejectedOptInLeavesSchemaLifecycleWithCaller() throws Exception {
        try (var store = GitSchemaRegistryStore.builder().repositoryDir(directory).build()) {
            var definition = definition(StringValue.getDescriptor());
            store.putDescriptorSet(definition.metadata().getArtifactSha256(), definition.descriptors());
            var access = new Access(store, definition);
            try {
                var config = config("unused").withManagedStorage(ManagedStoragePolicy.disabled());
                assertThatThrownBy(() -> RepoServices.build(config, BridgeEngine.standard(), null, access))
                        .isInstanceOf(IllegalArgumentException.class).hasMessage("Schema resolution requires qualified managed storage");
                assertThat(access.closes.get()).isZero();
                try (var scope = access.resolver.open(occurrence -> new RegistrySchemaResolver.Selected(definition.metadata(), definition.source()), () -> {})) {
                    assertThat(scope).isNotNull();
                }
            } finally { access.close(); assertThat(access.awaitIdle(Duration.ofSeconds(5))).isTrue(); }
        }
    }

    @Test void qualifiedStartupFailureLeavesSchemaLifecycleWithCaller() throws Exception {
        var config = config("startup-" + UUID.randomUUID());
        try (var store = GitSchemaRegistryStore.builder().repositoryDir(directory).build();
             var database = new LedgerDatabase(config.ledger())) {
            var tx = new Tx(database.entityManagerFactory());
            var access = new Access(store, definition(StringValue.getDescriptor()));
            tx.inTransaction(em -> {
                em.createNativeQuery("""
                        CREATE FUNCTION test_schema_host_startup_failure() RETURNS trigger LANGUAGE plpgsql AS $$
                        BEGIN RAISE EXCEPTION 'deliberate schema host reader registration failure'; END $$
                        """).executeUpdate();
                em.createNativeQuery("""
                        CREATE TRIGGER test_schema_host_startup_failure BEFORE INSERT ON repository_reader_incarnations
                        FOR EACH ROW EXECUTE FUNCTION test_schema_host_startup_failure()
                        """).executeUpdate();
            });
            try {
                assertThatThrownBy(() -> RepoServices.build(config, BridgeEngine.standard(), null, access))
                        .hasStackTraceContaining("deliberate schema host reader registration failure");
                assertThat(access.closes.get()).isZero();
                try (var scope = access.resolver.open(occurrence -> { throw new AssertionError("No lookup required"); }, () -> {})) {
                    assertThat(scope).isNotNull();
                }
            } finally {
                tx.inTransaction(em -> {
                    em.createNativeQuery("DROP TRIGGER test_schema_host_startup_failure ON repository_reader_incarnations").executeUpdate();
                    em.createNativeQuery("DROP FUNCTION test_schema_host_startup_failure()").executeUpdate();
                });
                access.close();
                assertThat(access.awaitIdle(Duration.ofSeconds(5))).isTrue();
            }
        }
    }

    @Test void journaledObservationFailureLeavesSchemaLifecycleWithCallerAndDrainsReaders() throws Exception {
        var config = config("journaled-startup-" + UUID.randomUUID());
        try (var store = GitSchemaRegistryStore.builder().repositoryDir(directory).build();
             var database = new LedgerDatabase(config.ledger())) {
            var tx = new Tx(database.entityManagerFactory());
            var access = new Access(store, definition(StringValue.getDescriptor()));
            long before = tx.readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM repository_reader_incarnations WHERE state='ACTIVE'").getSingleResult()).longValue());
            var journaled = new ManagedDocumentServices.Journaled(new DocumentPublicationRuntime.Assessments(
                    directory.resolve("absent-runtime-bundle"), Duration.ofMinutes(5), Duration.ofSeconds(5)),
                    (account, principal, operation) -> ADMIN);
            try {
                assertThatThrownBy(() -> new RepoServices(config, BridgeEngine.standard(), BlobStores.discover(),
                        null, access, null, journaled)).isInstanceOf(java.io.UncheckedIOException.class)
                        .hasMessageContaining("Cannot observe managed publication runtime");
                assertThat(access.closes.get()).isZero();
                long after = tx.readOnly(em -> ((Number) em.createNativeQuery(
                        "SELECT count(*) FROM repository_reader_incarnations WHERE state='ACTIVE'").getSingleResult()).longValue());
                assertThat(after).isEqualTo(before);
                try (var scope = access.resolver.open(occurrence -> { throw new AssertionError("No lookup required"); }, () -> {})) {
                    assertThat(scope).isNotNull();
                }
            } finally { access.close(); assertThat(access.awaitIdle(Duration.ofSeconds(5))).isTrue(); }
        }
    }

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
        try (var backing = BlobStores.discover().open("s3", Map.ofEntries(
                Map.entry("endpoint", STORAGE.getEndpoint().toString()),
                Map.entry("region", STORAGE.getRegion()),
                Map.entry("path-style", "true"),
                Map.entry("conditional-writes", "true"),
                Map.entry("access-key", STORAGE.getAccessKey()),
                Map.entry("secret-key", STORAGE.getSecretKey()),
                Map.entry("credentials-mode", "static"),
                Map.entry("api-call-timeout-ms", "300000"),
                Map.entry("api-attempt-timeout-ms", "60000"),
                Map.entry("connection-timeout-ms", "10000"),
                Map.entry("socket-timeout-ms", "60000")))) { backing.ensureNamespace(namespace); }
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
        var document = Document.newBuilder().setDocId("document").setOwnership(ownership)
                .setStructuredData(Any.pack(StringValue.of("managed registry payload"), "type.test")).build();
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
