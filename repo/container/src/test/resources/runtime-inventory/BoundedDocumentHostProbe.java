package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.repo.admission.DocumentSchemaArtifactCache;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.blob.redis.RedisBlobStoreProvider;
import ai.protomolt.proto.repo.container.ledger.*;
import ai.protomolt.proto.repo.schema.registry.RegistrySchemaResolver;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.repo.codec.*;
import ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import com.google.protobuf.*;
import ai.protomolt.proto.schema.registry.git.GitSchemaRegistryStore;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Actual PostgreSQL, Redis and Git resources; bounded typed publication and historical reads. */
public final class BoundedDocumentHostProbe {
    public static void run(Path bundle) throws Exception {
        String generation="bounded-document-"+UUID.randomUUID();
        var config=new RepoServiceConfig(0,new LedgerConfig(System.getenv("PROTOMOLT_TEST_JDBC"),
                System.getenv("PROTOMOLT_TEST_USER"),System.getenv("PROTOMOLT_TEST_PASSWORD")),
                "http://127.0.0.1:1","us-east-1","unused","unused","bounded-document",0,
                "redis",null,null,System.getenv("PROTOMOLT_TEST_REDIS_URI"),0,1024*1024)
                .withManagedStorage(new ManagedStoragePolicy(generation,"bounded-document-realm",true));
        var opens=new AtomicInteger(); var closes=new AtomicInteger();
        var real=new RedisBlobStoreProvider();
        BlobStoreProvider selected=new BlobStoreProvider() {
            public String id() { return "redis"; }
            public BackendIdentity managedIdentity(Map<String,String> options) {
                require("create-only".equals(options.get("write-policy")),"create-only managed identity");
                return real.managedIdentity(options);
            }
            public OpenedBlobStore open(Map<String,String> options) {
                require("create-only".equals(options.get("write-policy")),"create-only provider open");
                var actual=real.open(options); opens.incrementAndGet();
                try {
                require(actual.capabilities().containsAll(Set.of(BlobCapability.BOUNDED_READ,
                        BlobCapability.NON_EXPIRING_WRITES,BlobCapability.PHYSICAL_RECLAMATION)),"bounded retention capabilities");
                require(!actual.capabilities().contains(BlobCapability.STREAMING_WRITE),"no invented streaming capability");
                } catch (RuntimeException | Error failure) {
                    try { actual.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
                    throw failure;
                }
                return new OpenedBlobStore(actual.store(),() -> { actual.close(); closes.incrementAndGet(); },
                        actual.capabilities(),actual::ensureNamespace,actual.reclaimer());
            }
        };
        BlobStoreProvider forbidden=new BlobStoreProvider() {
            public String id() { return "s3"; }
            public OpenedBlobStore open(Map<String,String> options) { throw new AssertionError("S3 opened by Redis document host"); }
            public BackendIdentity managedIdentity(Map<String,String> options) { throw new AssertionError("S3 identity selected"); }
        };
        Path directory=Files.createTempDirectory("bounded-document-git");
        var definition=definition(StringValue.getDescriptor());
        try (var git=GitSchemaRegistryStore.builder().repositoryDir(directory).build();
             var database=new LedgerDatabase(config.ledger())) {
            git.putDescriptorSet(definition.metadata().getArtifactSha256(),definition.descriptors());
            var resolver=new RegistrySchemaResolver(git,new DocumentSchemaArtifactCache.Limits(8_000_000,16,4_000_000),4,16);
            ManagedSchemaAccess schemas=new ManagedSchemaAccess() {
                public DocumentSchemaAdmission.Resolution open(RepositoryCaller caller,DocumentPublicationMember member,RepositoryReadControl control) {
                    return resolver.open(occurrence -> new RegistrySchemaResolver.Selected(definition.metadata(),definition.source()),control::check);
                }
                public void close() { resolver.close(); }
                public boolean awaitIdle(Duration timeout) throws InterruptedException { return resolver.awaitLoads(timeout); }
            };
            var caller=new RepositoryCaller("bounded-host",true);
            var options=new ManagedPublicationOptions(bundle,Duration.ofMinutes(5),Duration.ofSeconds(5),
                    (account,principal,operation) -> caller).withTransport(
                    new ManagedPublicationOptions.Transport(auth -> caller,32L*1024*1024,2));
            try (var host=new RepoServices(config,BridgeEngine.standard(),BlobStores.of(List.of(selected,forbidden)),
                    null,schemas,null,options.journaled(),new BoundedDocumentProfile(1024*1024,64L*1024*1024))) {
                require(opens.get()==1,"one Redis provider");
                require(host.publicationRepository()!=null && host.historicalRepository()!=null,"document repositories mounted");
                require(host.services().size()==1 && host.services().getFirst() instanceof DocumentPublicationGrpcService,
                        "publication-only service list");
                unavailable(host::repository); unavailable(host::archiveRepository); unavailable(host::archiveMutationRepository);
                unavailable(host::driveRepository); unavailable(host::documentPublication);
                unavailable(host::seedAccountDrives);
                unavailable(() -> host.startHttp(0,"fixture-token"));
                try { host.startBoundedArchiveNetty(0,"fixture-token"); throw new AssertionError("archive listener exposed"); }
                catch (IllegalStateException expected) { }
                var fixture=prepare(host,new Tx(database.entityManagerFactory()));
                var result=host.publicationRepository().publishDocument(caller,fixture.request(),RepositoryReadControl.NONE);
                require(result.hasCommitted() && result.getCommitted().getMembersCount()==1,"published member");
                var replay=host.publicationRepository().publishDocument(caller,fixture.request(),RepositoryReadControl.NONE);
                require(result.equals(replay),"exact receipt replay");
                // Historical validation must use retained artifacts after live resolution stops.
                resolver.close();
                require(resolver.awaitLoads(Duration.ofSeconds(5)),"schema resolver drained");
                var revision=result.getCommitted().getMembers(0);
                try (var read=host.historicalRepository().readValidated(caller,revision.getAddress(),
                        UUID.fromString(revision.getRevisionId()),RepositoryReadControl.NONE)) {
                    require(read.document().equals(fixture.document()),"Redis historical document");
                    require(read.publicationRevision()==revision.getMutationRevision(),"historical publication identity");
                    read.authorizeDelivery(RepositoryReadControl.NONE);
                }
                require(closes.get()==0,"Redis remains open during host lifetime");
            } finally { schemas.close(); }
            require(closes.get()==1,"Redis closes once after host drain");
        } finally {
            try (var paths=Files.walk(directory)) {
                for (var path:paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path);
            }
        }
        System.out.println("BOUNDED_DOCUMENT_HOST_STARTUP_OK");
        System.out.println("BOUNDED_DOCUMENT_PUBLICATION_HISTORY_OK");
    }
    private record Fixture(PublishDocumentRequest request, Document document) {}
    private static Fixture prepare(RepoServices host, Tx tx) throws Exception {
        String account = "account-" + UUID.randomUUID();
        String namespace = "schema-" + UUID.randomUUID();
        var drive = new DriveRecord();
        drive.driveId = UUID.randomUUID(); drive.accountId = account; drive.name = "schema"; drive.driveType = "PIPELINE"; drive.bucket = namespace; drive.provider = "redis";
        host.driveLedger().insert(drive);
        var policy = DocumentAdmissionPolicy.of(DocumentSchemaPolicy.newBuilder()
                .setEncodingVersion(1).setAccountId(account).setMode(DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_TYPED_REQUIRED).setAnyResolvedSchema(true)
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
        documentBuilder.setStructuredData(Any.pack(StringValue.of("managed registry payload"), "type.test"));
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
        var request=PublishDocumentRequest.newBuilder().setIntent(command.intent())
                .addModes(DocumentPublicationMemberMode.newBuilder().setMemberId("document")
                        .setMode(DocumentPublicationMode.DOCUMENT_PUBLICATION_MODE_TYPED));
        bodies.forEach((key,body) -> request.addPayloads(DocumentPublicationPayload.newBuilder()
                .setMemberId(key.member()).setRevisionOrdinal(key.revisionOrdinal())
                .setContent(ByteString.copyFrom(body.bytes()))));
        return new Fixture(request.build(), document);
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
    private static void unavailable(Runnable call) {
        try { call.run(); throw new AssertionError("unbounded API exposed"); }
        catch (UnsupportedOperationException expected) { }
    }
    private static void require(boolean value,String message) { if (!value) throw new AssertionError(message); }
}
