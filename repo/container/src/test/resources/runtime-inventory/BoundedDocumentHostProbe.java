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
        var faults=new BoundedDocumentWriteFault();
        var readGate=new BoundedDocumentReadGate();
        var delayed=new BoundedDocumentDelayedWrite();
        var rejectionProbe=new BoundedDocumentRejectionProbe();
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
                return new OpenedBlobStore(rejectionProbe.wrap(delayed.wrap(readGate.wrap(faults.wrap(actual.store())),options.get("key-prefix"))),() -> { actual.close(); closes.incrementAndGet(); },
                        actual.capabilities(),actual::ensureNamespace,actual.reclaimer());
            }
        };
        BlobStoreProvider forbidden=new BlobStoreProvider() {
            public String id() { return "s3"; }
            public OpenedBlobStore open(Map<String,String> options) { throw new AssertionError("S3 opened by Redis document host"); }
            public BackendIdentity managedIdentity(Map<String,String> options) { throw new AssertionError("S3 identity selected"); }
        };
        Path directory=Files.createTempDirectory("bounded-document-git");
        var definition=definition(BoundedDocumentRejectionProbe.constrainedType());
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
            var caller=new RepositoryCaller("operator",true);
            var options=new ManagedPublicationOptions(bundle,Duration.ofMinutes(5),Duration.ofSeconds(5),
                    (account,principal,operation) -> caller).withTransport(
                    new ManagedPublicationOptions.Transport(auth -> caller,32L*1024*1024,2));
            Fixture fixture;
            PublishDocumentResponse result;
            Fixture updated;
            PublishDocumentResponse updateResult;
            try (var host=new RepoServices(config,BridgeEngine.standard(),BlobStores.of(List.of(selected,forbidden)),
                    new HistoricalReadAccess(auth -> caller,32L*1024*1024,2),schemas,null,options.journaled(),new BoundedDocumentProfile(1024*1024,64L*1024*1024))) {
                require(opens.get()==1,"one Redis provider");
                require(host.publicationRepository()!=null && host.historicalRepository()!=null,"document repositories mounted");
                require(host.services().size()==2
                        && host.services().stream().anyMatch(service -> service instanceof DocumentPublicationGrpcService)
                        && host.services().stream().anyMatch(service -> service instanceof DocumentHistoryGrpcService),
                        "publication and history service list");
                unavailable(host::repository); unavailable(host::archiveRepository); unavailable(host::archiveMutationRepository);
                unavailable(host::driveRepository); unavailable(host::documentPublication);
                unavailable(host::seedAccountDrives);
                unavailable(() -> host.startHttp(0,"fixture-token"));
                try { host.startBoundedArchiveNetty(0,"fixture-token"); throw new AssertionError("archive listener exposed"); }
                catch (IllegalStateException expected) { }
                fixture=prepare(host,new Tx(database.entityManagerFactory()));
                result=host.publicationRepository().publishDocument(caller,fixture.request(),RepositoryReadControl.NONE);
                require(result.hasCommitted() && result.getCommitted().getMembersCount()==1,"published member");
                var replay=host.publicationRepository().publishDocument(caller,fixture.request(),RepositoryReadControl.NONE);
                require(result.equals(replay),"exact receipt replay");
                verifyTransport(host,new Tx(database.entityManagerFactory()),caller,fixture.request(),result);
                var invalid=withValue(prepare(host,new Tx(database.entityManagerFactory())),"contract-invalid");
                var rejected=rejectionProbe.reject(host,new Tx(database.entityManagerFactory()),caller,invalid.request());
                updated=replacement(fixture,result.getCommitted().getMembers(0));
                faults.arm();
                try {
                    host.publicationRepository().publishDocument(caller,updated.request(),RepositoryReadControl.NONE);
                    throw new AssertionError("Lost write acknowledgement produced a success response");
                } catch (RuntimeException failure) {
                    require(faults.caused(failure),"original write failure is preserved: "+failure);
                }
                faults.verifyStored();
                require(host.documentLedger().findByReference(result.getCommitted().getMembers(0).getAddress())
                        .orElseThrow().mutationRevision==result.getCommitted().getMembers(0).getMutationRevision(),
                        "unacknowledged write did not advance document");
                try {
                    host.publicationRepository().publishDocument(caller,updated.request(),RepositoryReadControl.NONE);
                    throw new AssertionError("Unverified retry was adopted");
                } catch (DocumentPartAttemptLedger.FenceException expected) {
                    require(expected.getMessage().equals("Initial upload is not an exact live verified retry"),"unverified retry fence");
                }
                require(host.documentLedger().findByReference(result.getCommitted().getMembers(0).getAddress())
                        .orElseThrow().mutationRevision==result.getCommitted().getMembers(0).getMutationRevision(),
                        "unverified retry left current revision intact");
                // A distinct operation qualifies later publication; it is not recovery of the failed operation.
                updated=new Fixture(updated.request().toBuilder().setIntent(updated.request().getIntent().toBuilder()
                        .setOperationId(UUID.randomUUID().toString())).build(),updated.document());
                updateResult=host.publicationRepository().publishDocument(caller,updated.request(),RepositoryReadControl.NONE);
                require(updateResult.hasCommitted(),"replacement committed");
                var next=updateResult.getCommitted().getMembers(0);
                var previous=result.getCommitted().getMembers(0);
                require(next.getAddress().equals(previous.getAddress()) && !next.getRevisionId().equals(previous.getRevisionId())
                        && next.getMutationRevision()>previous.getMutationRevision(),"new immutable revision at same address");
                require(!updated.document().equals(fixture.document()),"replacement changes document bytes");
                require(host.publicationRepository().publishDocument(caller,fixture.request(),RepositoryReadControl.NONE)
                        .equals(result),"old receipt survives replacement");
                var delayedRequest=replacement(updated,next,"late uncommitted payload");
                delayed.arm();
                try {
                    host.publicationRepository().publishDocument(caller,delayedRequest.request(),RepositoryReadControl.NONE);
                    throw new AssertionError("Undelivered Redis request produced publication success");
                } catch (RuntimeException failure) {
                    require(BoundedDocumentDelayedWrite.timedOut(failure),"actual Redis caller timed out: "+failure);
                }
                delayed.assertAbsent(host.blobStore());
                delayed.assertDistinct(fixture.request(),updated.request());
                require(host.documentLedger().findByReference(next.getAddress()).orElseThrow().mutationRevision==next.getMutationRevision(),
                        "delayed request did not advance current document");
                System.out.println("BOUNDED_DOCUMENT_DELAYED_REQUEST_OK");
                // Historical validation must use retained artifacts after live resolution stops.
                resolver.close();
                require(resolver.awaitLoads(Duration.ofSeconds(5)),"schema resolver drained");
                rejectionProbe.replay(host,new Tx(database.entityManagerFactory()),caller,invalid.request(),rejected);
                var revision=result.getCommitted().getMembers(0);
                try (var read=host.historicalRepository().readValidated(caller,revision.getAddress(),
                        UUID.fromString(revision.getRevisionId()),RepositoryReadControl.NONE)) {
                    require(read.document().equals(fixture.document()),"Redis historical document");
                    require(read.publicationRevision()==revision.getMutationRevision(),"historical publication identity");
                    read.authorizeDelivery(RepositoryReadControl.NONE);
                }
                verifyHistoryTransport(host,caller,fixture.document(),revision);
                verifyHistoryTransport(host,caller,updated.document(),updateResult.getCommitted().getMembers(0));
                require(closes.get()==0,"Redis remains open during host lifetime");
                readGate.verifyShutdown(host,caller,revision,closes);
            } finally { schemas.close(); }
            require(closes.get()==1,"Redis closes once after host drain");
            Path restart=Files.createDirectory(Path.of(System.getenv("PROTOMOLT_TEST_BOUNDED_RESTART_DIR")));
            Files.writeString(restart.resolve("generation"),generation);
            faults.save(restart);
            delayed.save(restart);
            Files.write(restart.resolve("request.pb"),fixture.request().toByteArray());
            Files.write(restart.resolve("receipt.pb"),result.toByteArray());
            Files.write(restart.resolve("document.pb"),fixture.document().toByteArray());
            Files.write(restart.resolve("request-next.pb"),updated.request().toByteArray());
            Files.write(restart.resolve("receipt-next.pb"),updateResult.toByteArray());
            Files.write(restart.resolve("document-next.pb"),updated.document().toByteArray());
        } finally {
            try (var paths=Files.walk(directory)) {
                for (var path:paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path);
            }
        }
        System.out.println("BOUNDED_DOCUMENT_HOST_STARTUP_OK");
        System.out.println("BOUNDED_DOCUMENT_PUBLICATION_HISTORY_OK");
    }
    static void verifyHistoryTransport(RepoServices host, RepositoryCaller caller, Document expected,
            DocumentPublishedRevision revision) throws Exception {
        String name="bounded-history-"+UUID.randomUUID();
        host.startInProcess(name,"history-fixture-token",null);
        var channel=io.grpc.inprocess.InProcessChannelBuilder.forName(name).build();
        try {
            var request=ReadRevisionRequest.newBuilder().setAddress(revision.getAddress()).setRevisionId(revision.getRevisionId())
                    .setMode(HistoricalDocumentReadMode.HISTORICAL_DOCUMENT_READ_MODE_VALIDATED).build();
            var base=DocumentHistoryServiceGrpc.newBlockingStub(channel);
            expectStatus(io.grpc.Status.Code.UNAUTHENTICATED,() -> base.withDeadlineAfter(10,java.util.concurrent.TimeUnit.SECONDS).readRevision(request));
            var headers=new io.grpc.Metadata();
            headers.put(io.grpc.Metadata.Key.of("api_token",io.grpc.Metadata.ASCII_STRING_MARSHALLER),"history-fixture-token");
            var client=base.withInterceptors(io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(headers));
            var response=client.withDeadlineAfter(10,java.util.concurrent.TimeUnit.SECONDS).readRevision(request);
            require(response.hasValidated() && response.getValidated().getDocument().equals(expected),"remote retained-schema document");
            require(response.getRevisionId().equals(revision.getRevisionId()) && response.getAddress().equals(revision.getAddress())
                    && response.getMutationRevision()==revision.getMutationRevision(),"remote history identity");
            try (var local=host.historicalRepository().readValidated(caller,revision.getAddress(),
                    UUID.fromString(revision.getRevisionId()),RepositoryReadControl.NONE)) {
                require(response.getValidated().getCommandSha256().equals(local.commandSha256()),"remote command binding");
                require(response.getValidated().getPolicySha256().equals(local.policySha256()),"remote policy binding");
                require(response.getMetadata().equals(local.metadata()) && response.getManifest().equals(local.manifest()),"remote historical metadata");
            }
            var raw=client.withDeadlineAfter(10,java.util.concurrent.TimeUnit.SECONDS).readRevision(request.toBuilder()
                    .setMode(HistoricalDocumentReadMode.HISTORICAL_DOCUMENT_READ_MODE_RAW).build());
            require(raw.hasRaw() && !raw.hasValidated(),"raw delivery is explicit");
            try (var local=host.historicalRepository().readRaw(caller,revision.getAddress(),
                    UUID.fromString(revision.getRevisionId()),RepositoryReadControl.NONE)) {
                require(raw.getRaw().getFragmentsCount()==local.fragments().size(),"raw fragment count");
                for (int i=0;i<local.fragments().size();i++) {
                    var fragment=local.fragments().get(i);
                    require(raw.getRaw().getFragments(i).getRevisionOrdinal()==fragment.revisionOrdinal()
                            && raw.getRaw().getFragments(i).getContent().equals(ByteString.copyFrom(fragment.bytes())),"raw fragment bytes");
                }
            }
            expectStatus(io.grpc.Status.Code.INVALID_ARGUMENT,() -> client.withDeadlineAfter(10,java.util.concurrent.TimeUnit.SECONDS)
                    .readRevision(request.toBuilder().clearMode().build()));
        } finally {
            channel.shutdownNow();
            require(channel.awaitTermination(10,java.util.concurrent.TimeUnit.SECONDS),"history channel stopped");
        }
        System.out.println("BOUNDED_DOCUMENT_HISTORY_TRANSPORT_OK");
    }
    /** Real host authentication with a fixture operator credential, not production API-key provisioning. */
    private static void verifyTransport(RepoServices host, Tx tx, RepositoryCaller caller,
            PublishDocumentRequest original, PublishDocumentResponse receipt) throws Exception {
        String name="bounded-document-"+UUID.randomUUID();
        host.startInProcess(name,"bounded-fixture-token",null);
        var channel=io.grpc.inprocess.InProcessChannelBuilder.forName(name).build();
        try {
            var unauthenticated=DocumentPublicationServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(10,java.util.concurrent.TimeUnit.SECONDS);
            expectStatus(io.grpc.Status.Code.UNAUTHENTICATED,() -> unauthenticated.publishDocument(original));
            var headers=new io.grpc.Metadata();
            headers.put(io.grpc.Metadata.Key.of("api_token",io.grpc.Metadata.ASCII_STRING_MARSHALLER),"bounded-fixture-token");
            var client=unauthenticated.withInterceptors(io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(headers));
            require(client.withDeadlineAfter(10,java.util.concurrent.TimeUnit.SECONDS).publishDocument(original).equals(receipt),"remote replay matches local receipt");
            var fresh=prepare(host,tx);
            var published=client.publishDocument(fresh.request());
            require(published.hasCommitted(),"remote typed publication committed");
            require(host.publicationRepository().publishDocument(caller,fresh.request(),RepositoryReadControl.NONE)
                    .equals(published),"local replay matches remote receipt");
            byte[] tooLarge=new byte[1024*1024+1];
            var payload=fresh.request().getPayloads(0);
            int memberIndex=0;
            require(fresh.request().getIntent().getMembers(memberIndex).getMemberId().equals(payload.getMemberId()),
                    "oversized fixture member coordinate");
            var member=fresh.request().getIntent().getMembers(memberIndex).toBuilder();
            var part=member.getParts(payload.getRevisionOrdinal()).toBuilder();
            part.setUpload(part.getUpload().toBuilder().setSizeBytes(tooLarge.length)
                    .setSha256(DocumentPartCodec.sha256Hex(tooLarge)));
            member.setParts(payload.getRevisionOrdinal(),part);
            var oversized=fresh.request().toBuilder().setIntent(fresh.request().getIntent().toBuilder()
                    .setOperationId(UUID.randomUUID().toString()).setMembers(memberIndex,member))
                    .setPayloads(0,payload.toBuilder().setContent(ByteString.copyFrom(tooLarge))).build();
            DocumentPublicationInput.validate(oversized,RepositoryReadControl.NONE);
            expectStatus(io.grpc.Status.Code.INVALID_ARGUMENT,() -> client.publishDocument(oversized));
            try {
                host.publicationRepository().publishDocument(caller,oversized,RepositoryReadControl.NONE);
                throw new AssertionError("local oversized request accepted");
            } catch (IllegalArgumentException expected) {
                require(expected.getMessage().equals("Publication upload exceeds configured object limit"),"local object cap classification");
            }
        } finally {
            channel.shutdownNow();
            require(channel.awaitTermination(10,java.util.concurrent.TimeUnit.SECONDS),"bounded channel stopped");
        }
        System.out.println("BOUNDED_DOCUMENT_TRANSPORT_OK");
    }
    private static void expectStatus(io.grpc.Status.Code code,Runnable call) {
        try { call.run(); throw new AssertionError("Expected "+code); }
        catch (io.grpc.StatusRuntimeException failure) { require(failure.getStatus().getCode()==code,"transport status "+failure); }
    }
    private static Fixture replacement(Fixture original,DocumentPublishedRevision previous) {
        return replacement(original,previous,"updated registry payload");
    }
    private static Fixture replacement(Fixture original,DocumentPublishedRevision previous,String value) {
        var changed=withValue(original,value);
        var member=changed.request().getIntent().getMembers(0).toBuilder();
        member.setDestination(member.getDestination().toBuilder().setExpectedMutationRevision(previous.getMutationRevision()));
        return new Fixture(changed.request().toBuilder().setIntent(changed.request().getIntent().toBuilder().setMembers(0,member)).build(),changed.document());
    }
    private static Fixture withValue(Fixture original,String value) {
        var document=original.document().toBuilder()
                .setStructuredData(Any.pack(StringValue.of(value),"type.test")).build();
        var member=original.request().getIntent().getMembers(0).toBuilder().clearParts();
        var request=original.request().toBuilder().clearPayloads();
        for (var part:DocumentPartCodec.split(document,PartLayouts.document())) {
            int ordinal=member.getPartsCount();
            member.addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder()
                    .setPart(part.part()).setSubKey(part.subKey())).setUpload(PublicationUpload.newBuilder()
                    .setSizeBytes(part.bytes().length).setSha256(DocumentPartCodec.sha256Hex(part.bytes()))
                    .setContentType("application/protobuf")));
            request.addPayloads(DocumentPublicationPayload.newBuilder().setMemberId(member.getMemberId())
                    .setRevisionOrdinal(ordinal).setContent(ByteString.copyFrom(part.bytes())));
        }
        request.setIntent(original.request().getIntent().toBuilder().setOperationId(UUID.randomUUID().toString()).setMembers(0,member));
        return new Fixture(request.build(),document);
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
