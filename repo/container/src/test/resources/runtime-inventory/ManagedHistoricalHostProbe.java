package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.repo.admission.*;
import ai.protomolt.proto.repo.container.ledger.*;
import ai.protomolt.proto.repo.schema.registry.RegistrySchemaResolver;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.schema.registry.git.GitSchemaRegistryStore;
import com.google.protobuf.*;
import io.grpc.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Actual hosted composition over PostgreSQL, retention-qualified Redis and a Git schema registry. */
public final class ManagedHistoricalHostProbe {
    public static void main(String[] args) throws Exception {
        for (boolean enabled : new boolean[] {false, true}) for (boolean remoteFirst : new boolean[] {false, true})
            run(Path.of(args[0]), enabled, remoteFirst);
    }

    private static void run(Path bundle, boolean enabled, boolean remoteFirst) throws Exception {
        var config = new RepoServiceConfig(0, new LedgerConfig(System.getenv("PROTOMOLT_TEST_JDBC"),
                System.getenv("PROTOMOLT_TEST_USER"), System.getenv("PROTOMOLT_TEST_PASSWORD")),
                "http://127.0.0.1:1", "us-east-1", "unused", "unused", "historical-host", 0,
                "redis", null, null, System.getenv("PROTOMOLT_TEST_REDIS_URI"), 0, 1024 * 1024)
                .withManagedStorage(new ManagedStoragePolicy("historical-host-" + UUID.randomUUID(), "historical-host-realm", true));
        var caller = new RepositoryCaller("operator", true);
        var identity = new ReaderHostOptions(UUID.randomUUID(), "historical-host-test", UUID.randomUUID().toString());
        var definition = BoundedDocumentHostProbe.definition(StringValue.getDescriptor());
        Path directory = Files.createTempDirectory("historical-host-git");
        try (var git = GitSchemaRegistryStore.builder().repositoryDir(directory).build();
             var database = new LedgerDatabase(config.ledger())) {
            git.putDescriptorSet(definition.metadata().getArtifactSha256(), definition.descriptors());
            var resolver = new RegistrySchemaResolver(git, new DocumentSchemaArtifactCache.Limits(8_000_000, 16, 4_000_000), 4, 16);
            var resolutions = new AtomicInteger();
            var closes = new AtomicInteger();
            ManagedSchemaAccess schemas = new ManagedSchemaAccess() {
                public DocumentSchemaAdmission.Resolution open(RepositoryCaller actual, DocumentPublicationMember member, RepositoryReadControl control) {
                    require(actual.equals(caller), "host carries actual caller into schema resolution");
                    return resolver.open(occurrence -> {
                        resolutions.incrementAndGet();
                        return new RegistrySchemaResolver.Selected(definition.metadata(), definition.source());
                    }, control::check);
                }
                public void close() { closes.incrementAndGet(); resolver.close(); }
                public boolean awaitIdle(Duration timeout) throws InterruptedException { return resolver.awaitLoads(timeout); }
            };
            var recoveryKey = new java.util.concurrent.atomic.AtomicReference<DocumentPublicationCommand>();
            var recoveryCalls = new AtomicInteger();
            var options = new ManagedPublicationOptions(bundle, Duration.ofMinutes(5), Duration.ofSeconds(5),
                    (account, principal, operation) -> caller).withRecovery((account, principal, operation) -> {
                        var expected = Objects.requireNonNull(recoveryKey.get(), "Only historical execution requests recovery authority");
                        require(account.equals(expected.intent().getAccountId()) && principal.equals(caller.principalName())
                                && operation.equals(expected.operationId()), "exact operation recovery authority");
                        recoveryCalls.incrementAndGet();
                        return caller;
                    }).withTransport(new ManagedPublicationOptions.Transport(auth -> {
                        require(auth.caller().unrestricted(), "test uses the authenticated host operator token");
                        return caller;
                    }, 32L * 1024 * 1024, 2));
            if (enabled) options = options.withHistoricalPublication(2);
            try {
                try (var host = RepoServices.buildBoundedDocumentsHosted(config, BridgeEngine.standard(), null, schemas,
                        options, new BoundedDocumentOptions(1024 * 1024, 64L * 1024 * 1024), identity)) {
                    var tx = new Tx(database.entityManagerFactory());
                    var fixture = BoundedDocumentHostProbe.prepare(host, tx);
                    var initial = host.publicationRepository().publishDocument(caller, fixture.request(), RepositoryReadControl.NONE);
                    require(initial.hasCommitted(), "ordinary publication still works with either option");
                    var request = historical(tx, caller, fixture.request(), initial.getCommitted().getMembers(0));
                    recoveryKey.set(new DocumentPublicationCommand(request.getIntent()));
                    int before = resolutions.get();
                    String name = "managed-historical-" + UUID.randomUUID();
                    host.startInProcess(name, "historical-host-test-token", null);
                    var channel = io.grpc.inprocess.InProcessChannelBuilder.forName(name).build();
                    try {
                        var headers = new Metadata();
                        headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), "historical-host-test-token");
                        var stub = DocumentPublicationServiceGrpc.newBlockingStub(channel)
                                .withInterceptors(io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(headers)).withDeadlineAfter(30, TimeUnit.SECONDS);
                        if (!enabled) {
                            try { host.publicationRepository().publishDocument(caller, request, RepositoryReadControl.NONE);
                                throw new AssertionError("Historical publication activated without opt-in");
                            } catch (UnsupportedOperationException expected) { require(expected.getMessage().contains("Historical reuse"), "disabled library status"); }
                            try { stub.publishDocument(request); throw new AssertionError("Historical RPC activated without opt-in"); }
                            catch (StatusRuntimeException expected) { require(expected.getStatus().getCode() == Status.Code.UNIMPLEMENTED, "disabled RPC status"); }
                            require(resolutions.get() == before && recoveryCalls.get() == 0, "disabled history performs no schema or recovery work");
                        } else {
                            var result = remoteFirst ? stub.publishDocument(request)
                                    : host.publicationRepository().publishDocument(caller, request, RepositoryReadControl.NONE);
                            require(result.hasCommitted(), "mixed historical publication commits through host");
                            require(resolutions.get() == before + 1, "only new parsed upload resolves a live schema");
                            require(recoveryCalls.get() > 0, "explicit historical authority used");
                            require(stub.publishDocument(request).equals(result)
                                    && host.publicationRepository().publishDocument(caller, request, RepositoryReadControl.NONE).equals(result),
                                    "library and RPC exact retries return identical receipt");
                            require(resolutions.get() == before + 1, "replay does not resolve schemas");
                            var expected = fixture.document().toBuilder().putParserResults("fresh", parsed()).build();
                            verify(host, caller, result.getCommitted().getMembers(0), expected);
                            verifyIdentity(tx, caller, request, result.getCommitted().getMembers(0));
                            verify(host, caller, initial.getCommitted().getMembers(0), fixture.document());
                            long commits = tx.readOnly(em -> ((Number) em.createNativeQuery(
                                    "SELECT count(*) FROM document_revision_commits WHERE operation_id=:id")
                                    .setParameter("id", recoveryKey.get().operationId()).getSingleResult()).longValue());
                            require(commits == 1, "one historical revision commit across both public paths");
                        }
                    } finally { channel.shutdownNow(); require(channel.awaitTermination(5, TimeUnit.SECONDS), "host channel drained"); }
                }
                require(closes.get() == 1, "successful host closes schema admission exactly once");
                var states = new Tx(database.entityManagerFactory()).readOnly(em -> em.createNativeQuery(
                        "SELECT state FROM repository_reader_incarnations WHERE host_execution=:id", String.class)
                        .setParameter("id", identity.execution()).getResultList());
                require(states.equals(List.of("QUIESCED")), "owned reader quiesced after historical host shutdown");
            } finally { schemas.close(); require(schemas.awaitIdle(Duration.ofSeconds(5)), "registry drained"); }
        } finally {
            try (var paths = Files.walk(directory)) { for (var path : paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path); }
        }
        System.out.println("MANAGED_HISTORICAL_" + (enabled ? "ENABLED" : "DISABLED") + "_" + (remoteFirst ? "RPC" : "LIBRARY") + "_OK");
    }

    private static ParserResult parsed() {
        return ParserResult.newBuilder().setDocument(ParserDocument.newBuilder()
                .setShape(Any.pack(StringValue.of("fresh host parser payload"), "type.test"))).build();
    }

    private static PublishDocumentRequest historical(Tx tx, RepositoryCaller caller, PublishDocumentRequest source, DocumentPublishedRevision revision) {
        var member = source.getIntent().getMembers(0).toBuilder().clearParts().setDestination(DocumentRevisionCondition.newBuilder()
                .setAddress(revision.getAddress()).setExpectedMutationRevision(revision.getMutationRevision()));
        var ledger = new DocumentReadLedger(tx, UUID.randomUUID());
        var capture = ledger.captureHistorical(caller, revision.getAddress(), UUID.fromString(revision.getRevisionId()));
        try (var use = capture.use()) {
            for (var entry : use.plan().entries()) {
                var part = entry.part().part(); var binding = entry.part().binding();
                var slot = DocumentPublicationSlot.newBuilder().setPart(part.part()).setSubKey(part.subKey()).build();
                var object = PublicationObjectIdentity.newBuilder().setObjectId(entry.objectId().toString()).setBackendGeneration(binding.generation())
                        .setStorageRealm(binding.profile().storageRealm()).setNamespace(binding.namespace()).setObjectKey(part.key())
                        .setSizeBytes(part.size()).setSha256(part.sha256()).setContentType(part.contentType());
                // Create-only Redis has no provider version; do not invent one for its retained object.
                if (part.providerVersion() != null) object.setProviderVersion(part.providerVersion());
                member.addParts(DocumentPublicationPart.newBuilder().setSlot(slot).setHistoricalReuse(PublicationHistoricalReuse.newBuilder()
                        .setSource(revision.getAddress()).setRevisionId(revision.getRevisionId()).setRevisionOrdinal(entry.revisionOrdinal()).setSourceSlot(slot)
                        .setObject(object)));
            }
        } finally { capture.close(); ledger.releaseDrained(16); }
        var bytes = Document.newBuilder().setDocId(revision.getAddress().getDocId()).putParserResults("fresh", parsed()).build().toByteString();
        int ordinal = member.getPartsCount();
        member.addParts(DocumentPublicationPart.newBuilder().setSlot(DocumentPublicationSlot.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED))
                .setUpload(PublicationUpload.newBuilder().setSizeBytes(bytes.size()).setSha256(DocumentPartCodec.sha256Hex(bytes.toByteArray()))
                        .setContentType("application/protobuf")));
        return PublishDocumentRequest.newBuilder().setIntent(source.getIntent().toBuilder().setOperationId(UUID.randomUUID().toString())
                        .clearMembers().addMembers(member)).addAllModes(source.getModesList())
                .addPayloads(DocumentPublicationPayload.newBuilder().setMemberId(member.getMemberId()).setRevisionOrdinal(ordinal).setContent(bytes)).build();
    }

    private static void verify(RepoServices host, RepositoryCaller caller, DocumentPublishedRevision revision, Document expected) {
        try (var read = host.historicalRepository().readValidated(caller, revision.getAddress(), UUID.fromString(revision.getRevisionId()), RepositoryReadControl.NONE)) {
            require(read.document().equals(expected), "actual retained document readback");
            read.authorizeDelivery(RepositoryReadControl.NONE);
        }
    }

    private static void verifyIdentity(Tx tx, RepositoryCaller caller, PublishDocumentRequest request, DocumentPublishedRevision revision) {
        var ledger = new DocumentReadLedger(tx, UUID.randomUUID());
        var capture = ledger.captureHistorical(caller, revision.getAddress(), UUID.fromString(revision.getRevisionId()));
        try (var use = capture.use()) {
            require(use.plan().entries().size() == request.getIntent().getMembers(0).getPartsCount(), "all declared parts are retained");
            int reused = 0;
            for (var entry : use.plan().entries()) {
                var declaration = request.getIntent().getMembers(0).getParts(entry.revisionOrdinal());
                if (!declaration.hasHistoricalReuse()) continue;
                var expected = declaration.getHistoricalReuse().getObject();
                var part = entry.part().part(); var binding = entry.part().binding();
                require(entry.objectId().toString().equals(expected.getObjectId()) && binding.generation().equals(expected.getBackendGeneration())
                                && binding.profile().storageRealm().equals(expected.getStorageRealm()) && binding.namespace().equals(expected.getNamespace())
                                && part.key().equals(expected.getObjectKey()) && part.size() == expected.getSizeBytes()
                                && part.sha256().equals(expected.getSha256())
                                && (part.providerVersion() == null ? expected.getProviderVersion().isEmpty()
                                        : part.providerVersion().equals(expected.getProviderVersion())),
                        "host preserves exact retained physical identity and version semantics");
                reused++;
            }
            long expected = request.getIntent().getMembers(0).getPartsList().stream().filter(DocumentPublicationPart::hasHistoricalReuse).count();
            require(expected > 0 && reused == expected, "every declared reused part preserves its identity");
        } finally { capture.close(); ledger.releaseDrained(16); }
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
