package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.repo.admission.*;
import ai.protomolt.proto.repo.schema.registry.RegistrySchemaResolver;
import ai.protomolt.proto.repo.service.*;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.schema.registry.git.GitSchemaRegistryStore;
import com.google.protobuf.ByteString;
import io.grpc.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** A new managed service recovers a dead writer using only persisted state and caller bytes. */
final class ManagedHistoricalColdDispatchProbe {
    static void run(Tx tx, AssessmentProviderProbe provider, RepositoryCaller caller, DocumentPublicationCommand command,
            Map<Integer, ByteString> uploads) throws Exception {
        var config = new RepoServiceConfig(0, new LedgerConfig(System.getenv("PROTOMOLT_TEST_JDBC"),
                System.getenv("PROTOMOLT_TEST_USER"), System.getenv("PROTOMOLT_TEST_PASSWORD")),
                System.getenv("PROTOMOLT_TEST_S3_ENDPOINT"), System.getenv("PROTOMOLT_TEST_S3_REGION"),
                System.getenv("PROTOMOLT_TEST_S3_ACCESS"), System.getenv("PROTOMOLT_TEST_S3_SECRET"),
                "managed-historical-cold", 0, "s3", null, null, null, 0, 0L)
                .withManagedStorage(new ManagedStoragePolicy("assessment-s3", "assessment-provider", true));
        var identity = new ReaderHostOptions(UUID.randomUUID(), "managed-historical-cold", UUID.randomUUID().toString());
        var request = PublishDocumentRequest.newBuilder().setIntent(command.intent())
                .addModes(DocumentPublicationMemberMode.newBuilder().setMemberId(command.intent().getMembers(0).getMemberId())
                        .setMode(DocumentPublicationMode.DOCUMENT_PUBLICATION_MODE_TYPED));
        uploads.forEach((ordinal, bytes) -> request.addPayloads(DocumentPublicationPayload.newBuilder()
                .setMemberId(command.intent().getMembers(0).getMemberId()).setRevisionOrdinal(ordinal).setContent(bytes)));
        var coordinator = new RepositoryCaller(caller.principalName(), true);
        ManagedPublicationOptions.OperationAuthority authority = (account, principal, operation) -> {
            require(account.equals(command.intent().getAccountId()) && principal.equals(caller.principalName())
                    && operation.equals(command.operationId()), "exact managed recovery authority");
            return coordinator;
        };
        var fresh = ObservedAssessmentProbe.asset(com.google.protobuf.StringValue.getDescriptor());
        Path directory = Files.createTempDirectory("managed-cold-git");
        var resolutions = new AtomicInteger();
        var closes = new AtomicInteger();
        try (var git = GitSchemaRegistryStore.builder().repositoryDir(directory).build()) {
            git.putDescriptorSet(fresh.metadata().getArtifactSha256(), fresh.descriptors());
            var resolver = new RegistrySchemaResolver(git, new DocumentSchemaArtifactCache.Limits(8_000_000, 16, 4_000_000), 4, 16);
            ManagedSchemaAccess schemas = new ManagedSchemaAccess() {
                public DocumentSchemaAdmission.Resolution open(RepositoryCaller actual, DocumentPublicationMember member, RepositoryReadControl control) {
                    require(actual.equals(caller), "managed schema scope preserves scoped credential binding");
                    return resolver.open(occurrence -> {
                        require(member.getParts(occurrence.ordinal()).hasUpload(), "retained roots never use live registry");
                        resolutions.incrementAndGet();
                        return new RegistrySchemaResolver.Selected(fresh.metadata(), fresh.source());
                    }, control::check);
                }
                public void close() { closes.incrementAndGet(); resolver.close(); }
                public boolean awaitIdle(Duration timeout) throws InterruptedException { return resolver.awaitLoads(timeout); }
            };
            try {
            var credential = caller.credentialBinding().orElseThrow();
            var authentication = new ai.protomolt.proto.authz.AuthenticatedCaller(
                    ai.protomolt.proto.actions.Caller.scoped(caller.principalName(), Set.of()),
                    Optional.of(new ai.protomolt.proto.authz.CredentialBinding(credential.issuer(), credential.credentialId(), credential.generation())));
            var options = new ManagedPublicationOptions(Path.of(System.getenv("PROTOMOLT_TEST_RUNTIME_BUNDLE")),
                    Duration.ofMinutes(5), Duration.ofSeconds(5), authority).withRecovery(authority).withHistoricalPublication(2)
                    .withTransport(new ManagedPublicationOptions.Transport(auth -> {
                        require(auth.equals(authentication), "transport preserves the independently bound scoped test credential");
                        // Synthetic authentication fixture; the live credential row is independently checked by the runtime.
                        return caller;
                    }, 32L * 1024 * 1024, 2));
                try (var host = RepoServices.buildHosted(config, BridgeEngine.standard(), null, schemas, options, identity)) {
                    var retained = new ManagedBackendLedger(tx).find("assessment-s3").orElseThrow();
                    require(retained.equals(provider.profile()), "managed provider profile equals the immutable source binding");
                    String token = "managed-cold-token-" + UUID.randomUUID();
                    String name = "managed-cold-" + UUID.randomUUID();
                    host.startInProcess(name, "operator-" + UUID.randomUUID(),
                            (ai.protomolt.proto.authz.AuthenticatedCallerResolver) supplied -> supplied.equals(token)
                                    ? Optional.of(authentication) : Optional.empty());
                    var channel = io.grpc.inprocess.InProcessChannelBuilder.forName(name).build();
                    try {
                        var plain = DocumentPublicationServiceGrpc.newBlockingStub(channel).withDeadlineAfter(30, TimeUnit.SECONDS);
                        try { plain.publishDocument(request.build()); throw new AssertionError("Unauthenticated cold recovery accepted"); }
                        catch (StatusRuntimeException failure) { require(failure.getStatus().getCode() == Status.Code.UNAUTHENTICATED, "cold recovery requires authentication"); }
                        require(resolutions.get() == 0, "unauthenticated request does not resolve schema");
                        var headers = new Metadata();
                        headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
                        var stub = plain.withInterceptors(io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(headers));
                        var result = stub.publishDocument(request.build());
                        require(result.hasCommitted(), "managed cold recovery commits");
                        require(resolutions.get() == 1, "only resubmitted upload resolves a fresh schema");
                        require(host.publicationRepository().publishDocument(caller, request.build(), RepositoryReadControl.NONE).equals(result)
                                && stub.publishDocument(request.build()).equals(result), "managed library and RPC recover exact durable receipt");
                        require(new DocumentPublicationReplay(tx).observe(caller, command).result().orElseThrow().equals(result.getCommitted()),
                                "managed receipt equals persisted operation result");
                        require(resolutions.get() == 1, "managed replay performs no live schema work");
                        var reads = new DocumentReadLedger(tx, UUID.randomUUID());
                        HistoricalPublicColdDispatchProbe.verifyPublished(tx, provider, reads, caller, command, uploads, result.getCommitted());
                        require(reads.outstandingReads() == 0, "verification captures released");
                        var revision = result.getCommitted().getMembers(0);
                        try (var read = host.historicalRepository().readValidated(caller, revision.getAddress(),
                                UUID.fromString(revision.getRevisionId()), RepositoryReadControl.NONE)) {
                            var uploaded = Document.parseFrom(uploads.values().iterator().next());
                            require(!uploaded.getParserResultsMap().isEmpty(), "cold upload contains a parser shape");
                            require(read.document().getParserResultsMap().entrySet().containsAll(uploaded.getParserResultsMap().entrySet()),
                                    "managed retained validation includes the supplied parser shape");
                            read.authorizeDelivery(RepositoryReadControl.NONE);
                        }
                    } finally { channel.shutdownNow(); require(channel.awaitTermination(5, TimeUnit.SECONDS), "managed cold channel drained"); }
                }
                require(closes.get() == 1, "managed cold host closes owned schema admission once");
                var states = tx.readOnly(em -> em.createNativeQuery("""
                        SELECT r.state,r.quiescence_source,h.state FROM repository_reader_incarnations r
                        JOIN repository_reader_host_executions h ON h.execution=r.host_execution WHERE h.execution=:id
                        """).setParameter("id", identity.execution()).getResultList());
                require(states.size() == 2, "full managed host owns document and archive readers");
                for (var state : states) {
                    var row = (Object[]) state;
                    require("QUIESCED".equals(row[0]) && "LOCAL_DRAIN".equals(row[1]) && "FENCED".equals(row[2]),
                            "both managed readers and host close with local drain evidence");
                }
            } finally { schemas.close(); require(schemas.awaitIdle(Duration.ofSeconds(5)), "managed registry drained before Git close"); }
        } finally {
            try (var paths = Files.walk(directory)) { for (var path : paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path); }
        }
        System.out.println("HISTORICAL_MANAGED_COLD_DISPATCH_GRPC_OK");
    }

    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
