package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.engine.DocumentPartReader;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Public library and authenticated gRPC dispatch against the fixture's real SQL and storage. */
final class HistoricalPublicDispatchProbe {
    static void run(Tx tx, AssessmentProviderProbe provider, RepositoryCaller caller, DocumentPublicationCommand original,
            DocumentUploadPlan.Placement placement, DocumentPublishedRevision source,
            Map<Integer, ByteString> fragments, PayloadBudget budget, javax.sql.DataSource database) throws Exception {
        run(tx, provider, caller, original, placement, source, fragments, budget, null, "normal");
        for (String phase : List.of("start", "create")) {
            try (var fault = new HistoricalCreateCommitFault(database, true)) {
                run(fault.tx(), provider, caller, original, placement, source, fragments, budget, fault, phase);
            }
        }
        run(tx, provider, caller, original, placement, source, fragments, budget, null, "reject");
        run(tx, provider, caller, original, placement, source, fragments, budget, null, "takeover");
    }

    private static void run(Tx tx, AssessmentProviderProbe provider, RepositoryCaller caller, DocumentPublicationCommand original,
            DocumentUploadPlan.Placement placement, DocumentPublishedRevision source,
            Map<Integer, ByteString> fragments, PayloadBudget budget, HistoricalCreateCommitFault fault, String phase) throws Exception {
        long baseline = budget.reservedBytes();
        var reads = new DocumentReadLedger(tx, UUID.randomUUID());
        var reader = new DocumentPartReader((generation, profile) -> {
            require(generation.equals(placement.generation()) && profile.equals(provider.profile()), "exact historical read provider");
            return provider.store();
        }, 4, 4_000_000, budget);
        var coordinator = new RepositoryCaller(caller.principalName(), true);
        var keys = new HashSet<UUID>();
        DocumentPublicationRuntime.RecoveryAuthority authority = (account, principal, operation) -> {
            require(account.equals(original.intent().getAccountId()) && principal.equals(caller.principalName())
                    && keys.contains(operation), "exact host coordinator lookup");
            return coordinator;
        };
        try (var opened = new ai.protomolt.proto.repo.blob.s3.S3BlobStoreProvider().open(Map.of(
                "endpoint", System.getenv("PROTOMOLT_TEST_S3_ENDPOINT"), "region", System.getenv("PROTOMOLT_TEST_S3_REGION"),
                "path-style", "true", "conditional-writes", "true", "access-key", System.getenv("PROTOMOLT_TEST_S3_ACCESS"),
                "secret-key", System.getenv("PROTOMOLT_TEST_S3_SECRET")));
             var takeover = new HistoricalPublicTakeoverProbe(opened)) {
            var runtime = DocumentPublicationRuntime.historicalJournaled(tx, new DriveLedger(tx), reads, reader, budget,
                    (generation, profile) -> {
                        require(generation.equals(placement.generation()) && profile.equals(provider.profile()), "exact historical upload provider");
                        return new DocumentPublicationRuntime.Backend(profile.identity(), phase.equals("takeover") ? takeover.opened() : opened);
                    }, new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000),
                    new SqlTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(15)), 2, Duration.ofMillis(25),
                    phase.equals("takeover") ? Duration.ofSeconds(2) : Duration.ofMinutes(2), 2, DocumentPublicationCommand.MAX_COMMAND_BYTES, 16, false,
                    new DocumentPublicationRuntime.Assessments(Path.of(System.getenv("PROTOMOLT_TEST_RUNTIME_BUNDLE")),
                            Duration.ofMinutes(5), Duration.ofSeconds(5)), authority::forOperation,
                    new DocumentPublicationRuntime.ExternalWorkers() {
                        public void closeAdmission() {}
                        public boolean awaitIdle(Duration wait) { return true; }
                    }, 2, authority);
            var selections = new AtomicInteger();
            var resolutions = new AtomicInteger();
            var container = ObservedAssessmentProbe.asset(Document.getDescriptor());
            var definition = phase.equals("reject") ? ObservedAssessmentProbe.invalidSchema()
                    : ObservedAssessmentProbe.asset(com.google.protobuf.StringValue.getDescriptor());
            var armed = new java.util.concurrent.atomic.AtomicBoolean();
            var repository = runtime.repository((actual, command, control) -> {
                require(actual.equals(caller), "actual scoped caller reaches selection");
                selections.incrementAndGet();
                return new DocumentPublicationRuntime.PublicationSelection(Map.of(placement.drive().id(),
                        new DocumentPublicationRuntime.Placement(new DriveLedger(tx).findById(placement.drive().id()).orElseThrow(),
                                placement.generation(), placement.profile())),
                        Map.of(), Optional.of(container),
                        (scopeCaller, member, scopeControl) -> {
                            if (phase.equals("create") && armed.compareAndSet(false, true)) {
                                var assessment = tx.readOnly(em -> (UUID) em.createNativeQuery("""
                                        SELECT assessment_id FROM repository_publication_assessment_starts
                                        WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=0
                                        """).setParameter("a", command.intent().getAccountId()).setParameter("p", caller.principalName())
                                        .setParameter("o", command.operationId()).getSingleResult());
                                fault.arm(assessment);
                            }
                            return new DocumentSchemaAdmission.Resolution() {
                            public DocumentSchemaAdmission.Definition select(DocumentSchemaAdmission.Selection occurrence) {
                                resolutions.incrementAndGet();
                                return definition;
                            }
                            public void close() {}
                            };
                        });
            });
            try {
                for (boolean remote : fault == null ? List.of(false, true) : List.of(false)) {
                    var current = new DocumentLedger(tx).findByNodeId(
                            ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(source.getAddress())).orElseThrow();
                    var member = original.intent().getMembers(0);
                    var command = new DocumentPublicationCommand(original.intent().toBuilder().setOperationId(UUID.randomUUID().toString())
                            .setMembers(0, member.toBuilder().setDestination(member.getDestination().toBuilder()
                                    .setExpectedMutationRevision(current.mutationRevision))).build());
                    keys.add(command.operationId());
                    var request = PublishDocumentRequest.newBuilder().setIntent(command.intent())
                            .addModes(DocumentPublicationMemberMode.newBuilder().setMemberId(member.getMemberId())
                                    .setMode(DocumentPublicationMode.DOCUMENT_PUBLICATION_MODE_TYPED));
                    for (int ordinal = 0; ordinal < member.getPartsCount(); ordinal++) if (member.getParts(ordinal).hasUpload())
                        request.addPayloads(DocumentPublicationPayload.newBuilder().setMemberId(member.getMemberId())
                                .setRevisionOrdinal(ordinal).setContent(fragments.get(ordinal)));
                    if (phase.equals("start")) fault.armStart(new RepositoryOperationLedger.Key(command.intent().getAccountId(),
                            caller.principalName(), command.operationId()));
                    if (fault != null) {
                        try {
                            repository.publishDocument(caller, request.build(), RepositoryReadControl.NONE);
                            throw new AssertionError("Public historical call did not lose its " + phase + " acknowledgement");
                        } catch (RuntimeException failure) { fault.requireFailure(failure); }
                    }
                    int resolvedBeforeRetry = resolutions.get();
                    if (phase.equals("takeover") && !remote) {
                        takeover.exercise(tx, runtime, repository, caller, request.build(), selections, resolutions);
                        HistoricalPublicColdDispatchProbe.verifyPublished(tx, provider, reads, caller, command, fragments,
                                repository.publishDocument(caller, request.build(), RepositoryReadControl.NONE).getCommitted());
                    } else if (remote) transport(repository, caller, request.build(), selections, resolutions, phase.equals("reject"));
                    else exercise(repository, caller, request.build(), selections, resolutions, phase.equals("reject"));
                    if (phase.equals("create")) require(resolvedBeforeRetry > 0 && resolutions.get() == resolvedBeforeRetry,
                            "public CREATE retry reuses retained assessment without schema resolution");
                    runtime.tick();
                    require(runtime.withHistoricalAttempts(attempts -> attempts.drain().unresolved()) == 0,
                            "completed public operation releases its generation slot");
                }
            } finally {
                takeover.close();
                runtime.close();
                boolean stopped = false;
                for (int pass = 0; pass < 16 && !stopped; pass++) stopped = runtime.shutdownStep(Duration.ofSeconds(1));
                require(stopped, "public historical runtime drains");
            }
        }
        require(budget.reservedBytes() == baseline, "public historical dispatch releases byte reservations");
        System.out.println(phase.equals("takeover") ? "HISTORICAL_PUBLIC_CONCURRENT_TAKEOVER_OK"
                : phase.equals("reject") ? "HISTORICAL_PUBLIC_REJECTION_LIBRARY_GRPC_OK"
                : fault == null ? "HISTORICAL_PUBLIC_DISPATCH_LIBRARY_GRPC_OK"
                : "HISTORICAL_PUBLIC_" + phase.toUpperCase(java.util.Locale.ROOT) + "_ACK_RECOVERY_OK");
    }

    private static void exercise(DocumentPublicationRepository repository, RepositoryCaller caller, PublishDocumentRequest request,
            AtomicInteger selections, AtomicInteger resolutions, boolean rejected) {
        var result = repository.publishDocument(caller, request, RepositoryReadControl.NONE);
        require(rejected ? result.hasRejected() : result.hasCommitted(), "public historical publication returns expected decision");
        DocumentPublicationResponseValidator.requireValid(new DocumentPublicationCommand(request.getIntent()), caller.principalName(), result);
        int selected = selections.get(), resolved = resolutions.get();
        require(repository.publishDocument(caller, request, RepositoryReadControl.NONE).equals(result), "exact retry returns same receipt");
        require(selections.get() == selected && resolutions.get() == resolved, "terminal replay performs no selection or resolution");
    }

    static void transport(DocumentPublicationRepository repository, RepositoryCaller caller, PublishDocumentRequest request,
            AtomicInteger selections, AtomicInteger resolutions, boolean rejected) throws Exception {
        String token = "historical-fixture-" + UUID.randomUUID();
        var credential = caller.credentialBinding().orElseThrow();
        var authentication = new ai.protomolt.proto.authz.AuthenticatedCaller(
                ai.protomolt.proto.actions.Caller.scoped(caller.principalName(), Set.of()),
                Optional.of(new ai.protomolt.proto.authz.CredentialBinding(credential.issuer(), credential.credentialId(), credential.generation())));
        var delivery = new PayloadBudget(64_000_000);
        var clientBudget = new PayloadBudget(64_000_000);
        try (var service = new ai.protomolt.proto.repo.service.DocumentPublicationGrpcService(repository, actual -> {
            require(actual.equals(authentication), "transport binds authenticated fixture identity");
            return caller;
        }, delivery, 2); var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            String name = io.grpc.inprocess.InProcessServerBuilder.generateName();
            var server = io.grpc.inprocess.InProcessServerBuilder.forName(name).executor(executor)
                    .maxInboundMessageSize(DocumentPublicationInput.MAX_ENVELOPE_BYTES)
                    .intercept(new ai.protomolt.proto.authz.grpc.ApiTokenServerInterceptor("operator-fixture-" + UUID.randomUUID(),
                            (ai.protomolt.proto.authz.AuthenticatedCallerResolver) value -> token.equals(value)
                                    ? Optional.of(authentication) : Optional.empty()))
                    .addService(service).build().start();
            var channel = io.grpc.inprocess.InProcessChannelBuilder.forName(name).build();
            try {
                var headers = new io.grpc.Metadata();
                headers.put(io.grpc.Metadata.Key.of("authorization", io.grpc.Metadata.ASCII_STRING_MARSHALLER), "Bearer " + token);
                var stub = DocumentPublicationServiceGrpc.newFutureStub(channel)
                        .withInterceptors(io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(headers));
                var remote = new ai.protomolt.proto.repo.publication.grpc.RemoteDocumentPublicationRepository(
                        caller, stub, clientBudget, Duration.ofSeconds(60), 2);
                exercise(remote, caller, request, selections, resolutions, rejected);
            } finally {
                channel.shutdownNow(); require(channel.awaitTermination(10, TimeUnit.SECONDS), "historical channel drains");
                server.shutdownNow(); require(server.awaitTermination(10, TimeUnit.SECONDS), "historical server drains");
            }
        }
        require(clientBudget.reservedBytes() == 0 && delivery.reservedBytes() == 0, "transport budgets released");
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
