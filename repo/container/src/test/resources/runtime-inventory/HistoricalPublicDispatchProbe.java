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
        for (String phase : List.of("start", "create", "publication")) {
            try (var fault = new HistoricalCreateCommitFault(database, true)) {
                run(fault.tx(), provider, caller, original, placement, source, fragments, budget, fault, phase);
            }
        }
        run(tx, provider, caller, original, placement, source, fragments, budget, null, "reject");
        run(tx, provider, caller, original, placement, source, fragments, budget, null, "takeover");
        run(tx, provider, caller, original, placement, source, fragments, budget, null, "cancel");
        run(tx, provider, caller, original, placement, source, fragments, budget, null, "shutdown");
        run(tx, provider, caller, original, placement, source, fragments, budget, null, "rpc-cancel");
        run(tx, provider, caller, original, placement, source, fragments, budget, null, "rpc-deadline");
        run(tx, provider, caller, original, placement, source, fragments, budget, null, "rpc-close");
        run(tx, provider, caller, original, placement, source, fragments, budget, null, "cleanup");
        run(tx, provider, caller, original, placement, source, fragments, budget, null, "cleanup-sql");
        for (String phase : List.of("credential-library", "credential-grpc")) {
            var binding = new RepositoryCredentialBinding("public-revocation", UUID.randomUUID(), 1);
            new RepositoryCredentialAuthorities(tx).register(new RepositoryCaller("operator", true), binding, caller.principalName());
            var scoped = new RepositoryCaller(caller.principalName(), false, caller.accountIds(), caller.identities(), Optional.of(binding));
            run(tx, provider, scoped, original, placement, source, fragments, budget, null, phase);
        }
        for (String phase : List.of("read-library", "read-grpc", "write-library", "write-grpc", "policy-library", "policy-grpc"))
            run(tx, provider, caller, original, placement, source, fragments, budget, null, phase);
    }

    private static void run(Tx tx, AssessmentProviderProbe provider, RepositoryCaller caller, DocumentPublicationCommand original,
            DocumentUploadPlan.Placement placement, DocumentPublishedRevision source,
            Map<Integer, ByteString> fragments, PayloadBudget budget, HistoricalCreateCommitFault fault, String phase) throws Exception {
        long baseline = budget.reservedBytes();
        boolean stoppingCase = phase.equals("cancel") || phase.equals("shutdown") || phase.startsWith("rpc-")
                || phase.startsWith("credential-") || phase.startsWith("read-") || phase.startsWith("write-") || phase.startsWith("policy-");
        var readerId = UUID.randomUUID();
        var reads = new DocumentReadLedger(tx, readerId);
        var reader = new DocumentPartReader((generation, profile) -> {
            require(generation.equals(placement.generation()) && profile.equals(provider.profile()), "exact historical read provider");
            return provider.store();
        }, 4, 4_000_000, budget);
        var coordinator = new RepositoryCaller(caller.principalName(), true);
        var failCleanup = new java.util.concurrent.atomic.AtomicBoolean();
        var cleanupFailure = new IllegalStateException("injected public cleanup authority failure");
        var keys = new HashSet<UUID>();
        DocumentPublicationRuntime.RecoveryAuthority authority = (account, principal, operation) -> {
            require(account.equals(original.intent().getAccountId()) && principal.equals(caller.principalName())
                    && keys.contains(operation), "exact host coordinator lookup");
            if (failCleanup.get()) throw cleanupFailure;
            return coordinator;
        };
        try (var opened = new ai.protomolt.proto.repo.blob.s3.S3BlobStoreProvider().open(Map.of(
                "endpoint", System.getenv("PROTOMOLT_TEST_S3_ENDPOINT"), "region", System.getenv("PROTOMOLT_TEST_S3_REGION"),
                "path-style", "true", "conditional-writes", "true", "access-key", System.getenv("PROTOMOLT_TEST_S3_ACCESS"),
                "secret-key", System.getenv("PROTOMOLT_TEST_S3_SECRET")));
             var takeover = new HistoricalPublicTakeoverProbe(opened)) {
            // This phase observes actual provider calls without holding their replies.
            if (phase.equals("publication") || phase.equals("normal") || phase.equals("start") || phase.startsWith("cleanup")) takeover.close();
            var runtime = DocumentPublicationRuntime.historicalJournaled(tx, new DriveLedger(tx), reads, reader, budget,
                    (generation, profile) -> {
                        require(generation.equals(placement.generation()) && profile.equals(provider.profile()), "exact historical upload provider");
                        return new DocumentPublicationRuntime.Backend(profile.identity(), phase.equals("takeover")
                                || phase.equals("publication") || phase.equals("normal") || phase.equals("start") || phase.startsWith("cleanup")
                                || stoppingCase ? takeover.opened() : opened);
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
                for (boolean remote : fault == null && !stoppingCase ? List.of(false, true) : List.of(false)) {
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
                    if (phase.equals("publication")) fault.armPublication(new RepositoryOperationLedger.Key(command.intent().getAccountId(),
                            caller.principalName(), command.operationId()));
                    if (fault != null) {
                        try {
                            repository.publishDocument(caller, request.build(), RepositoryReadControl.NONE);
                            throw new AssertionError("Public historical call did not lose its " + phase + " acknowledgement");
                        } catch (RuntimeException failure) { fault.requireFailure(failure); }
                    }
                    if (phase.equals("start")) {
                        replayRefusals(repository, caller, request.build(), selections, resolutions, takeover, true);
                        System.out.println("HISTORICAL_PUBLIC_PENDING_REPLAY_REFUSALS_LIBRARY_GRPC_OK");
                    }
                    byte[] durableBytes = phase.equals("publication") ? tx.readOnly(em ->
                            (byte[]) em.createNativeQuery("SELECT result_bytes FROM repository_operation_success"
                                            + " WHERE account_id=:a AND principal=:p AND operation_id=:o")
                                    .setParameter("a", command.intent().getAccountId()).setParameter("p", caller.principalName())
                                    .setParameter("o", command.operationId()).getSingleResult()) : null;
                    var durablePublication = durableBytes == null ? null : DocumentPublicationResult.parseFrom(durableBytes);
                    int resolvedBeforeRetry = resolutions.get();
                    int selectedBeforeRetry = selections.get(), putsBeforeRetry = takeover.completedPuts();
                    if (stoppingCase) {
                        if (phase.endsWith("-library")) HistoricalPublicAuthorizationProbe.run(tx, takeover, runtime, reads,
                                repository, caller, request.build(), selections, resolutions, phase.split("-")[0],
                                ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(source.getAddress()));
                        else if (phase.endsWith("-grpc")) withTransport(repository, caller, 2,
                                (remoteRepository, service, delivery, serverCancelled) -> HistoricalPublicAuthorizationProbe.run(
                                        tx, takeover, runtime, reads, remoteRepository, caller, request.build(), selections, resolutions,
                                        phase.split("-")[0], ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(source.getAddress())));
                        else if (phase.equals("rpc-cancel") || phase.equals("rpc-deadline")) withTransport(repository, caller, 1,
                                phase.equals("rpc-deadline") ? Duration.ofSeconds(10) : Duration.ofSeconds(60),
                                (remoteRepository, service, delivery, serverCancelled) -> takeover.exerciseRemoteCancellation(
                                        tx, runtime, reads, remoteRepository, caller, request.build(), service, delivery, serverCancelled,
                                        phase.equals("rpc-deadline")));
                        else if (phase.equals("rpc-close")) withTransport(repository, caller, 1,
                                (remoteRepository, service, delivery, serverCancelled) -> takeover.exerciseTransportClose(
                                        tx, runtime, reads, remoteRepository, caller, request.build(), service, delivery));
                        else takeover.exerciseStop(tx, runtime, reads, repository, caller, request.build(), phase.equals("cancel"));
                        continue;
                    } else if (phase.equals("takeover") && !remote) {
                        takeover.exercise(tx, runtime, repository, caller, request.build(), selections, resolutions);
                        HistoricalPublicColdDispatchProbe.verifyPublished(tx, provider, reads, caller, command, fragments,
                                repository.publishDocument(caller, request.build(), RepositoryReadControl.NONE).getCommitted());
                    } else if (remote) transport(repository, caller, request.build(), selections, resolutions, phase.equals("reject"));
                    else exercise(repository, caller, request.build(), selections, resolutions, phase.equals("reject"));
                    if (phase.equals("create")) require(resolvedBeforeRetry > 0 && resolutions.get() == resolvedBeforeRetry,
                            "public CREATE retry reuses retained assessment without schema resolution");
                    if (phase.equals("normal") && !remote) {
                        replayRefusals(repository, caller, request.build(), selections, resolutions, takeover, false);
                        System.out.println("HISTORICAL_PUBLIC_REPLAY_REFUSALS_LIBRARY_GRPC_OK");
                    }
                    if (phase.equals("publication")) {
                        require(repository.publishDocument(caller, request.build(), RepositoryReadControl.NONE)
                                        .getCommitted().equals(durablePublication), "library receipt equals the original SQL commit receipt");
                        withTransport(repository, caller, 2, (remoteRepository, service, delivery, serverCancelled) -> {
                            var received = remoteRepository.publishDocument(caller, request.build(), RepositoryReadControl.NONE);
                            require(received.getCommitted().equals(durablePublication), "gRPC receipt equals the original SQL commit receipt");
                            require(remoteRepository.publishDocument(caller, request.build(), RepositoryReadControl.NONE).equals(received),
                                    "gRPC exact replay preserves the original SQL receipt");
                        });
                        require(putsBeforeRetry == 1 && takeover.completedPuts() == putsBeforeRetry
                                        && selectedBeforeRetry == selections.get() && resolvedBeforeRetry > 0
                                        && resolvedBeforeRetry == resolutions.get(),
                                "lost publication reply replays through library and gRPC without provider or schema work");
                        for (String table : List.of("repository_operation_success", "document_revision_commits", "document_assessment_owners"))
                            require(tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table
                                            + " WHERE account_id=:a AND principal=:p AND operation_id=:o")
                                    .setParameter("a", command.intent().getAccountId()).setParameter("p", caller.principalName())
                                    .setParameter("o", command.operationId()).getSingleResult()).longValue()) == 1,
                                    "lost publication reply preserves exactly one row in " + table);
                        HistoricalPublicColdDispatchProbe.verifyPublished(tx, provider, reads, caller, command, fragments,
                                repository.publishDocument(caller, request.build(), RepositoryReadControl.NONE).getCommitted());
                    }
                    if (phase.equals("cleanup") && !remote) {
                        var committed = repository.publishDocument(caller, request.build(), RepositoryReadControl.NONE);
                        int puts = takeover.completedPuts(), selected = selections.get(), resolved = resolutions.get();
                        failCleanup.set(true);
                        try {
                            require(repository.publishDocument(caller, request.build(), RepositoryReadControl.NONE).equals(committed),
                                    "authorized receipt delivery does not require cleanup authority");
                            transport(repository, caller, request.build(), selections, resolutions, false);
                            try {
                                runtime.tick();
                                throw new AssertionError("Cleanup authority failure was swallowed");
                            } catch (IllegalStateException failure) {
                                require(failure == cleanupFailure, "maintenance reports the original cleanup failure");
                            }
                            require(runtime.withHistoricalAttempts(attempts -> attempts.drain().unresolved()) == 1
                                            && reads.outstandingReads() > 0 && budget.reservedBytes() > baseline,
                                    "failed cleanup retains the generation, captures and bytes");
                            require(repository.publishDocument(caller, request.build(), RepositoryReadControl.NONE).equals(committed),
                                    "cleanup failure does not replace the durable receipt");
                            require(puts == takeover.completedPuts() && selected == selections.get() && resolved == resolutions.get(),
                                    "cleanup failure and terminal replay add no PUT, selection or schema resolution");
                        } finally { failCleanup.set(false); }
                    }
                    if (phase.equals("cleanup-sql") && !remote) {
                        var committed = repository.publishDocument(caller, request.build(), RepositoryReadControl.NONE);
                        int puts = takeover.completedPuts(), selected = selections.get(), resolved = resolutions.get();
                        var releaseFault = new HistoricalPinReleaseFault(tx, readerId, new RepositoryOperationLedger.Key(
                                command.intent().getAccountId(), caller.principalName(), command.operationId()));
                        try (releaseFault) {
                            try {
                                runtime.tick();
                                throw new AssertionError("SQL pin-release failure was swallowed");
                            } catch (RuntimeException failure) { releaseFault.requireFailure(failure); }
                            releaseFault.requireRetained();
                            require(runtime.withHistoricalAttempts(attempts -> attempts.drain().unresolved()) == 1
                                            && reads.outstandingReads() > 0 && budget.reservedBytes() > baseline,
                                    "SQL rollback retains generation, read lifetimes and bytes");
                            require(repository.publishDocument(caller, request.build(), RepositoryReadControl.NONE).equals(committed),
                                    "failed pin release preserves exact committed receipt");
                            withTransport(repository, caller, 2, (remoteRepository, service, delivery, serverCancelled) ->
                                    require(remoteRepository.publishDocument(caller, request.build(), RepositoryReadControl.NONE).equals(committed),
                                            "gRPC returns same receipt while SQL cleanup is failing"));
                            require(puts == takeover.completedPuts() && selected == selections.get() && resolved == resolutions.get(),
                                    "SQL cleanup failure and terminal replay add no provider PUT or schema work");
                        }
                        runtime.tick();
                        releaseFault.requireReleased();
                        HistoricalPublicColdDispatchProbe.verifyPublished(tx, provider, reads, caller, command, fragments, committed.getCommitted());
                    }
                    runtime.tick();
                    require(runtime.withHistoricalAttempts(attempts -> attempts.drain().unresolved()) == 0,
                            "completed public operation releases its generation slot");
                }
            } finally {
                failCleanup.set(false);
                takeover.close();
                runtime.close();
                boolean stopped = false;
                for (int pass = 0; pass < 16 && !stopped; pass++) stopped = runtime.shutdownStep(Duration.ofSeconds(1));
                require(stopped, "public historical runtime drains");
            }
        }
        require(budget.reservedBytes() == baseline, "public historical dispatch releases byte reservations");
        System.out.println(phase.equals("cleanup-sql") ? "HISTORICAL_PUBLIC_SQL_CLEANUP_RETRY_OK"
                : phase.equals("cleanup") ? "HISTORICAL_PUBLIC_CLEANUP_RETRY_OK"
                : stoppingCase ? "HISTORICAL_PUBLIC_" + phase.replace('-', '_').toUpperCase(java.util.Locale.ROOT) + "_DRAIN_OK"
                : phase.equals("takeover") ? "HISTORICAL_PUBLIC_CONCURRENT_TAKEOVER_OK"
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

    private static void replayRefusals(DocumentPublicationRepository repository, RepositoryCaller caller, PublishDocumentRequest request,
            AtomicInteger selections, AtomicInteger resolutions, HistoricalPublicTakeoverProbe provider, boolean pending) throws Exception {
        int selected = selections.get(), resolved = resolutions.get(), puts = provider.completedPuts();
        HistoricalPublicReplayRefusalProbe.intent(repository, caller, request, pending);
        withTransport(repository, caller, 4, (remoteRepository, service, delivery, serverCancelled) ->
                HistoricalPublicReplayRefusalProbe.intent(remoteRepository, caller, request, pending));
        for (var unauthorized : HistoricalPublicReplayRefusalProbe.unauthorized(caller)) {
            HistoricalPublicReplayRefusalProbe.refuse(repository, unauthorized.caller(), request, unauthorized.code());
            withTransport(repository, unauthorized.caller(), 2, (remoteRepository, service, delivery, serverCancelled) ->
                    HistoricalPublicReplayRefusalProbe.refuse(remoteRepository, unauthorized.caller(), request, unauthorized.code()));
        }
        require(selected == selections.get() && resolved == resolutions.get() && puts == provider.completedPuts(),
                "negative public replays perform no selection, schema resolution or PUT");
    }

    static void transport(DocumentPublicationRepository repository, RepositoryCaller caller, PublishDocumentRequest request,
            AtomicInteger selections, AtomicInteger resolutions, boolean rejected) throws Exception {
        withTransport(repository, caller, 2, (remote, service, delivery, serverCancelled) ->
                exercise(remote, caller, request, selections, resolutions, rejected));
    }

    @FunctionalInterface
    private interface TransportAction {
        void run(DocumentPublicationRepository remote, ai.protomolt.proto.repo.service.DocumentPublicationGrpcService service,
                PayloadBudget delivery, java.util.concurrent.CountDownLatch serverCancelled) throws Exception;
    }

    private static void withTransport(DocumentPublicationRepository repository, RepositoryCaller caller,
            int transportCapacity, TransportAction action) throws Exception {
        withTransport(repository, caller, transportCapacity, Duration.ofSeconds(60), action);
    }

    private static void withTransport(DocumentPublicationRepository repository, RepositoryCaller caller,
            int transportCapacity, Duration timeout, TransportAction action) throws Exception {
        String token = "historical-fixture-" + UUID.randomUUID();
        var credential = caller.credentialBinding().orElseThrow();
        var authentication = new ai.protomolt.proto.authz.AuthenticatedCaller(
                ai.protomolt.proto.actions.Caller.scoped(caller.principalName(), Set.of()),
                Optional.of(new ai.protomolt.proto.authz.CredentialBinding(credential.issuer(), credential.credentialId(), credential.generation())));
        var delivery = new PayloadBudget(64_000_000);
        var clientBudget = new PayloadBudget(64_000_000);
        var serverCancelled = new java.util.concurrent.CountDownLatch(1);
        try (var service = new ai.protomolt.proto.repo.service.DocumentPublicationGrpcService(repository, actual -> {
            require(actual.equals(authentication), "transport binds authenticated fixture identity");
            io.grpc.Context.current().addListener(context -> serverCancelled.countDown(), Runnable::run);
            return caller;
        }, delivery, transportCapacity); var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
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
                        caller, stub, clientBudget, timeout, 2);
                action.run(remote, service, delivery, serverCancelled);
            } finally {
                channel.shutdownNow(); require(channel.awaitTermination(10, TimeUnit.SECONDS), "historical channel drains");
                server.shutdownNow(); require(server.awaitTermination(10, TimeUnit.SECONDS), "historical server drains");
            }
        }
        require(clientBudget.reservedBytes() == 0 && delivery.reservedBytes() == 0, "transport budgets released");
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
