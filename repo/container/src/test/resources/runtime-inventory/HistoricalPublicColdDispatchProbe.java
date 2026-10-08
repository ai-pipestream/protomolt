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
import java.util.concurrent.atomic.AtomicInteger;

/** Fresh-process public recovery accepts caller request bytes, not predecessor plans or handles. */
final class HistoricalPublicColdDispatchProbe {
    static void run(Tx tx, AssessmentProviderProbe provider, RepositoryCaller caller, DocumentPublicationCommand command,
            Map<Integer, ByteString> uploads, PayloadBudget budget) throws Exception {
        long baseline = budget.reservedBytes();
        var member = command.intent().getMembers(0);
        var request = PublishDocumentRequest.newBuilder().setIntent(command.intent())
                .addModes(DocumentPublicationMemberMode.newBuilder().setMemberId(member.getMemberId())
                        .setMode(DocumentPublicationMode.DOCUMENT_PUBLICATION_MODE_TYPED));
        uploads.forEach((ordinal, bytes) -> request.addPayloads(DocumentPublicationPayload.newBuilder()
                .setMemberId(member.getMemberId()).setRevisionOrdinal(ordinal).setContent(bytes)));
        var coordinator = new RepositoryCaller(caller.principalName(), true);
        DocumentPublicationRuntime.RecoveryAuthority authority = (account, principal, operation) -> {
            require(account.equals(command.intent().getAccountId()) && principal.equals(caller.principalName())
                    && operation.equals(command.operationId()), "exact cold recovery authority lookup");
            return coordinator;
        };
        var reads = new DocumentReadLedger(tx, UUID.randomUUID());
        var providerReads = new AtomicInteger();
        var reader = new DocumentPartReader((generation, profile) -> {
            require(generation.equals("assessment-s3") && profile.equals(provider.profile()), "exact retained historical read backend");
            providerReads.incrementAndGet();
            return provider.store();
        }, 4, 4_000_000, budget);
        try (var opened = new ai.protomolt.proto.repo.blob.s3.S3BlobStoreProvider().open(Map.of(
                "endpoint", System.getenv("PROTOMOLT_TEST_S3_ENDPOINT"), "region", System.getenv("PROTOMOLT_TEST_S3_REGION"),
                "path-style", "true", "conditional-writes", "true", "access-key", System.getenv("PROTOMOLT_TEST_S3_ACCESS"),
                "secret-key", System.getenv("PROTOMOLT_TEST_S3_SECRET")))) {
            var runtime = DocumentPublicationRuntime.historicalJournaled(tx, new DriveLedger(tx), reads, reader, budget,
                    (generation, profile) -> {
                        require(generation.equals("assessment-s3") && profile.equals(provider.profile()), "exact recovered upload backend");
                        return new DocumentPublicationRuntime.Backend(profile.identity(), opened);
                    }, new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000),
                    new SqlTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(15)), 2, Duration.ofMillis(25),
                    Duration.ofMinutes(2), 2, DocumentPublicationCommand.MAX_COMMAND_BYTES, 16, false,
                    new DocumentPublicationRuntime.Assessments(Path.of(System.getenv("PROTOMOLT_TEST_RUNTIME_BUNDLE")),
                            Duration.ofMinutes(5), Duration.ofSeconds(5)), authority::forOperation,
                    new DocumentPublicationRuntime.ExternalWorkers() {
                        public void closeAdmission() {}
                        public boolean awaitIdle(Duration wait) { return true; }
                    }, 2, authority);
            var selections = new AtomicInteger();
            var resolutions = new AtomicInteger();
            var container = ObservedAssessmentProbe.asset(Document.getDescriptor());
            var definition = ObservedAssessmentProbe.asset(com.google.protobuf.StringValue.getDescriptor());
            var repository = runtime.repository((actual, selected, control) -> {
                require(actual.equals(caller) && selected.canonical().equals(command.canonical()), "scoped cold selection");
                selections.incrementAndGet();
                // Deliberately no placement: recovery must load the immutable journal, not begin initial ownership.
                return new DocumentPublicationRuntime.PublicationSelection(Map.of(), Map.of(), Optional.of(container),
                        (scopeCaller, selectedMember, scopeControl) -> new DocumentSchemaAdmission.Resolution() {
                            public DocumentSchemaAdmission.Definition select(DocumentSchemaAdmission.Selection occurrence) {
                                require(selectedMember.getParts(occurrence.ordinal()).hasUpload(), "historical schema uses retained definition");
                                resolutions.incrementAndGet();
                                return definition;
                            }
                            public void close() {}
                        });
            });
            try {
                HistoricalPublicDispatchProbe.transport(repository, caller, request.build(), selections, resolutions, false);
                require(providerReads.get() > 0, "fresh process reads actual historical provider bytes");
                require(resolutions.get() == 1 && selections.get() == 1, "only upload resolves a fresh schema");
                var replay = repository.publishDocument(caller, request.build(), RepositoryReadControl.NONE);
                require(replay.hasCommitted(), "library replay sees the transport's durable commit");
                require(new DocumentPublicationReplay(tx).observe(caller, command).result().orElseThrow().equals(replay.getCommitted()),
                        "public receipt equals the durable operation result");
                require(selections.get() == 1 && resolutions.get() == 1, "library replay uses no selection or schema lookup");
                verifyPublished(tx, provider, reads, caller, command, uploads, replay.getCommitted());
                runtime.tick();
                require(runtime.withHistoricalAttempts(attempts -> attempts.drain().unresolved()) == 0,
                        "fresh runtime retires its recovered generation");
            } finally {
                runtime.close();
                boolean stopped = false;
                for (int pass = 0; pass < 16 && !stopped; pass++) stopped = runtime.shutdownStep(Duration.ofSeconds(1));
                require(stopped, "fresh historical runtime drains");
            }
        }
        require(reads.outstandingReads() == 0 && budget.reservedBytes() == baseline, "fresh runtime releases reads and bytes");
        System.out.println("HISTORICAL_PUBLIC_COLD_DISPATCH_GRPC_OK");
    }

    private static void verifyPublished(Tx tx, AssessmentProviderProbe provider, DocumentReadLedger reads, RepositoryCaller caller,
            DocumentPublicationCommand command, Map<Integer, ByteString> uploads, DocumentPublicationResult result) {
        var member = result.getMembers(0);
        var current = new DocumentLedger(tx).findByNodeId(
                ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(member.getAddress())).orElseThrow();
        var revision = UUID.fromString(member.getRevisionId());
        require(current.mutationRevision == member.getMutationRevision()
                        && new DocumentPublicationLedger(tx).findForRead(current).orElseThrow().revisionId().equals(revision),
                "cold publication receipt identifies the current committed revision");
        var captured = reads.captureHistorical(caller, member.getAddress(), revision);
        try {
            try (var use = captured.use()) {
                require(use.plan().entries().size() == command.intent().getMembers(0).getPartsCount(), "all cold publication parts retained");
                for (var entry : use.plan().entries()) {
                    var declaration = command.intent().getMembers(0).getParts(entry.revisionOrdinal());
                    var part = entry.part().part();
                    var binding = entry.part().binding();
                    var bytes = provider.store().getBounded(binding.namespace(), part.key(), part.providerVersion(),
                            Math.toIntExact(part.size())).data();
                    require(bytes.length == part.size()
                                    && ai.protomolt.proto.repo.codec.DocumentPartCodec.sha256Hex(bytes).equals(part.sha256()),
                            "cold published provider version matches size and checksum");
                    if (declaration.hasUpload()) {
                        require(Arrays.equals(bytes, uploads.get(entry.revisionOrdinal()).toByteArray()), "cold upload bytes preserved");
                    } else {
                        var original = declaration.getHistoricalReuse().getObject();
                        require(entry.objectId().toString().equals(original.getObjectId())
                                        && binding.generation().equals(original.getBackendGeneration())
                                        && binding.profile().storageRealm().equals(original.getStorageRealm())
                                        && binding.namespace().equals(original.getNamespace())
                                        && part.key().equals(original.getObjectKey())
                                        && part.providerVersion().equals(original.getProviderVersion()),
                                "cold recovery reuses the exact historical physical identity");
                    }
                }
            }
        } finally { captured.close(); reads.releaseDrained(16); }
        for (var declaration : command.intent().getMembers(0).getPartsList()) {
            if (!declaration.hasHistoricalReuse()) continue;
            var source = declaration.getHistoricalReuse();
            var original = reads.captureHistorical(caller, source.getSource(), UUID.fromString(source.getRevisionId()));
            try (var use = original.use()) {
                var entry = use.plan().entries().stream().filter(part -> part.revisionOrdinal() == source.getRevisionOrdinal())
                        .findFirst().orElseThrow();
                require(entry.objectId().toString().equals(source.getObject().getObjectId())
                                && entry.part().part().providerVersion().equals(source.getObject().getProviderVersion()),
                        "original historical revision remains independently readable and unchanged");
            } finally { original.close(); reads.releaseDrained(16); }
        }
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
