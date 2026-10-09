package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.engine.DocumentPartReader;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.DocumentPublishedRevision;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Real SQL captures and provider batches owned by the publication runtime's shutdown. */
final class HistoricalRuntimeShutdownProbe {
    static void run(Tx tx, AssessmentProviderProbe provider, RepositoryCaller caller,
            DocumentPublicationCommand original, DocumentUploadPlan.Placement placement,
            DocumentPublishedRevision source, PayloadBudget budget) throws Exception {
        long baseline = budget.reservedBytes();
        var reads = new DocumentReadLedger(tx, UUID.randomUUID());
        var reader = new TrackedReader(new DocumentPartReader((generation, profile) -> {
            require(generation.equals(placement.generation()) && profile.equals(provider.profile()), "exact provider binding");
            return provider.store();
        }, 4, 4_000_000, budget));
        var externalClosed = new AtomicBoolean();
        var failAuthority = new AtomicBoolean();
        var coordinator = new RepositoryCaller(caller.principalName(), true);
        var keys = new HashSet<RepositoryOperationLedger.Key>();
        var runtime = DocumentPublicationRuntime.historicalJournaled(tx, new DriveLedger(tx), reads, reader, budget,
                (generation, profile) -> { throw new AssertionError("Read-only shutdown fixture selected an upload backend"); },
                new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000),
                new SqlTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(15)), 2, Duration.ofMillis(25),
                Duration.ofMinutes(2), 2, DocumentPublicationCommand.MAX_COMMAND_BYTES, 16, false,
                new DocumentPublicationRuntime.Assessments(Path.of(System.getenv("PROTOMOLT_TEST_RUNTIME_BUNDLE")),
                        Duration.ofMinutes(5), Duration.ofSeconds(5)),
                (account, principal, operation) -> {
                    require(keys.contains(new RepositoryOperationLedger.Key(account, principal, operation)), "exact operation drain lookup");
                    if (failAuthority.get()) throw new IllegalStateException("injected authority lookup failure");
                    return coordinator;
                }, new DocumentPublicationRuntime.ExternalWorkers() {
                    public void closeAdmission() { externalClosed.set(true); }
                    public boolean awaitIdle(Duration wait) { return true; }
                }, 2);
        var retained = new java.util.concurrent.atomic.AtomicReference<RepositoryInstalledHistoricalAttempts>();
        var held = new java.util.concurrent.atomic.AtomicReference<DocumentRetainedReader.Batch>();
        var histories = new ArrayList<DocumentReadLedger.PinnedHistory>();
        try {
            runtime.withHistoricalAttempts(attempts -> {
            retained.set(attempts); // Test inspection only; no registry mutations after the action.
            for (int index = 0; index < 2; index++) {
                var current = new DocumentLedger(tx).findByNodeId(
                        ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(source.getAddress())).orElseThrow();
                var member = original.intent().getMembers(0);
                var command = new DocumentPublicationCommand(original.intent().toBuilder().setOperationId(UUID.randomUUID().toString())
                        .setMembers(0, member.toBuilder().setDestination(member.getDestination().toBuilder()
                                .setExpectedMutationRevision(current.mutationRevision))).build());
                var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
                keys.add(key);
                var record = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                        Map.of(placement.drive().id(), placement), Duration.ofMinutes(2), 0);
                try (var request = attempts.beginInitial(caller, record,
                        Map.of("a", DocumentPublicationCandidate.Mode.TYPED), UUID.randomUUID())) {
                    var history = reads.captureHistorical(caller, source.getAddress(), UUID.fromString(source.getRevisionId()));
                    histories.add(history);
                    var sources = DocumentHistoricalAssessmentSources.open(command, caller, List.of(history), RepositoryReadControl.NONE);
                    var accepted = sources.work();
                    try { request.attachSources(sources, accepted, RepositoryReadControl.NONE); }
                    catch (RuntimeException | Error failure) { accepted.close(); sources.close(); history.close(); throw failure; }
                    sources.close();
                    request.openExecution(coordinator, RepositoryReadControl.NONE);
                    if (index == 0) {
                        int ordinal = member.getPartsList().stream().filter(part -> part.hasHistoricalReuse())
                                .findFirst().orElseThrow().getHistoricalReuse().getRevisionOrdinal();
                        held.set(reader.readHistorical(history, ordinal, RepositoryReadControl.NONE));
                        require(!held.get().parts().isEmpty(), "real provider bytes remain borrowed");
                    }
                }
            }
            runtime.close();
            require(!runtime.shutdownStep(Duration.ZERO), "accepted action prevents shutdown even between registry borrows");
            require(attempts.drain().unresolved() == 2, "accepted action retains both generations");
            try {
                runtime.withHistoricalAttempts(ignored -> { throw new AssertionError("Closed runtime admitted new historical work"); });
                throw new AssertionError("Missing closed-runtime refusal");
            } catch (RepositoryException expected) {
                require(expected.code() == RepositoryException.Code.UNAVAILABLE, "new historical call is refused after close");
            }
            return null;
            });
            var attempts = retained.get();
            require(!reader.closed && !externalClosed.get(), "outer close preserves nested resources");
            failAuthority.set(true);
            try { runtime.shutdownStep(Duration.ZERO); throw new AssertionError("Missing authority failure"); }
            catch (IllegalStateException expected) { require(expected.getMessage().contains("injected authority"), "lookup failure survives"); }
            require(attempts.drain().unresolved() == 2 && !reader.closed, "failed cleanup retains owners and reader");
            failAuthority.set(false);
            require(!runtime.shutdownStep(Duration.ZERO), "held batch prevents runtime shutdown");
            require(attempts.drain().unresolved() == 1 && !histories.get(0).isReleased() && histories.get(1).isReleased(),
                    "ready generation drains despite held earlier generation");
            require(!reader.closed && !externalClosed.get() && budget.reservedBytes() > baseline,
                    "timeout retains reader admission and byte ownership");
            held.getAndSet(null).close();
            boolean stopped = false;
            for (int pass = 0; pass < 4 && !stopped; pass++) stopped = runtime.shutdownStep(Duration.ofSeconds(1));
            require(stopped && reader.closed && externalClosed.get(), "shutdown retry closes nested resources");
            require(attempts.drain().unresolved() == 0 && reads.outstandingReads() == 0
                    && budget.reservedBytes() == baseline, "shutdown returns all captures and memory");
        } finally {
            failAuthority.set(false);
            if (held.get() != null) held.getAndSet(null).close();
            runtime.close();
            boolean stopped = false;
            for (int pass = 0; pass < 4 && !stopped; pass++) stopped = runtime.shutdownStep(Duration.ofSeconds(1));
            require(stopped, "fixture shutdown completes");
        }
        System.out.println("HISTORICAL_RUNTIME_SHUTDOWN_OK");
    }

    private static final class TrackedReader implements DocumentRetainedReader, DocumentAssessmentReader,
            DocumentHistoricalRetainedReader, DocumentReadLifecycle.Reader {
        private final DocumentPartReader delegate;
        private boolean closed;
        TrackedReader(DocumentPartReader delegate) { this.delegate = delegate; }
        public Batch readRetained(DocumentReadLedger.PinnedPlan plan, String member, RepositoryReadControl control) {
            return delegate.readRetained(plan, member, control);
        }
        public Batch readAssessment(DocumentReadLedger.PinnedAssessment capture, String member, RepositoryReadControl control) {
            return delegate.readAssessment(capture, member, control);
        }
        public Batch readHistorical(DocumentReadLedger.PinnedHistory history, int ordinal, RepositoryReadControl control) {
            return delegate.readHistorical(history, ordinal, control);
        }
        public void close() { closed = true; delegate.close(); }
        public boolean awaitIdle(Duration wait) throws InterruptedException { return delegate.awaitIdle(wait); }
    }
    private static void require(boolean condition, String message) { if (!condition) throw new AssertionError(message); }
}
