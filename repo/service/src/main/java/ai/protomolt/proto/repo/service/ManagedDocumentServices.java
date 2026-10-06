package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.container.ledger.DocumentPublicationRuntime;
import ai.protomolt.proto.repo.container.ledger.DocumentReadLedger;
import ai.protomolt.proto.repo.container.ledger.DocumentReadLifecycle;
import ai.protomolt.proto.repo.container.ledger.DriveLedger;
import ai.protomolt.proto.repo.container.ledger.ManagedBackendLedger;
import ai.protomolt.proto.repo.container.ledger.SqlTimeouts;
import ai.protomolt.proto.repo.container.ledger.Tx;
import ai.protomolt.proto.repo.engine.DocumentPartReader;
import java.time.Duration;
import java.util.UUID;
import java.util.Objects;

/** Host-owned native document resources and explicitly configured historical transport. */
final class ManagedDocumentServices {
    final DocumentPublicationRuntime publication;
    final ai.protomolt.proto.repo.engine.DocumentHistoricalOperations history;
    final DocumentHistoryGrpcService historyService;
    final DocumentHistoryMaterializationGrpcService materializationService;
    final ManagedSchemaAccess schemas;
    private final boolean managedDrain;

    record Journaled(DocumentPublicationRuntime.Assessments assessments, DocumentPublicationRuntime.DrainAuthority authority) {
        Journaled { Objects.requireNonNull(assessments); Objects.requireNonNull(authority); }
    }

    ManagedDocumentServices(Tx tx, DriveLedger drives, String generation,
            ManagedBackendLedger.Profile profile, OpenedBlobStore backing, boolean deliverEvents) {
        this(tx, drives, generation, profile, backing, deliverEvents, null);
    }

    ManagedDocumentServices(Tx tx, DriveLedger drives, String generation,
            ManagedBackendLedger.Profile profile, OpenedBlobStore backing, boolean deliverEvents, HistoricalReadAccess access) {
        this(tx, drives, generation, profile, backing, deliverEvents, access, null);
    }

    ManagedDocumentServices(Tx tx, DriveLedger drives, String generation,
            ManagedBackendLedger.Profile profile, OpenedBlobStore backing, boolean deliverEvents, HistoricalReadAccess access,
            ManagedSchemaAccess schemas) {
        this(tx, drives, generation, profile, backing, deliverEvents, access, schemas, null);
    }

    ManagedDocumentServices(Tx tx, DriveLedger drives, String generation,
            ManagedBackendLedger.Profile profile, OpenedBlobStore backing, boolean deliverEvents, HistoricalReadAccess access,
            ManagedSchemaAccess schemas, Journaled journaled) {
        Objects.requireNonNull(backing);
        managedDrain = journaled != null;
        if (managedDrain) Objects.requireNonNull(schemas, "Managed journaled publication requires owned schema access");
        this.schemas = schemas;
        var timeouts = new SqlTimeouts(Duration.ofSeconds(2), Duration.ofSeconds(5));
        var bounded = tx.withTimeouts(timeouts);
        var profiles = new ManagedBackendLedger(bounded);
        if (!profiles.find(generation).filter(profile::equals).isPresent())
            throw new IllegalStateException("Original document backend profile is not bound");
        var budget = new PayloadBudget(64L * 1024 * 1024);
        var reader = new DocumentPartReader((original, selected) -> {
            requireOriginal(generation, profile, original, selected);
            return backing.store();
        }, 16, 8L * 1024 * 1024, budget);
        var ledger = new DocumentReadLedger(bounded, UUID.randomUUID(), 32);
        try {
            history = new ai.protomolt.proto.repo.engine.DocumentHistoricalOperations(ledger, reader, budget);
            var responses = access == null ? null : new PayloadBudget(access.responseBudgetBytes());
            historyService = access == null ? null : new DocumentHistoryGrpcService(history, access.bindings(),
                    responses, access.maxConcurrentCalls());
            materializationService = access == null ? null : access.materializationLimits().map(limits ->
                    new DocumentHistoryMaterializationGrpcService(history, access.bindings(), limits,
                            responses, access.maxConcurrentCalls())).orElse(null);
            DocumentPublicationRuntime.Backends backends = (original, selected) -> {
                requireOriginal(generation, profile, original, selected);
                return new DocumentPublicationRuntime.Backend(profile.identity(), backing);
            };
            var limits = new DocumentRevisionAssembly.Limits(8L * 1024 * 1024, 10_000, 100, 100_000, 1_000_000);
            if (journaled == null) publication = new DocumentPublicationRuntime(bounded, drives, ledger, reader, budget, backends,
                    limits, timeouts, 4, Duration.ofMillis(25), Duration.ofMinutes(5), 32, 8L * 1024 * 1024, 100, deliverEvents);
            else {
                try {
                    publication = DocumentPublicationRuntime.managedJournaled(bounded, drives, ledger, reader, budget, backends,
                            limits, timeouts, 4, Duration.ofMillis(25), Duration.ofMinutes(5), 32, 8L * 1024 * 1024, 100,
                            deliverEvents, journaled.assessments(), journaled.authority(), new DocumentPublicationRuntime.ExternalWorkers() {
                                public void closeAdmission() { schemas.close(); }
                                public boolean awaitIdle(Duration timeout) throws InterruptedException { return schemas.awaitIdle(timeout); }
                            });
                } catch (java.io.IOException failure) { throw new java.io.UncheckedIOException("Cannot observe managed publication runtime", failure); }
            }
        } catch (RuntimeException | Error failure) {
            // Nothing has been exposed: no calls, batches or provider workers can exist.
            try {
                if (!new DocumentReadLifecycle(ledger, reader, 100).shutdownStep(Duration.ofSeconds(5)))
                    throw new IllegalStateException("Fresh publication reader did not quiesce after startup failure");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                failure.addSuppressed(interrupted);
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != failure) failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private static void requireOriginal(String generation, ManagedBackendLedger.Profile profile,
            String original, ManagedBackendLedger.Profile selected) {
        if (!generation.equals(original) || !profile.equals(selected))
            throw new IllegalStateException("Original document backend is not configured on this host");
    }

    void drain(Duration timeout) {
        long budget = timeout.toNanos(), start = System.nanoTime();
        try {
            while (!publication.shutdownStep(Duration.ofNanos(Math.max(0, budget - (System.nanoTime() - start))))) {
                if (System.nanoTime() - start >= budget)
                    throw new IllegalStateException("Native publication resources still active; shared resources retained");
            }
            if (!managedDrain && schemas != null && !schemas.awaitIdle(Duration.ofNanos(Math.max(0, budget - (System.nanoTime() - start)))))
                throw new IllegalStateException("Schema provider loads still active; shared resources retained");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Native publication drain interrupted; shared resources retained", interrupted);
        }
    }

    void closeAdmission() {
        publication.close();
        if (!managedDrain && schemas != null) schemas.close();
    }
}
