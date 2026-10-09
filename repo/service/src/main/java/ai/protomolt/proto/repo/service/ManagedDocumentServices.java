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
import ai.protomolt.proto.repo.spi.RepositoryException;
import java.time.Duration;
import java.util.UUID;
import java.util.Objects;
import java.util.Optional;

/** Host-owned native document resources and explicitly configured historical transport. */
final class ManagedDocumentServices {
    final DocumentPublicationRuntime publication;
    final ai.protomolt.proto.repo.spi.DocumentPublicationRepository publicationRepository;
    final DocumentPublicationGrpcService publicationService;
    final ai.protomolt.proto.repo.engine.DocumentHistoricalOperations history;
    final DocumentHistoryGrpcService historyService;
    final DocumentHistoryMaterializationGrpcService materializationService;
    final ManagedSchemaAccess schemas;
    private final boolean managedDrain;

    record Journaled(DocumentPublicationRuntime.Assessments assessments, DocumentPublicationRuntime.DrainAuthority authority,
            DocumentPublicationRuntime.RecoveryAuthority recovery, ManagedPublicationOptions.Transport transport,
            Optional<ManagedPublicationOptions.Historical> historical) {
        Journaled {
            Objects.requireNonNull(assessments); Objects.requireNonNull(authority); Objects.requireNonNull(historical);
            if (historical.isPresent()) Objects.requireNonNull(recovery, "Historical recovery authority");
        }
        Journaled(DocumentPublicationRuntime.Assessments assessments, DocumentPublicationRuntime.DrainAuthority authority,
                DocumentPublicationRuntime.RecoveryAuthority recovery, ManagedPublicationOptions.Transport transport) {
            this(assessments,authority,recovery,transport,Optional.empty());
        }
        Journaled(DocumentPublicationRuntime.Assessments assessments, DocumentPublicationRuntime.DrainAuthority authority,
                DocumentPublicationRuntime.RecoveryAuthority recovery) {
            this(assessments,authority,recovery,null);
        }
        Journaled(DocumentPublicationRuntime.Assessments assessments, DocumentPublicationRuntime.DrainAuthority authority) {
            this(assessments,authority,null);
        }
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
        this(tx,drives,generation,profile,backing,deliverEvents,access,schemas,journaled,null);
    }

    ManagedDocumentServices(Tx tx, DriveLedger drives, String generation,
            ManagedBackendLedger.Profile profile, OpenedBlobStore backing, boolean deliverEvents, HistoricalReadAccess access,
            ManagedSchemaAccess schemas, Journaled journaled, BoundedDocumentProfile boundedDocuments) {
        this(tx, drives, generation, profile, backing, deliverEvents, access, schemas, journaled,
                boundedDocuments, null);
    }

    ManagedDocumentServices(Tx tx, DriveLedger drives, String generation,
            ManagedBackendLedger.Profile profile, OpenedBlobStore backing, boolean deliverEvents, HistoricalReadAccess access,
            ManagedSchemaAccess schemas, Journaled journaled, BoundedDocumentProfile boundedDocuments, UUID hostExecution) {
        Objects.requireNonNull(backing);
        managedDrain = journaled != null;
        if (managedDrain) Objects.requireNonNull(schemas, "Managed journaled publication requires owned schema access");
        this.schemas = schemas;
        var timeouts = new SqlTimeouts(Duration.ofSeconds(2), Duration.ofSeconds(5));
        var bounded = tx.withTimeouts(timeouts);
        var profiles = new ManagedBackendLedger(bounded);
        if (!profiles.find(generation).filter(profile::equals).isPresent())
            throw new IllegalStateException("Original document backend profile is not bound");
        var selection=journaled==null ? null : new ManagedPublicationSelection(
                new DriveLedger(bounded,drives::validateBackend),profiles,generation,profile,schemas);
        // Construct transport state before acquiring native worker/lifecycle resources.
        // This composition is not exposed until publicationRepository is assigned.
        var transport=journaled==null ? null : journaled.transport();
        publicationService=transport==null ? null : new DocumentPublicationGrpcService(this::publish,
                transport.bindings(),new PayloadBudget(transport.deliveryBudgetBytes()),transport.maxConcurrentCalls());
        var budget = new PayloadBudget(boundedDocuments == null ? 64L * 1024 * 1024 : boundedDocuments.payloadBudgetBytes());
        var reader = new DocumentPartReader((original, selected) -> {
            requireOriginal(generation, profile, original, selected);
            return backing.store();
        }, 16, 8L * 1024 * 1024, budget);
        // Neither object has escaped and the reader starts no provider work during construction.
        final DocumentReadLedger ledger;
        try {
            ledger = hostExecution == null ? new DocumentReadLedger(bounded, UUID.randomUUID(), 32)
                    : new DocumentReadLedger(bounded, UUID.randomUUID(), hostExecution, 32);
        } catch (RuntimeException | Error failure) {
            reader.close();
            throw failure;
        }
        DocumentPublicationRuntime constructed = null;
        var schemaOwnership = managedDrain ? new ManagedSchemaOwnership(schemas) : null;
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
            if (journaled == null) constructed = new DocumentPublicationRuntime(bounded, drives, ledger, reader, budget, backends,
                    limits, timeouts, 4, Duration.ofMillis(25), Duration.ofMinutes(5), 32, 8L * 1024 * 1024, 100, deliverEvents);
            else {
                try {
                    if (journaled.historical().isPresent()) constructed = DocumentPublicationRuntime.managedHistoricalJournaled(
                            bounded, drives, ledger, reader, budget, backends,
                            limits, timeouts, 4, Duration.ofMillis(25), Duration.ofMinutes(5), 32, 8L * 1024 * 1024, 100,
                            deliverEvents, journaled.assessments(), journaled.authority(), schemaOwnership,
                            journaled.historical().orElseThrow().generationCapacity(), journaled.recovery());
                    else constructed = DocumentPublicationRuntime.managedJournaled(bounded, drives, ledger, reader, budget, backends,
                            limits, timeouts, 4, Duration.ofMillis(25), Duration.ofMinutes(5), 32, 8L * 1024 * 1024, 100,
                            deliverEvents, journaled.assessments(), journaled.authority(), schemaOwnership, journaled.recovery());
                } catch (java.io.IOException failure) { throw new java.io.UncheckedIOException("Cannot observe managed publication runtime", failure); }
            }
            var repository=selection==null ? null : constructed.repository(selection,
                    boundedDocuments == null ? (int)ai.protomolt.proto.repo.spi.DocumentPublicationInput.MAX_UPLOAD_BYTES
                            : boundedDocuments.maxObjectBytes());
            if (schemaOwnership != null) schemaOwnership.transfer();
            publication = constructed;
            publicationRepository = repository;
        } catch (RuntimeException | Error failure) {
            // Nothing has been exposed: no calls, batches or provider workers can exist.
            try {
                if (publicationService != null) publicationService.close();
                boolean drained;
                if (constructed == null) drained = new DocumentReadLifecycle(ledger, reader, 100).shutdownStep(Duration.ofSeconds(5));
                else {
                    constructed.close();
                    drained = constructed.shutdownStep(Duration.ofSeconds(5));
                }
                if (!drained)
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
            throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                    "Original document backend is unavailable");
    }

    private ai.protomolt.proto.repo.v1.PublishDocumentResponse publish(
            ai.protomolt.proto.repo.spi.RepositoryCaller caller, ai.protomolt.proto.repo.v1.PublishDocumentRequest request,
            ai.protomolt.proto.repo.spi.RepositoryReadControl control) {
        return publicationRepository.publishDocument(caller,request,control);
    }

    void awaitTransportIdle(Duration timeout) {
        try {
            if (publicationService!=null && !publicationService.awaitIdle(timeout))
                throw new RepositoryDrainTimeoutException(RepositoryDrainTimeoutException.Phase.PUBLICATION_RPC,
                        "Publication RPCs still active; shared resources retained");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Publication RPC drain interrupted; shared resources retained",interrupted);
        }
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
        if (publicationService!=null) publicationService.close();
        publication.close();
        if (!managedDrain && schemas != null) schemas.close();
    }
}
