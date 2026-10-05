package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.container.ledger.DocumentHistoricalReadPlan;
import ai.protomolt.proto.repo.container.ledger.DocumentReadLedger;
import ai.protomolt.proto.repo.spi.HistoricalDocumentRepository;
import ai.protomolt.proto.repo.spi.HistoricalMaterializationRepository;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentManifest;
import ai.protomolt.proto.repo.v1.NodeAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Shared capture/read/delivery behavior. The host owns reader maintenance and shutdown. */
public final class DocumentHistoricalOperations implements HistoricalDocumentRepository, HistoricalMaterializationRepository {
    private final DocumentReadLedger ledger;
    private final DocumentPartReader parts;
    private final DocumentHistoricalReader validated;

    /** Reader and ledger are borrowed from one host lifecycle; this facade never closes either. */
    public DocumentHistoricalOperations(DocumentReadLedger ledger, DocumentPartReader parts, PayloadBudget budget) {
        this.ledger = Objects.requireNonNull(ledger);
        this.parts = Objects.requireNonNull(parts);
        validated = new DocumentHistoricalReader(parts, budget);
    }

    @Override public RawRead readRaw(RepositoryCaller caller, NodeAddress address, UUID revision, RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        DocumentReadBatch batch = null;
        boolean delivered = false;
        try (var history = capture(caller, address, revision, control); var use = history.use()) {
            try {
                control.check();
                var plan = use.plan();
                batch = parts.readHistorical(history, control);
                var content = batch.parts();
                if (content.size() != plan.entries().size()) throw new RepositoryException(
                        RepositoryException.Code.DATA_LOSS, "Historical fragment count differs from captured revision");
                var fragments = new ArrayList<Fragment>(content.size());
                for (int index = 0; index < content.size(); index++) {
                    control.check();
                    fragments.add(new Fragment(plan.entries().get(index).revisionOrdinal(), ByteBuffer.wrap(content.get(index).bytes())));
                }
                control.check();
                var result = new Raw(plan, history, batch, fragments);
                history.authorizeDelivery(control);
                delivered = true;
                return result;
            } catch (RuntimeException failure) {
                reauthorizeFailure(history, control, failure);
                throw failure;
            }
        } finally {
            if (!delivered && batch != null) batch.close();
        }
    }

    @Override public ValidatedRead readValidated(RepositoryCaller caller, NodeAddress address, UUID revision, RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        DocumentHistoricalRead result = null;
        boolean delivered = false;
        try (var history = capture(caller, address, revision, control)) {
            try {
                control.check();
                result = validated.readValidated(history, control);
                control.check();
                history.authorizeDelivery(control);
                delivered = true;
                return result;
            } catch (RuntimeException failure) {
                reauthorizeFailure(history, control, failure);
                throw failure;
            }
        } finally {
            if (!delivered && result != null) result.close();
        }
    }

    @Override public HistoricalMaterializationRepository.Result readMaterialized(
            RepositoryCaller caller, NodeAddress address, UUID revision,
            HistoricalMaterializationRepository.Selection selection,
            HistoricalMaterializationRepository.Limits limits, RepositoryReadControl control) {
        Objects.requireNonNull(selection); Objects.requireNonNull(limits);
        var result = readMaterialized(caller, address, revision, selection.revisionOrdinal(),
                new ai.protomolt.proto.repo.admission.DocumentSchemaMaterialization.Selection(selection.rootSha256(), selection.pathSha256()),
                new ai.protomolt.proto.repo.admission.DocumentSchemaMaterialization.Limits(limits.maxFragmentBytes(),
                        limits.maxEvidenceBytes(), limits.maxRetainedBytes(), limits.maxReferences(),
                        limits.maxDecodedBytes(), limits.maxBoundaries()), control);
        boolean transferred = false;
        try {
            var adapted = new Materialized(selection, result);
            transferred = true;
            return adapted;
        } finally { if (!transferred) result.close(); }
    }

    private record Materialized(HistoricalMaterializationRepository.Selection selection,
            ai.protomolt.proto.repo.container.ledger.DocumentHistoricalMaterialization result)
            implements HistoricalMaterializationRepository.Result {
        @Override public synchronized HistoricalMaterializationRepository.View view(RepositoryReadControl control) {
            var view = result.view(control);
            var occurrence = view.occurrence();
            return new HistoricalMaterializationRepository.View(selection, view.original(), view.value(), view.schema(),
                    new HistoricalMaterializationRepository.Occurrence(occurrence.ordinal(), occurrence.root(), occurrence.typeUrl(),
                            occurrence.prefix(), occurrence.valueSha256(), occurrence.valueSizeBytes()),
                    new HistoricalMaterializationRepository.Definition(view.metadata(), view.descriptorArtifact(),
                            view.reference(), view.metadataArtifact()));
        }
        @Override public synchronized void close() { result.close(); }
    }

    /** Selected typed view; does not confer a fresh validation verdict. */
    public ai.protomolt.proto.repo.container.ledger.DocumentHistoricalMaterialization readMaterialized(
            RepositoryCaller caller, NodeAddress address, UUID revision, int ordinal,
            ai.protomolt.proto.repo.admission.DocumentSchemaMaterialization.Selection selection,
            ai.protomolt.proto.repo.admission.DocumentSchemaMaterialization.Limits limits,
            RepositoryReadControl control) {
        Objects.requireNonNull(control).check(); Objects.requireNonNull(selection); Objects.requireNonNull(limits);
        ai.protomolt.proto.repo.container.ledger.DocumentHistoricalMaterialization result = null;
        boolean delivered = false;
        try (var history = capture(caller, address, revision, control)) {
            try {
                result = validated.readMaterialized(history, ordinal, selection, limits, control);
                history.authorizeDelivery(control);
                delivered = true;
                return result;
            } catch (RuntimeException failure) {
                reauthorizeFailure(history, control, failure);
                throw failure;
            }
        } finally {
            if (!delivered && result != null) result.close();
        }
    }

    private DocumentReadLedger.PinnedHistory capture(RepositoryCaller caller, NodeAddress address,
            UUID revision, RepositoryReadControl control) {
        Objects.requireNonNull(caller); Objects.requireNonNull(address); Objects.requireNonNull(revision);
        ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(address);
        // Result close stays local. Recover one drained slot only when admission
        // needs it, without hiding a failed SQL release or retrying uncertain capture.
        ledger.releaseDrainedAtCapacity(1);
        control.check();
        return ledger.captureHistorical(caller, address, revision);
    }

    private static void reauthorizeFailure(DocumentReadLedger.PinnedHistory history,
            RepositoryReadControl control, RuntimeException failure) {
        control.check();
        if (failure instanceof RepositoryException repository
                && (repository.code() == RepositoryException.Code.CANCELLED
                || repository.code() == RepositoryException.Code.DEADLINE_EXCEEDED))
            throw new RepositoryException(repository.code(), "Historical read cancelled or expired");
        history.authorizeDelivery(control);
    }

    private static final class Raw implements RawRead {
        private final DocumentHistoricalReadPlan plan;
        private final DocumentReadLedger.PinnedHistory history;
        private DocumentReadBatch bytes;
        private List<Fragment> fragments;
        private Raw(DocumentHistoricalReadPlan plan, DocumentReadLedger.PinnedHistory history,
                DocumentReadBatch bytes, List<Fragment> fragments) {
            this.plan = plan; this.history = history; this.bytes = bytes; this.fragments = List.copyOf(fragments);
        }
        private void requireOpen() { if (bytes == null) throw new IllegalStateException("Historical read is closed"); }
        @Override public synchronized NodeAddress address() { requireOpen(); return plan.address(); }
        @Override public synchronized UUID revision() { requireOpen(); return plan.revision(); }
        @Override public synchronized long publicationRevision() { requireOpen(); return plan.publicationRevision(); }
        @Override public synchronized DocumentManifest manifest() { requireOpen(); return plan.manifest(); }
        @Override public synchronized ai.protomolt.proto.repo.v1.HistoricalDocumentMetadata metadata() { requireOpen(); return plan.metadata(); }
        @Override public synchronized List<Fragment> fragments() { requireOpen(); return fragments; }
        @Override public synchronized void authorizeDelivery(RepositoryReadControl control) {
            requireOpen(); history.authorizeDelivery(control);
        }
        @Override public synchronized void close() {
            if (bytes == null) return;
            bytes.close(); bytes = null; fragments = null;
        }
    }
}
