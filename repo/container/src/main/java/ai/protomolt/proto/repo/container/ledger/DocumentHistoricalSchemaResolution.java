package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentRetainedSchemaResolution;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Owns budgeted historical SQL assets and attempt-local retained schema resolution.
 * Borrows a live source Use; callers retain it through assessment and publication,
 * hash target fragments before opening this scope, and recheck current READ before
 * exposing a result. This loads definitions, never an inherited validation verdict.
 */
final class DocumentHistoricalSchemaResolution implements AutoCloseable {
    private DocumentRetainedSchemaResolution resolution;
    private final List<PayloadBudget.Lease> leases;
    private final Runnable control;

    private DocumentHistoricalSchemaResolution(DocumentRetainedSchemaResolution resolution,
            List<PayloadBudget.Lease> leases, Runnable control) {
        this.resolution = resolution; this.leases = List.copyOf(leases); this.control = control;
    }

    static DocumentHistoricalSchemaResolution open(DocumentReadLedger.PinnedHistory history,
            DocumentReadLedger.PinnedRead<DocumentHistoricalReadPlan>.Use use,
            Map<Integer, Integer> mapping, List<DocumentHistoricalReadPlan.Entry> selected,
            PayloadBudget budget, RepositoryReadControl readControl) {
        Runnable control = () -> { readControl.check(); use.plan(); };
        control.run();
        var plan = use.plan();
        if (mapping.size() > plan.entries().size() || selected.size() > plan.entries().size())
            throw new IllegalArgumentException("Retained schema selection exceeds pinned source");
        mapping = Map.copyOf(mapping);
        selected = List.copyOf(selected);
        var sourceEntries = new HashMap<Integer, DocumentHistoricalReadPlan.Entry>();
        for (var entry : plan.entries()) { control.run(); sourceEntries.put(entry.revisionOrdinal(), entry); }
        var ordinals = new java.util.HashSet<Integer>();
        for (var entry : selected) {
            control.run();
            if (!entry.equals(sourceEntries.get(entry.revisionOrdinal())) || !ordinals.add(entry.revisionOrdinal()))
                throw new IllegalArgumentException("Retained schema selection differs from pinned source");
        }
        if (mapping.size() != selected.size() || !ordinals.equals(new java.util.HashSet<>(mapping.values())))
            throw new IllegalArgumentException("Retained schema mapping differs from selected ordinals");
        var leases = new ArrayList<PayloadBudget.Lease>();
        DocumentRetainedSchemaResolution retained = null;
        boolean transferred = false;
        try {
            var snapshot = history.captureSchemas(use, control, bytes -> leases.add(reserve(budget, bytes)));
            final DocumentHistoricalSchemaBinding binding;
            try { binding = DocumentHistoricalSchemaBinding.read(plan.address(), snapshot, control); }
            catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
                throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Invalid historical schema binding", failure);
            }
            requireRootBindings(snapshot, binding, selected, budget, readControl);
            var source = binding.validationRequest(Map.of(), snapshot.roots());
            try {
                retained = DocumentRetainedSchemaResolution.open(source, mapping,
                        hash -> java.util.Optional.ofNullable(snapshot.artifacts().get(hash)), binding.policy().limits(),
                        bytes -> { var lease = reserve(budget, bytes); return lease::close; }, control);
            } catch (InvalidProtocolBufferException failure) {
                throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Invalid retained schema encoding", failure);
            }
            control.run();
            var result = new DocumentHistoricalSchemaResolution(retained, leases, control);
            transferred = true;
            return result;
        } finally {
            if (!transferred) {
                try { if (retained != null) retained.close(); }
                finally { for (int i = leases.size() - 1; i >= 0; i--) leases.get(i).close(); }
            }
        }
    }

    /** Borrowed per-member resolver; its definitions remain valid only while this scope is open. */
    DocumentRetainedSchemaResolution resolution() {
        if (resolution == null) throw new IllegalStateException("Historical schema resolution is closed");
        control.run();
        return resolution;
    }

    @Override public void close() {
        if (resolution == null) return;
        try { resolution.close(); }
        finally {
            resolution = null;
            for (int i = leases.size() - 1; i >= 0; i--) leases.get(i).close();
        }
    }

    static void requireRootBindings(DocumentHistoricalSchemaRows.Snapshot snapshot,
            DocumentHistoricalSchemaBinding binding, java.util.List<DocumentHistoricalReadPlan.Entry> selected,
            PayloadBudget budget, RepositoryReadControl control) {
        var entries = new HashMap<Integer, DocumentHistoricalReadPlan.Entry>();
        selected.forEach(entry -> entries.put(entry.revisionOrdinal(), entry));
        for (var root : snapshot.roots()) {
            control.check();
            if (root.ordinal() < 0 || root.ordinal() >= binding.member().getPartsCount())
                throw DocumentHistoricalSchemaRows.invalid("Historical root ordinal is outside source member");
            var part = binding.member().getParts(root.ordinal());
            long size; String hash;
            switch (part.getContentCase()) {
                case UPLOAD -> { size = part.getUpload().getSizeBytes(); hash = part.getUpload().getSha256(); }
                case REUSE -> { size = part.getReuse().getObject().getSizeBytes(); hash = part.getReuse().getObject().getSha256(); }
                case HISTORICAL_REUSE -> { size = part.getHistoricalReuse().getObject().getSizeBytes(); hash = part.getHistoricalReuse().getObject().getSha256(); }
                default -> throw DocumentHistoricalSchemaRows.invalid("Historical root has no source payload");
            }
            if (size != root.fragmentSize() || !hash.equals(root.fragmentSha()))
                throw DocumentHistoricalSchemaRows.invalid("Historical root differs from source payload identity");
            var selectedEntry = entries.get(root.ordinal());
            if (selectedEntry != null && (selectedEntry.part().part().size() != size
                    || !selectedEntry.part().part().sha256().equals(hash)))
                throw DocumentHistoricalSchemaRows.invalid("Historical root differs from selected physical identity");
            try {
                var evidence = DocumentSchemaAdmission.decodeRootEvidence(root.ordinal(), root.evidence(),
                        bytes -> { var lease = reserve(budget, bytes); return lease::close; }, control::check);
                if (!root.locatorSha().equals(evidence.locatorSha256()))
                    throw DocumentHistoricalSchemaRows.invalid("Historical root locator differs from evidence");
            } catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
                throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Invalid historical root evidence", failure);
            }
        }
    }

    private static PayloadBudget.Lease reserve(PayloadBudget budget, long bytes) {
        try { return budget.reserve(bytes); }
        catch (PayloadBudget.CapacityExceededException failure) {
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,
                    "Historical schema resolution capacity exhausted", failure);
        }
    }
}
