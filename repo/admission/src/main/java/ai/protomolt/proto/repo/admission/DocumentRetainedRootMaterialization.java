package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v1.DocumentSchemaRootLocator;
import ai.protomolt.proto.repo.v1.RepositoryAnyResolution;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;

/**
 * Binds one typed retained root to an already inventoried exact fragment. The host
 * authenticates revision/ordinal, canonical evidence and schema associations before
 * entry, owns the inventory/asset/result budgets, and rechecks access at delivery.
 * Malformed parsed evidence violates that precondition; its shape errors are not
 * reclassified here. The host must classify failures of the stored evidence codec.
 * This is not the opaque raw-fragment reader or a nested occurrence path walker.
 */
final class DocumentRetainedRootMaterialization {
    private DocumentRetainedRootMaterialization() {}

    static DocumentAnyMaterialization.View read(int ordinal, DocumentAnyRootInventory.Result inventory,
            DocumentSchemaRootLocator recordedRoot, RepositoryAnyResolution recordedBoundary,
            DocumentAnyMaterialization.Mode mode, DocumentSchemaAdmission.Reference reference,
            DocumentRetainedSchemaAssets retained, DocumentAnyMaterialization.Limits limits, Runnable control) {
        if (ordinal < 0) throw new IllegalArgumentException("negative revision fragment ordinal");
        Objects.requireNonNull(inventory); Objects.requireNonNull(recordedRoot); Objects.requireNonNull(recordedBoundary);
        Objects.requireNonNull(mode); Objects.requireNonNull(reference); Objects.requireNonNull(retained);
        Objects.requireNonNull(limits); Objects.requireNonNull(control);
        Runnable active = () -> {
            if (Thread.currentThread().isInterrupted()) throw new CancellationException("retained root read interrupted");
            control.run();
        };
        active.run();
        // Parsed evidence still needs bounded shape validation. The host verifies stored codec/digest.
        DocumentSchemaEvidenceCodec.measureAndValidate(recordedRoot, active);
        DocumentSchemaEvidenceCodec.measureAndValidate(recordedBoundary, active);
        if (!recordedBoundary.hasResolved()) throw lost("typed retained root has no recorded definition");
        DocumentAnyRootInventory.Root selected = null;
        for (var root : inventory.roots()) {
            active.run();
            // Project only the matching coordinate. Full root projection verifies membership
            // with a list lookup; doing that for every root would make this search quadratic.
            if (!DocumentSchemaOccurrenceProjection.projectSteps(root.access(), java.util.Map.of(), active)
                    .equals(recordedRoot.getAccessList())) continue;
            if (DocumentSchemaRootProjection.project(inventory, root, active).equals(recordedRoot)) {
                if (selected != null) throw lost("retained root matches more than one inventoried location");
                selected = root;
            }
        }
        if (selected == null) throw lost("retained root differs from exact fragment inventory");
        var occurrence = new DocumentSchemaAdmission.Selection(ordinal, recordedRoot, recordedBoundary.getTypeUrl(),
                List.of(), recordedBoundary.getValueSha256(), recordedBoundary.getValueSizeBytes());
        Runnable guarded = () -> {
            try { active.run(); }
            catch (RuntimeException failure) { throw new ControlFailure(failure); }
        };
        final DocumentAnyMaterialization.View view;
        try {
            view = DocumentAnyMaterialization.read(occurrence, selected.envelope(), mode, limits, ignored -> {
                if (!reference.typeUrl().equals(recordedBoundary.getTypeUrl())
                        || !reference.descriptorSha256().equals(recordedBoundary.getResolved().getArtifactSha256()))
                    throw lost("retained root schema association differs from recorded boundary");
                // Full association verification includes canonical metadata and any retained source.
                // Absence/corruption is DataLoss. There is deliberately no discovery resolver.
                final DocumentSchemaAssetBinding bound;
                try { bound = retained.resolve(reference.internal(), guarded); }
                catch (ControlFailure failure) { throw failure; }
                catch (RuntimeException failure) { throw new ResolutionFailure(failure); }
                if (!bound.schema().condition().equals(recordedBoundary.getResolved().getSchema()))
                    throw lost("retained root schema condition differs from recorded boundary");
                return new DocumentAnyMaterialization.Resolved(bound);
            }, guarded);
        } catch (ControlFailure failure) {
            throw failure.original;
        } catch (ResolutionFailure failure) {
            throw failure.original;
        } catch (DocumentAnyMaterialization.IdentityMismatch failure) {
            throw new DocumentRetainedSchemaAssets.DataLoss("retained root envelope differs from recorded value identity", failure);
        }
        active.run();
        if (view instanceof DocumentAnyMaterialization.Failed failed
                && (failed.failure().reason() == DocumentAnyMaterialization.Reason.MALFORMED_PAYLOAD
                    || failed.failure().reason() == DocumentAnyMaterialization.Reason.CORRUPT_DEFINITION)) {
            throw new DocumentRetainedSchemaAssets.DataLoss("retained typed root cannot be decoded with its recorded definition",
                    failed.failure().cause().orElse(null));
        }
        // Preserve and resource refusal do not verify/load required schema assets. They confer no verdict.
        return view;
    }

    private static DocumentRetainedSchemaAssets.DataLoss lost(String message) {
        return new DocumentRetainedSchemaAssets.DataLoss(message);
    }
    private static final class ControlFailure extends RuntimeException {
        private final RuntimeException original;
        private ControlFailure(RuntimeException original) { this.original = original; }
    }
    private static final class ResolutionFailure extends RuntimeException {
        private final RuntimeException original;
        private ResolutionFailure(RuntimeException original) { this.original = original; }
    }
}
