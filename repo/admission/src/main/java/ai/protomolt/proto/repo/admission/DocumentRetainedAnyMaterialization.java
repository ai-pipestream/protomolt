package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v1.RepositoryAnyResolution;
import com.google.protobuf.Any;

/** One exact retained boundary; callers authenticate location and own all inputs/results. */
final class DocumentRetainedAnyMaterialization {
    private DocumentRetainedAnyMaterialization() {}
    static DocumentAnyMaterialization.View read(DocumentSchemaAdmission.Selection occurrence, Any original,
            RepositoryAnyResolution recordedBoundary, DocumentAnyMaterialization.Mode mode,
            DocumentSchemaAdmission.Reference reference, DocumentRetainedSchemaAssets retained,
            DocumentAnyMaterialization.Limits limits, Runnable active) {
        Runnable guarded = () -> {
            try { active.run(); }
            catch (RuntimeException failure) { throw new ControlFailure(failure); }
        };
        final DocumentAnyMaterialization.View view;
        try {
            view = DocumentAnyMaterialization.read(occurrence, original, mode, limits, ignored -> {
                if (!reference.typeUrl().equals(recordedBoundary.getTypeUrl())
                        || !reference.descriptorSha256().equals(recordedBoundary.getResolved().getArtifactSha256()))
                    throw lost("retained Any schema association differs from recorded boundary");
                // Full association verification includes canonical metadata and any retained source.
                // Absence/corruption is DataLoss. There is deliberately no discovery resolver.
                final DocumentSchemaAssetBinding bound;
                try { bound = retained.resolve(reference.internal(), guarded); }
                catch (ControlFailure failure) { throw failure; }
                catch (RuntimeException failure) { throw new ResolutionFailure(failure); }
                if (!bound.schema().condition().equals(recordedBoundary.getResolved().getSchema()))
                    throw lost("retained Any schema condition differs from recorded boundary");
                return new DocumentAnyMaterialization.Resolved(bound);
            }, guarded);
        } catch (ControlFailure failure) {
            throw failure.original;
        } catch (ResolutionFailure failure) {
            throw failure.original;
        } catch (DocumentAnyMaterialization.IdentityMismatch failure) {
            throw new DocumentRetainedSchemaAssets.DataLoss("retained Any envelope differs from recorded value identity", failure);
        }
        active.run();
        if (view instanceof DocumentAnyMaterialization.Failed failed
                && (failed.failure().reason() == DocumentAnyMaterialization.Reason.MALFORMED_PAYLOAD
                    || failed.failure().reason() == DocumentAnyMaterialization.Reason.CORRUPT_DEFINITION)) {
            throw new DocumentRetainedSchemaAssets.DataLoss("retained typed Any cannot be decoded with its recorded definition",
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
