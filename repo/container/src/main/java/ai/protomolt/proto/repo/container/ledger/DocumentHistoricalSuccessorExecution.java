package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.util.*;

/** Private attachment from a live local activation attempt, never from a cold receipt alone. */
final class DocumentHistoricalSuccessorExecution {
    private DocumentHistoricalSuccessorExecution() {}

    static DocumentHistoricalExecution open(Tx tx, PayloadBudget budget, RepositorySuccessorInstall.Plan plan,
            DocumentPublicationPreparationRecord retention, DocumentHistoricalAssessmentSources sources,
            DocumentPreparationCaptureDrain.Capture capture, DocumentHistoricalAssessmentSources.Work accepted,
            RepositoryCaller caller, DriveLedger drives, RepositoryReadControl control,
            DocumentPublicationScopeCalls.Call scope) {
        DocumentHistoricalAssessmentSources.Work work = null;
        PayloadBudget.Lease retained = null;
        DocumentHistoricalExecution execution = null;
        try {
            work = accepted.fork(); work.histories(sources); work.requireCaller(caller); work.authorize(control);
            var next = plan.next();

            var references = work.references(next.command(), control::check);
            var pins = DocumentPreparationSourcePins.prepare(next.command(), references, control::check);
            var prepared = DocumentOperationUploadAdmission.prepareHistorical(next.command(), next.placements(),
                    next.seeds().attempts(), next.lease(), next.seeds().uploadTokens(), references, control::check);
            final DocumentHistoricalSuccessorBinding binding;
            final byte[] digest;
            try (var scratch = budget.reserve(3L * DocumentPublicationPreparationCodec.MAX_BYTES)) {
                binding = new DocumentHistoricalSuccessorBinding(plan, retention, capture.identity(), pins);
                digest = DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(next));
                retained = budget.reserve(binding.retainedBytes() + DocumentPreparationSourcePins.MAX_BYTES
                        + DocumentPublicationModesJournal.MAX_BYTES);
            }
            var owner = tx.inTransaction(em -> {
                var claim = RepositoryExecutionClaimLedger.lockLive(em, next.key(), next.command().sha256(),
                        plan.reservation().predecessor().epoch()+1, plan.reservation().successorToken());
                return RepositoryOperationLedger.lockLiveOwner(em, next.key(), next.predecessorGeneration()+1,
                        next.seeds().ownerNonce(), Optional.of(claim));
            });
            execution = new DocumentHistoricalExecution(work, retained, owner, prepared, plan.modes(), pins,
                    scope, tx, budget, drives, next, capture.identity(), digest, binding);
            execution.validateAttachment(caller, control);
            return execution;
        } catch (RuntimeException | Error failure) {
            try {
                if (execution != null) execution.close();
                else {
                    var cleanup = new ArrayList<AutoCloseable>();
                    if (work != null) cleanup.add(work);
                    if (retained != null) cleanup.add(retained);
                    cleanup.add(scope);
                    DocumentHistoricalAssessmentSources.closeAll(cleanup);
                }
            } catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
            throw failure;
        }
    }
}
