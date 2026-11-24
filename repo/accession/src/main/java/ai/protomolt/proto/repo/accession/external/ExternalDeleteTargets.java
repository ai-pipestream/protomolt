package ai.protomolt.proto.repo.accession.external;

import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.findOperation;

import ai.protomolt.proto.repo.external.v1.ExternalLink;
import ai.protomolt.proto.repo.external.v1.ExternalProjectionLifecycle;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationAction;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationIntent;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationLedger;
import ai.protomolt.proto.repo.external.v1.ExternalPublicationOperationRecord;
import ai.protomolt.proto.repo.external.v1.ExternalSystemBinding;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Resolves the external object and binding configuration revision a tombstone or purge deletion
 * operates on from the recorded ledger state, refusing unrecorded objects and targets that have
 * since been republished under a newer source revision.
 */
final class ExternalDeleteTargets {

    /** For a purge: every source revision is covered. */
    static final long ALL_REVISIONS = 0L;

    private ExternalDeleteTargets() {
    }

    record ResolvedDeleteTarget(ExternalSystemBinding binding, String externalObjectId) {
    }

    /**
     * When a request carries an operation ID the ledger already knows and names no target of
     * its own, the target is the one that operation recorded. Resolving it afresh could pick a
     * different object once the first one is gone, and turn a replay into a conflict.
     */
    static Optional<ResolvedDeleteTarget> replayedTarget(
            Optional<ExternalPublicationLedger> ledgerOpt,
            String operationId,
            String explicitExternalObjectId,
            long explicitBindingRevision) {
        boolean namesObject = explicitExternalObjectId != null && !explicitExternalObjectId.isEmpty();
        if (ledgerOpt.isEmpty() || operationId == null || operationId.isEmpty()) {
            return Optional.empty();
        }
        Optional<ExternalPublicationOperationRecord> recorded =
                findOperation(ledgerOpt.get(), operationId)
                        .filter(operation -> operation.getIntent().getAction()
                                == ExternalPublicationAction.EXTERNAL_PUBLICATION_ACTION_DELETE);
        if (recorded.isPresent() && namesObject
                && !recorded.get().getIntent().getExternalObjectId().equals(explicitExternalObjectId)) {
            throw Status.ALREADY_EXISTS
                    .withDescription("Operation '" + operationId + "' deleted another external object")
                    .asRuntimeException();
        }
        if (recorded.isPresent() && explicitBindingRevision > 0L
                && recorded.get().getIntent().getBindingRevision() != explicitBindingRevision) {
            throw Status.ALREADY_EXISTS
                    .withDescription("Operation '" + operationId + "' deleted under binding revision "
                            + recorded.get().getIntent().getBindingRevision() + ", not "
                            + explicitBindingRevision)
                    .asRuntimeException();
        }
        return recorded.map(operation -> new ResolvedDeleteTarget(
                operation.getBindingSnapshot(),
                operation.getIntent().getExternalObjectId()));
    }

    static ResolvedDeleteTarget resolveDeleteTarget(
            String bindingId,
            long explicitBindingRevision,
            String explicitExternalObjectId,
            long coveredUpToSourceRevision,
            Optional<ExternalPublicationLedger> ledgerOpt) {
        Optional<ResolvedDeleteTarget> recorded = recordedTarget(
                ledgerOpt, explicitBindingRevision, explicitExternalObjectId,
                coveredUpToSourceRevision);
        if (recorded.isPresent()) {
            return recorded.get();
        }
        if (explicitExternalObjectId == null || explicitExternalObjectId.isEmpty()) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("No externalObjectId supplied and no prior ExternalLink recorded")
                    .asRuntimeException();
        }
        throw Status.FAILED_PRECONDITION
                .withDescription("External object '" + explicitExternalObjectId
                        + "' is not recorded for this source under binding '" + bindingId + "'")
                .asRuntimeException();
    }

    /**
     * The deletion target the ledger itself records: an object together with the binding
     * snapshot it was published under, taken from one link so the two can never be mixed. An
     * object ID alone means nothing outside the dataset and namespace of its binding revision.
     *
     * <p>With no object named, the target is the current link, else the newest historical link
     * that is not yet deleted, else the newest historical link, else (with no link at all) the
     * newest publication still pending. With an object named, it is that object's link,
     * restricted to {@code explicitBindingRevision} when one is given.
     *
     * <p>Only projections published up to {@code coveredUpToSourceRevision} are considered, so
     * a tombstone handled late never reaches a publication made after the source was restored.
     *
     * @return the recorded target, or empty when the named object has no link in the ledger
     * @throws io.grpc.StatusRuntimeException FAILED_PRECONDITION when no object is named and
     *         {@code explicitBindingRevision} is not the revision the default link used
     */
    static Optional<ResolvedDeleteTarget> recordedTarget(
            Optional<ExternalPublicationLedger> ledgerOpt,
            long explicitBindingRevision,
            String explicitExternalObjectId,
            long coveredUpToSourceRevision) {
        if (ledgerOpt.isEmpty()) {
            return Optional.empty();
        }
        List<ExternalLink> candidates =
                linksInDeletionOrder(ledgerOpt.get(), coveredUpToSourceRevision);
        if (explicitExternalObjectId != null && !explicitExternalObjectId.isEmpty()) {
            Optional<ResolvedDeleteTarget> covered = recordedSnapshotFor(
                    ledgerOpt.get(), candidates, explicitBindingRevision, explicitExternalObjectId,
                    coveredUpToSourceRevision);
            if (covered.isEmpty() && coveredUpToSourceRevision != ALL_REVISIONS
                    && recordedSnapshotFor(ledgerOpt.get(),
                            linksInDeletionOrder(ledgerOpt.get(), ALL_REVISIONS),
                            explicitBindingRevision, explicitExternalObjectId, ALL_REVISIONS)
                    .isPresent()) {
                throw Status.FAILED_PRECONDITION
                        .withDescription("External object '" + explicitExternalObjectId
                                + "' was published after source revision "
                                + coveredUpToSourceRevision + "; this tombstone does not cover it")
                        .asRuntimeException();
            }
            covered.ifPresent(target -> requireNotRepublished(
                    ledgerOpt.get(), target, coveredUpToSourceRevision));
            return covered;
        }
        boolean onlyDeletedHistory = candidates.stream().allMatch(link ->
                link.getProjectionLifecycle()
                        == ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED);
        if (onlyDeletedHistory) {
            Optional<ResolvedDeleteTarget> pending = unsettledPublication(
                    ledgerOpt.get(), explicitBindingRevision, coveredUpToSourceRevision);
            if (pending.isPresent() || candidates.isEmpty()) {
                return pending;
            }
        }
        ExternalLink chosen = candidates.getFirst();
        if (explicitBindingRevision > 0L && chosen.getBindingRevision() != explicitBindingRevision) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("No externalObjectId supplied, and the recorded object '"
                            + chosen.getExternalObjectId() + "' was published under binding revision "
                            + chosen.getBindingRevision() + ", not " + explicitBindingRevision)
                    .asRuntimeException();
        }
        ResolvedDeleteTarget target = new ResolvedDeleteTarget(
                chosen.getBindingSnapshot(), chosen.getExternalObjectId());
        requireNotRepublished(ledgerOpt.get(), target, coveredUpToSourceRevision);
        return Optional.of(target);
    }

    /**
     * Whether two binding revisions address the same place in the external system. A binding
     * can be revised without moving (new labels, rotated credentials), so the revision number
     * does not say. An object ID means the same object exactly when provider, dataset,
     * namespace and endpoint are the same.
     */
    static boolean sameExternalTarget(ExternalSystemBinding left, ExternalSystemBinding right) {
        return left.getProvider().equals(right.getProvider())
                && left.getDataset().equals(right.getDataset())
                && left.getNamespace().equals(right.getNamespace())
                && left.getEndpointSummary().equals(right.getEndpointSummary());
    }

    /**
     * Refuses a tombstone deletion whose object has been published again, under the same ID in
     * the same external target, for a source revision after the tombstone. The provider knows
     * the object by ID alone, so deleting it then would delete the republished one.
     */
    static void requireNotRepublished(
            ExternalPublicationLedger ledger,
            ResolvedDeleteTarget target,
            long coveredUpToSourceRevision) {
        if (coveredUpToSourceRevision == ALL_REVISIONS) {
            return;
        }
        List<ExternalLink> all = new ArrayList<>(ledger.getHistoricalLinksList());
        if (ledger.hasCurrentLink()) {
            all.add(ledger.getCurrentLink());
        }
        long republishedAt = 0L;
        for (ExternalLink link : all) {
            if (link.getExternalObjectId().equals(target.externalObjectId())
                    && sameExternalTarget(link.getBindingSnapshot(), target.binding())
                    && link.getSourceRevision() > coveredUpToSourceRevision) {
                republishedAt = Math.max(republishedAt, link.getSourceRevision());
            }
        }
        for (int i = 0; i < ledger.getOperationsCount(); i++) {
            ExternalPublicationOperationRecord operation = ledger.getOperations(i);
            ExternalPublicationIntent intent = operation.getIntent();
            if (ExternalLedgerStore.publicationMayHoldObject(ledger, i)
                    && intent.getExternalObjectId().equals(target.externalObjectId())
                    && sameExternalTarget(operation.getBindingSnapshot(), target.binding())
                    && intent.getSourceRevision() > coveredUpToSourceRevision) {
                republishedAt = Math.max(republishedAt, intent.getSourceRevision());
            }
        }
        if (republishedAt > 0L) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("External object '" + target.externalObjectId()
                            + "' was republished for source revision " + republishedAt
                            + ", after tombstone revision " + coveredUpToSourceRevision
                            + "; deleting it would delete that publication")
                    .asRuntimeException();
        }
    }

    private static Optional<ResolvedDeleteTarget> recordedSnapshotFor(
            ExternalPublicationLedger ledger,
            List<ExternalLink> linksInOrder,
            long explicitBindingRevision,
            String externalObjectId,
            long coveredUpToSourceRevision) {
        Map<Long, ExternalSystemBinding> byRevision = new LinkedHashMap<>();
        for (ExternalLink link : linksInOrder) {
            if (link.getExternalObjectId().equals(externalObjectId)) {
                byRevision.putIfAbsent(link.getBindingRevision(), link.getBindingSnapshot());
            }
        }
        for (int i = ledger.getOperationsCount() - 1; i >= 0; i--) {
            ExternalPublicationOperationRecord operation = ledger.getOperations(i);
            if (ExternalLedgerStore.publicationMayHoldObject(ledger, i)
                    && covered(operation.getIntent().getSourceRevision(), coveredUpToSourceRevision)
                    && operation.getIntent().getExternalObjectId().equals(externalObjectId)) {
                byRevision.putIfAbsent(
                        operation.getIntent().getBindingRevision(), operation.getBindingSnapshot());
            }
        }
        if (explicitBindingRevision > 0L) {
            return Optional.ofNullable(byRevision.get(explicitBindingRevision))
                    .map(binding -> new ResolvedDeleteTarget(binding, externalObjectId));
        }
        if (byRevision.size() > 1) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("External object '" + externalObjectId
                            + "' is recorded under binding revisions " + byRevision.keySet()
                            + "; name the binding revision to delete it from")
                    .asRuntimeException();
        }
        return byRevision.values().stream().findFirst()
                .map(binding -> new ResolvedDeleteTarget(binding, externalObjectId));
    }

    private static Optional<ResolvedDeleteTarget> unsettledPublication(
            ExternalPublicationLedger ledger,
            long explicitBindingRevision,
            long coveredUpToSourceRevision) {
        for (int i = ledger.getOperationsCount() - 1; i >= 0; i--) {
            ExternalPublicationOperationRecord operation = ledger.getOperations(i);
            if (!ExternalLedgerStore.publicationMayHoldObject(ledger, i)) {
                continue;
            }
            if (explicitBindingRevision > 0L
                    && operation.getIntent().getBindingRevision() != explicitBindingRevision) {
                continue;
            }
            if (!covered(operation.getIntent().getSourceRevision(), coveredUpToSourceRevision)) {
                continue;
            }
            return Optional.of(new ResolvedDeleteTarget(
                    operation.getBindingSnapshot(), operation.getIntent().getExternalObjectId()));
        }
        return Optional.empty();
    }

    private static List<ExternalLink> linksInDeletionOrder(
            ExternalPublicationLedger ledger, long coveredUpToSourceRevision) {
        List<ExternalLink> ordered = new ArrayList<>(ledger.getHistoricalLinksCount() + 1);
        if (ledger.hasCurrentLink()
                && covered(ledger.getCurrentLink().getSourceRevision(), coveredUpToSourceRevision)) {
            ordered.add(ledger.getCurrentLink());
        }
        List<ExternalLink> deleted = new ArrayList<>();
        for (int i = ledger.getHistoricalLinksCount() - 1; i >= 0; i--) {
            ExternalLink historical = ledger.getHistoricalLinks(i);
            if (!covered(historical.getSourceRevision(), coveredUpToSourceRevision)) {
                continue;
            }
            if (historical.getProjectionLifecycle()
                    == ExternalProjectionLifecycle.EXTERNAL_PROJECTION_LIFECYCLE_DELETED) {
                deleted.add(historical);
            } else {
                ordered.add(historical);
            }
        }
        ordered.addAll(deleted);
        return ordered;
    }

    static boolean covered(long sourceRevision, long coveredUpToSourceRevision) {
        return coveredUpToSourceRevision == ALL_REVISIONS
                || sourceRevision <= coveredUpToSourceRevision;
    }
}
