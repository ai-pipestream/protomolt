package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy;
import ai.protomolt.proto.repo.admission.DocumentSchemaAssessment;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentPublicationMember;
import ai.protomolt.proto.repo.v1.PublicationHistoricalReuse;
import com.google.protobuf.ByteString;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Private current-policy assessment of one member from one pinned typed historical source.
 * No destination authorization, active-policy fence, reference commit or public restore API.
 * Input buffers must remain stable during capture. The result owns reserved private copies;
 * the host separately bounds parsed heap and drains provider work before releasing its batch.
 */
final class DocumentHistoricalRestoreAssessment implements AutoCloseable {
    private final DocumentReadLedger.PinnedHistory history;
    private final DocumentReadLedger.PinnedRead<DocumentHistoricalReadPlan>.Use pin;
    private DocumentSchemaAssessment assessment;
    private final PayloadBudget.Lease fragmentsLease;

    private DocumentHistoricalRestoreAssessment(DocumentReadLedger.PinnedHistory history,
            DocumentReadLedger.PinnedRead<DocumentHistoricalReadPlan>.Use pin, DocumentSchemaAssessment assessment,
            PayloadBudget.Lease fragmentsLease) {
        this.history = history; this.pin = pin; this.assessment = assessment; this.fragmentsLease = fragmentsLease;
    }

    static DocumentHistoricalRestoreAssessment assess(DocumentReadLedger.PinnedHistory history, DocumentPublicationMember member,
            DocumentAdmissionPolicy policy, ByteString commandSha256, Map<Integer, ByteString> fragments,
            Instant evaluatedAt, PayloadBudget budget, RepositoryReadControl control) {
        var pin = history.use();
        DocumentSchemaAssessment assessment = null;
        PayloadBudget.Lease fragmentsLease = null;
        boolean transferred = false;
        try {
            control.check();
            history.authorizeDelivery(control);
            var plan = pin.plan();
            if (member.getSerializedSize() > DocumentPublicationCommand.MAX_COMMAND_BYTES
                    || member.getPartsCount() > DocumentPublicationCommand.MAX_PARTS
                    || !member.getDestination().getAddress().getAccountId().equals(plan.address().getAccountId()))
                throw new IllegalArgumentException("Restore member scope or size differs from historical source");
            var selectors = new ArrayList<PublicationHistoricalReuse>();
            var mapping = new HashMap<Integer, Integer>();
            for (int ordinal = 0; ordinal < member.getPartsCount(); ordinal++) {
                control.check();
                var part = member.getParts(ordinal);
                if (part.hasEmpty()) continue;
                if (!part.hasHistoricalReuse())
                    throw new UnsupportedOperationException("Historical assessment requires historical or empty parts");
                var selector = part.getHistoricalReuse();
                if (!part.getSlot().equals(selector.getSourceSlot()))
                    throw new IllegalArgumentException("Restore destination slot differs from historical selection");
                selectors.add(selector);
                mapping.put(ordinal, selector.getRevisionOrdinal());
            }
            var selected = history.selectRetained(pin, selectors, control);
            if (!mapping.keySet().equals(fragments.keySet()))
                throw new IllegalArgumentException("Restore fragments differ from selected ordinals");
            long total = 0;
            for (var ordinal : mapping.keySet()) {
                control.check();
                var bytes = java.util.Objects.requireNonNull(fragments.get(ordinal));
                if (bytes.size() != member.getParts(ordinal).getHistoricalReuse().getObject().getSizeBytes()
                        || bytes.size() > policy.limits().maxFragmentBytes() - total)
                    throw new IllegalArgumentException("Restore fragment size exceeds declaration or current policy");
                total += bytes.size();
            }
            fragmentsLease = reserve(budget, total);
            var ownedFragments = new HashMap<Integer, ByteString>();
            for (var ordinal : mapping.keySet()) {
                control.check();
                var copy = com.google.protobuf.UnsafeByteOperations.unsafeWrap(fragments.get(ordinal).toByteArray());
                if (!DocumentCommandContent.sha256(copy, control::check).equals(
                        member.getParts(ordinal).getHistoricalReuse().getObject().getSha256()))
                    throw new IllegalArgumentException("Restore fragment hash differs from selected object");
                ownedFragments.put(ordinal, copy);
            }
            try (var schemas = DocumentHistoricalSchemaResolution.open(history, pin, mapping, selected, budget, control)) {
                var retained = schemas.resolution();
                assessment = policy.assess(commandSha256, member, Map.copyOf(ownedFragments), retained.container(), retained,
                        bytes -> { var lease = reserve(budget, bytes); return lease::close; }, evaluatedAt, control::check);
                retained.requireComplete(assessment.view());
            } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
                throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Invalid retained schema encoding", failure);
            }
            history.authorizeDelivery(control);
            pin.plan();
            var result = new DocumentHistoricalRestoreAssessment(history, pin, assessment, fragmentsLease);
            transferred = true;
            return result;
        } catch (RuntimeException failure) {
            // Recheck control and current access before exposing either results or detailed failures.
            control.check();
            history.authorizeDelivery(control);
            throw failure;
        } finally {
            if (!transferred) {
                if (assessment != null) assessment.close();
                if (fragmentsLease != null) fragmentsLease.close();
                pin.close();
            }
        }
    }

    /** Borrowed view; retain this owner through consumption. Every new exposure rechecks source READ. */
    synchronized DocumentSchemaAssessment.View view(RepositoryReadControl control) {
        if (assessment == null) throw new IllegalStateException("Historical restore assessment is closed");
        pin.plan();
        history.authorizeDelivery(control);
        pin.plan();
        return assessment.view();
    }

    @Override public synchronized void close() {
        if (assessment == null) return;
        try { assessment.close(); }
        finally { assessment = null; fragmentsLease.close(); pin.close(); }
    }

    private static PayloadBudget.Lease reserve(PayloadBudget budget, long bytes) {
        try { return budget.reserve(bytes); }
        catch (PayloadBudget.CapacityExceededException failure) {
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,
                    "Historical restore assessment capacity exhausted", failure);
        }
    }
}
