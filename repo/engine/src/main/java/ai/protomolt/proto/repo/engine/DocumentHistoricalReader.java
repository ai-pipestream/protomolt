package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.container.ledger.DocumentHistoricalValidation;
import ai.protomolt.proto.repo.container.ledger.DocumentReadLedger;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.Objects;

/** Retained-schema validation over real protected provider reads; owns neither reader nor budget. */
public final class DocumentHistoricalReader {
    private final DocumentPartReader parts;
    private final PayloadBudget budget;
    /** Hosts may share this budget with the part reader; no capacity wait occurs while holding a lease. */
    public DocumentHistoricalReader(DocumentPartReader parts, PayloadBudget budget) {
        this.parts = Objects.requireNonNull(parts); this.budget = Objects.requireNonNull(budget);
    }

    /**
     * Validates using the archived contract and definitions with the current runtime.
     * Opaque revisions or missing retained definitions are explicit failures, never
     * a downgrade to unvalidated content. Use the raw reader for opaque preservation.
     * The host still closes/drains/releases the supplied history handle.
     */
    public DocumentHistoricalRead readValidated(DocumentReadLedger.PinnedHistory history, RepositoryReadControl control) {
        Objects.requireNonNull(history); Objects.requireNonNull(control);
        DocumentReadBatch raw = null;
        PayloadBudget.Lease copies = null;
        DocumentHistoricalValidation validation = null;
        boolean handedOff = false;
        try (var use = history.use()) {
            var plan = use.plan();
            raw = parts.readHistorical(history, control);
            var fragments = raw.parts();
            if (fragments.size() != plan.entries().size())
                throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Historical fragment count differs from plan");
            long size = 0;
            for (var fragment : fragments) size = Math.addExact(size, fragment.bytes().length);
            try { copies = budget.reserve(Math.multiplyExact(size, 2)); }
            catch (PayloadBudget.CapacityExceededException exhausted) {
                throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Historical fragment copy capacity exhausted", exhausted);
            }
            var encoded = new HashMap<Integer, ByteString>();
            for (int index = 0; index < fragments.size(); index++) {
                control.check();
                encoded.put(plan.entries().get(index).revisionOrdinal(), ByteString.copyFrom(fragments.get(index).bytes()));
            }
            validation = history.validateFragments(encoded, budget, control);
            var result = new DocumentHistoricalRead(raw, validation, copies, plan.revision(), plan.address(),
                    plan.manifest(), plan.publicationRevision(), plan.metadata(), history);
            handedOff = true;
            return result;
        } catch (java.util.concurrent.CancellationException cancelled) {
            throw new RepositoryException(RepositoryException.Code.CANCELLED, "Historical validation cancelled");
        } catch (RepositoryException failure) {
            if (failure.code() == RepositoryException.Code.CANCELLED || failure.code() == RepositoryException.Code.DEADLINE_EXCEEDED)
                throw new RepositoryException(failure.code(), "Historical validation cancelled or expired");
            throw failure;
        } finally {
            if (!handedOff) {
                if (validation != null) validation.close();
                if (copies != null) copies.close();
                if (raw != null) raw.close();
            }
        }
    }
}
