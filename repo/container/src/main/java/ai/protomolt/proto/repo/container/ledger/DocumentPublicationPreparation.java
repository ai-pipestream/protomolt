package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Composes staged uploads and protected retained reads into an owned admission candidate. */
final class DocumentPublicationPreparation {
    /** Carries a checked parser failure across the synchronous upload callback only. */
    private static final class ParseFailure extends RuntimeException {
        private ParseFailure(InvalidProtocolBufferException cause) { super(cause); }
    }
    record Admission(DocumentSchemaPolicies.Selection policy,
            Map<String, DocumentPublicationCandidate.Mode> modes,
            Optional<DocumentSchemaAdmission.Definition> container,
            DocumentPublicationCandidate.Resolver resolver, DocumentRevisionAssembly.Limits opaqueLimits) {
        Admission {
            Objects.requireNonNull(policy); modes = Map.copyOf(modes);
            Objects.requireNonNull(container); Objects.requireNonNull(resolver); Objects.requireNonNull(opaqueLimits);
        }
    }

    /** Borrow candidate and selections until close; commit still rechecks all durable fences. */
    static final class Prepared implements AutoCloseable {
        private DocumentPublicationCandidate candidate;
        private Map<String, DocumentSelectedAttemptLedger.Selected> selections;
        private Prepared(DocumentPublicationCandidate candidate,
                Map<String, DocumentSelectedAttemptLedger.Selected> selections) {
            this.candidate = candidate; this.selections = Map.copyOf(selections);
        }
        synchronized DocumentPublicationCandidate candidate() { requireOpen(); return candidate; }
        synchronized Map<String, DocumentSelectedAttemptLedger.Selected> selections() { requireOpen(); return selections; }
        private void requireOpen() {
            if (candidate == null) throw new IllegalStateException("Publication preparation is closed");
        }
        @Override public synchronized void close() {
            if (candidate == null) return;
            candidate.close(); candidate = null; selections = Map.of();
        }
    }

    private final DocumentUploadCoordinator uploads;
    private final DocumentRetainedReader retained;
    private final PayloadBudget budget;

    DocumentPublicationPreparation(DocumentUploadCoordinator uploads, DocumentRetainedReader retained, PayloadBudget budget) {
        this.uploads = Objects.requireNonNull(uploads);
        this.retained = Objects.requireNonNull(retained); this.budget = Objects.requireNonNull(budget);
    }

    /**
     * The host owns the pinned plan, reader and coordinator. It must close/drain the
     * plan and release SQL pins, retaining failed cleanup handles for recovery.
     * This method neither commits nor releases pins. Its result owns copied bytes
     * through schema staging and commit; source batches need only outlive that copy.
     */
    Prepared prepare(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentOperationUploadAdmission.Prepared plan, Map<DocumentUploadPayloads.Key, PartObject> bodies,
            Map<String, String> attributes, DocumentReadLedger.PinnedPlan pinned, Admission admission,
            RepositoryReadControl control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(admission); Objects.requireNonNull(pinned); Objects.requireNonNull(control).check();
        try {
            return uploads.stageAndPrepareOwned(caller, owner, plan, bodies, attributes, control::check,
                (staged, view, active) -> {
                    var readControl = new RepositoryReadControl() {
                        @Override public long remainingNanos() { return control.remainingNanos(); }
                        @Override public boolean isCancelled() { return control.isCancelled(); }
                        @Override public void check() { control.check(); active.run(); }
                    };
                    var selections = new HashMap<String, DocumentSelectedAttemptLedger.Selected>();
                    for (var member : staged.members()) {
                        if (selections.put(member.selection().member(), member.selection()) != null)
                            throw new IllegalArgumentException("Duplicate staged publication member");
                    }
                    try (var inputs = DocumentPublicationInputs.capture(plan.plan().command(), owner, view, pinned,
                            retained, readControl)) {
                        var candidate = DocumentPublicationCandidate.prepare(plan.plan().command(), admission.policy(),
                                admission.modes(), inputs.fragments(), admission.container(), admission.resolver(),
                                budget, admission.opaqueLimits(), readControl::check);
                        try { return new Prepared(candidate, selections); }
                        catch (RuntimeException | Error failure) { candidate.close(); throw failure; }
                    } catch (InvalidProtocolBufferException failure) {
                        throw new ParseFailure(failure);
                    }
                });
        } catch (ParseFailure failure) {
            // Do not label all malformed data as caller input: a retained fragment
            // may be involved. Preserve the parser failure for the operation boundary.
            var cause = (InvalidProtocolBufferException) failure.getCause();
            for (var cleanup : failure.getSuppressed()) cause.addSuppressed(cleanup);
            throw cause;
        }
    }
}
