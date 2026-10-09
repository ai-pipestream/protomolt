package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Instant;
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

    /** Owns the complete assessment, including invalid values; grants no publication or rejection authority. */
    static final class Assessed implements AutoCloseable {
        private DocumentPublicationAssessment assessment;
        private Map<String, DocumentSelectedAttemptLedger.Selected> selections;
        private Assessed(DocumentPublicationAssessment assessment,
                Map<String, DocumentSelectedAttemptLedger.Selected> selections) {
            this.assessment = assessment; this.selections = Map.copyOf(selections);
        }
        synchronized DocumentPublicationAssessment assessment() { requireOpen(); return assessment; }
        synchronized Map<String, DocumentSelectedAttemptLedger.Selected> selections() { requireOpen(); return selections; }
        /** Consume only after promotion succeeds; failures in checking leave the assessment available. */
        synchronized Prepared promoteAccepted(Runnable control) throws InvalidProtocolBufferException {
            requireOpen();
            var candidate = assessment.promoteAccepted(control);
            try { return new Prepared(candidate, selections); }
            catch (RuntimeException | Error failure) { candidate.close(); throw failure; }
            finally { assessment = null; selections = Map.of(); }
        }
        private void requireOpen() {
            if (assessment == null) throw new IllegalStateException("Publication assessment preparation is closed");
        }
        @Override public synchronized void close() {
            if (assessment == null) return;
            assessment.close(); assessment = null; selections = Map.of();
        }
    }

    @FunctionalInterface private interface OwnedPreparation<T extends AutoCloseable> {
        T create(DocumentPublicationInputs inputs, Map<String, DocumentSelectedAttemptLedger.Selected> selections,
                RepositoryReadControl control) throws InvalidProtocolBufferException;
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
        Objects.requireNonNull(admission);
        return prepareOwned(caller, owner, plan, bodies, attributes, pinned, control, (inputs, selections, active) -> {
            var candidate = DocumentPublicationCandidate.prepare(plan.plan().command(), admission.policy(),
                    admission.modes(), inputs.fragments(), admission.container(), admission.resolver(),
                    budget, admission.opaqueLimits(), active::check);
            try { return new Prepared(candidate, selections); }
            catch (RuntimeException | Error failure) { candidate.close(); throw failure; }
        });
    }

    /**
     * Assess once while upload and retained-source inputs are still owned. The
     * returned scope owns copied fragments and schema evidence after those inputs
     * close. The caller must separately stage, replay and fence any terminal
     * decision; operational and structural failures propagate without a verdict.
     */
    Assessed assess(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentOperationUploadAdmission.Prepared plan, Map<DocumentUploadPayloads.Key, PartObject> bodies,
            Map<String, String> attributes, DocumentReadLedger.PinnedPlan pinned, Admission admission,
            Instant evaluatedAt, RepositoryReadControl control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(admission); Objects.requireNonNull(evaluatedAt);
        return prepareOwned(caller, owner, plan, bodies, attributes, pinned, control, (inputs, selections, active) -> {
            var assessment = DocumentPublicationAssessment.prepare(plan.plan().command(), admission.policy(),
                    admission.modes(), inputs.fragments(), admission.container(), admission.resolver(),
                    budget, admission.opaqueLimits(), evaluatedAt, active::check);
            try { return new Assessed(assessment, selections); }
            catch (RuntimeException | Error failure) { assessment.close(); throw failure; }
        });
    }

    private <T extends AutoCloseable> T prepareOwned(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentOperationUploadAdmission.Prepared plan, Map<DocumentUploadPayloads.Key, PartObject> bodies,
            Map<String, String> attributes, DocumentReadLedger.PinnedPlan pinned, RepositoryReadControl control,
            OwnedPreparation<T> preparation) throws InvalidProtocolBufferException {
        Objects.requireNonNull(pinned); Objects.requireNonNull(control).check();
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
                        return preparation.create(inputs, selections, readControl);
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
