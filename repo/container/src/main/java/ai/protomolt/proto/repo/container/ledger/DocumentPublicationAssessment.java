package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentAdmissionReservations;
import ai.protomolt.proto.repo.admission.DocumentAssessmentManifestCodec;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemaAssessment;
import ai.protomolt.proto.repo.admission.DocumentSchemaAssessmentReplay;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryException;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Owns a complete operation assessment; no independent proof, authorization or terminal decision. */
final class DocumentPublicationAssessment implements AutoCloseable {
    record MemberFailure(String member, DocumentSchemaAssessment.Failure failure) {}
    private final DocumentPublicationFragments fragments;
    private final DocumentSchemaPolicies.Selection policy;
    private final Instant evaluatedAt;
    private final PayloadBudget budget;
    private Map<String, DocumentPublicationCandidate.Mode> modes;
    private Map<String, DocumentSchemaAssessment.View> typed;
    private Map<String, DocumentCommandContent> opaque;
    private Map<String, ByteString> artifacts;
    private final List<DocumentSchemaAssessment> owners;
    private MemberFailure failure;
    private boolean verifying;
    private boolean promoted;

    private DocumentPublicationAssessment(DocumentPublicationFragments fragments, DocumentSchemaPolicies.Selection policy,
            Instant evaluatedAt, Map<String, DocumentPublicationCandidate.Mode> modes,
            Map<String, DocumentSchemaAssessment> typed, Map<String, DocumentCommandContent> opaque,
            Map<String, ByteString> artifacts, MemberFailure failure, PayloadBudget budget) {
        this.fragments = fragments; this.policy = policy; this.evaluatedAt = evaluatedAt;
        this.modes = modes; this.opaque = Map.copyOf(opaque);
        var views = new LinkedHashMap<String, DocumentSchemaAssessment.View>();
        typed.forEach((id, owner) -> views.put(id, owner.view()));
        this.typed = Map.copyOf(views);
        this.owners = new ArrayList<>(typed.values()); this.failure = failure;
        this.artifacts = artifacts;
        this.budget = budget;
    }

    /**
     * Inputs remain stable during capture. The result owns fragment copies and typed
     * schema/evidence assets until close; the host owns resolver and decoded-heap
     * budgets, policy authorization and source pins. All hashes precede resolution;
     * later member failures cannot be hidden by an earlier value violation.
     */
    static DocumentPublicationAssessment prepare(DocumentPublicationCommand command, DocumentSchemaPolicies.Selection policy,
            Map<String, DocumentPublicationCandidate.Mode> modes, Map<String, Map<Integer, ByteString>> supplied,
            Optional<DocumentSchemaAdmission.Definition> container, DocumentPublicationCandidate.Resolver resolver,
            PayloadBudget budget, DocumentRevisionAssembly.Limits opaqueLimits, Instant evaluatedAt, Runnable control)
            throws InvalidProtocolBufferException {
        Objects.requireNonNull(command); Objects.requireNonNull(policy); Objects.requireNonNull(modes);
        command.requireExecutionSupported();
        return prepareInternal(command, policy, modes, supplied, container, resolver, budget, opaqueLimits,
                evaluatedAt, control, null, ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE);
    }

    static Historical prepareHistorical(DocumentPublicationCommand command, DocumentSchemaPolicies.Selection policy,
            Map<String, DocumentPublicationCandidate.Mode> modes, Map<String, Map<Integer, ByteString>> supplied,
            Optional<DocumentSchemaAdmission.Definition> container, DocumentPublicationCandidate.Resolver resolver,
            PayloadBudget budget, DocumentRevisionAssembly.Limits opaqueLimits, Instant evaluatedAt,
            ai.protomolt.proto.repo.spi.RepositoryCaller caller, List<DocumentReadLedger.PinnedHistory> histories,
            ai.protomolt.proto.repo.spi.RepositoryReadControl control) throws InvalidProtocolBufferException {
        var sources = DocumentHistoricalAssessmentSources.open(command, caller, histories, control);
        DocumentPublicationAssessment assessment = null;
        boolean delivered = false;
        try {
            assessment = prepareInternal(command, policy, modes, supplied, container, resolver, budget, opaqueLimits,
                    evaluatedAt, control::check, sources, control);
            sources.authorize(control);
            var result = new Historical(assessment, sources); delivered = true;
            return result;
        } catch (RuntimeException | InvalidProtocolBufferException failure) {
            sources.authorize(control);
            throw failure;
        } finally {
            if (!delivered) {
                if (assessment != null) assessment.close();
                sources.close();
            }
        }
    }

    /** Historical callers receive only this owner, never a candidate or terminal-publication capability. */
    static final class Historical implements AutoCloseable {
        record MemberInspection(ByteString commandSha256, String policySha256, Instant evaluatedAt,
                int rootCount, Optional<DocumentSchemaAssessment.Failure> failure) {}
        /**
         * Authorized-once summaries plus the caller-supplied immutable command and policy.
         * Contains no live view, payload, descriptor or retention capability. Internal only:
         * failure paths and rule identities may contain user-authored schema text.
         */
        record Snapshot(DocumentPublicationCommand command, DocumentSchemaPolicies.Selection policy, Instant evaluatedAt,
                Map<String, MemberInspection> typed, java.util.Set<String> opaque, Optional<MemberFailure> failure) {}
        final class Inspection {
            private final ai.protomolt.proto.repo.spi.RepositoryReadControl control;
            private boolean active = true;
            private Inspection(ai.protomolt.proto.repo.spi.RepositoryReadControl control) { this.control = control; }
            Snapshot snapshot() {
                synchronized (Historical.this) {
                    if (!active) throw new IllegalStateException("Historical inspection callback has ended");
                    requireOpen(); sources.authorize(control);
                    var members = new LinkedHashMap<String, MemberInspection>();
                    assessment.typed().forEach((id, view) -> members.put(id, new MemberInspection(view.request().commandSha256(),
                            view.request().policySha256(), view.evaluatedAt(), view.roots().size(), view.failure())));
                    return new Snapshot(assessment.command(), assessment.policy(), assessment.evaluatedAt(),
                            Map.copyOf(members), java.util.Set.copyOf(assessment.opaque().keySet()), assessment.failure());
                }
            }
        }
        private DocumentPublicationAssessment assessment;
        private final DocumentHistoricalAssessmentSources sources;
        private boolean inspecting;
        private Historical(DocumentPublicationAssessment assessment, DocumentHistoricalAssessmentSources sources) {
            this.assessment = assessment; this.sources = sources;
        }
        /** The inspection facade expires at callback exit; its summaries are values authorized at snapshot time. */
        synchronized void inspect(java.util.function.Consumer<Inspection> consumer,
                ai.protomolt.proto.repo.spi.RepositoryReadControl control) {
            requireOpen();
            if (inspecting) throw new IllegalStateException("Historical assessment inspection is active");
            inspecting = true;
            var inspection = new Inspection(control);
            try {
                sources.authorize(control);
                consumer.accept(inspection);
            } finally {
                inspection.active = false;
                try { sources.authorize(control); }
                finally { inspecting = false; }
            }
        }
        synchronized void verifySchemas(ai.protomolt.proto.repo.spi.RepositoryReadControl control) throws InvalidProtocolBufferException {
            requireOpen();
            if (inspecting) throw new IllegalStateException("Historical assessment inspection is active");
            inspecting = true;
            try { sources.authorize(control); assessment.verifySchemas(control::check); }
            finally {
                try { sources.authorize(control); }
                finally { inspecting = false; }
            }
        }
        private void requireOpen() { if (assessment == null) throw new IllegalStateException("Historical assessment is closed"); }
        @Override public synchronized void close() {
            if (inspecting) throw new IllegalStateException("Historical assessment inspection is active");
            if (assessment == null) return;
            try { assessment.close(); }
            finally { assessment = null; sources.close(); }
        }
    }

    private static DocumentPublicationAssessment prepareInternal(DocumentPublicationCommand command, DocumentSchemaPolicies.Selection policy,
            Map<String, DocumentPublicationCandidate.Mode> modes, Map<String, Map<Integer, ByteString>> supplied,
            Optional<DocumentSchemaAdmission.Definition> container, DocumentPublicationCandidate.Resolver resolver,
            PayloadBudget budget, DocumentRevisionAssembly.Limits opaqueLimits, Instant evaluatedAt, Runnable control,
            DocumentHistoricalAssessmentSources historical, ai.protomolt.proto.repo.spi.RepositoryReadControl readControl)
            throws InvalidProtocolBufferException {
        Objects.requireNonNull(command); Objects.requireNonNull(policy); Objects.requireNonNull(modes);
        Objects.requireNonNull(container); Objects.requireNonNull(resolver); Objects.requireNonNull(budget);
        Objects.requireNonNull(opaqueLimits); Objects.requireNonNull(evaluatedAt); active(control);
        var selectedModes = DocumentPublicationCandidate.requireModes(command, policy, modes,
                member -> container.isPresent() || (historical != null
                        && member.getPartsList().stream().anyMatch(part -> part.hasHistoricalReuse())
                        && member.getPartsList().stream().allMatch(part -> part.hasHistoricalReuse() || part.hasEmpty())), control);
        if (historical != null) for (var member : command.intent().getMembersList()) {
            if (selectedModes.get(member.getMemberId()) == DocumentPublicationCandidate.Mode.OPAQUE
                    && member.getPartsList().stream().anyMatch(part -> part.hasHistoricalReuse()))
                throw new UnsupportedOperationException("Historical opaque assessment requires explicit source classification");
        }
        var reservations = reservations(budget);
        var typed = new LinkedHashMap<String, DocumentSchemaAssessment>();
        var owners = new ArrayList<DocumentSchemaAssessment>();
        var snapshot = historical == null ? DocumentPublicationFragments.capture(command, supplied, budget, control)
                : DocumentPublicationFragments.captureHistorical(command, supplied, historical.references(command, control), budget, control);
        boolean transferred = false;
        try {
            var opaque = new LinkedHashMap<String, DocumentCommandContent>();
            var union = new DocumentSchemaUnion();
            MemberFailure failure = null;
            var digest = ByteString.copyFrom(HexFormat.of().parseHex(command.sha256()));
            for (var member : command.intent().getMembersList()) {
                active(control);
                var id = member.getMemberId();
                var bytes = snapshot.fragments().get(id);
                if (selectedModes.get(id) == DocumentPublicationCandidate.Mode.TYPED) {
                    DocumentSchemaAssessment assessment;
                    if (historical == null) {
                        assessment = policy.policy().assess(digest, member, bytes, container.orElseThrow(),
                                occurrence -> resolver.select(member, occurrence), reservations, evaluatedAt, () -> active(control));
                    } else {
                        try (var source = historical.resolve(member, container, resolver, policy.policy().limits(), budget, readControl)) {
                            var composite = source.resolver();
                            assessment = policy.policy().assess(digest, member, bytes, composite.container(), composite,
                                    reservations, evaluatedAt, () -> active(control));
                            try { composite.requireComplete(assessment.view()); }
                            catch (RuntimeException | Error | InvalidProtocolBufferException failureDuringCheck) {
                                assessment.close(); throw failureDuringCheck;
                            }
                        }
                    }
                    try { owners.add(assessment); }
                    catch (RuntimeException | Error failed) { assessment.close(); throw failed; }
                    typed.put(id, assessment);
                    union.add(assessment.roots(), assessment.artifacts(), () -> active(control));
                    if (failure == null && assessment.failure().isPresent())
                        failure = new MemberFailure(id, assessment.failure().orElseThrow());
                } else opaque.put(id, historical == null
                        ? DocumentCommandContent.check(command, id, bytes, false, opaqueLimits, () -> active(control))
                        : DocumentCommandContent.checkHistorical(command, id, bytes, false, opaqueLimits,
                                historical.references(command, control), () -> active(control)));
            }
            active(control);
            var result = new DocumentPublicationAssessment(snapshot, policy, evaluatedAt, selectedModes, typed, opaque,
                    union.artifacts(), failure, budget);
            transferred = true;
            return result;
        } finally {
            if (!transferred) {
                for (int i = owners.size() - 1; i >= 0; i--) owners.get(i).close();
                snapshot.close();
            }
        }
    }

    /**
     * Transfer this owner's fragments into a candidate after one independent strict
     * check per typed member. Failure leaves this assessment open for retry. Success
     * consumes this scope: close becomes a no-op and the candidate releases its bytes.
     */
    DocumentPublicationCandidate promoteAccepted(Runnable control) throws InvalidProtocolBufferException {
        beginVerification();
        var proofs = new LinkedHashMap<String, DocumentSchemaAdmission.Proof>();
        var prepared = new ArrayList<DocumentSchemaAdmission.PreparedProof>();
        boolean transferred = false;
        try {
            active(control);
            if (failure != null) throw new IllegalArgumentException("Invalid operation assessment cannot be promoted");
            var command = fragments.command();
            if (typed.size() + opaque.size() != command.intent().getMembersCount()
                    || modes.size() != command.intent().getMembersCount())
                throw new IllegalArgumentException("Assessment membership differs from operation");
            var digest = ByteString.copyFrom(HexFormat.of().parseHex(command.sha256()));
            var reservations = reservations(budget);
            for (var member : command.intent().getMembersList()) {
                active(control);
                var id = member.getMemberId();
                if (modes.get(id) == DocumentPublicationCandidate.Mode.OPAQUE) {
                    if (!opaque.containsKey(id) || typed.containsKey(id) || policy.policy().requiresTyped(member))
                        throw new IllegalArgumentException("Opaque member has inconsistent assessment mode");
                    continue;
                }
                if (modes.get(id) != DocumentPublicationCandidate.Mode.TYPED || opaque.containsKey(id))
                    throw new IllegalArgumentException("Typed member has inconsistent assessment mode");
                var view = Objects.requireNonNull(typed.get(id), "Missing typed assessment");
                if (!view.request().commandSha256().equals(digest) || !view.request().member().equals(member)
                        || !view.evaluatedAt().equals(evaluatedAt))
                    throw new IllegalArgumentException("Member assessment differs from operation identity");
                var owner = DocumentSchemaAdmission.checkAccepted(view, policy.policy().limits(), reservations,
                        () -> active(control));
                try { prepared.add(owner); }
                catch (RuntimeException | Error failed) { owner.close(); throw failed; }
                proofs.put(id, owner.proof());
            }
            var schemas = DocumentSchemaBatch.prepare(command, policy, proofs, reservations, () -> active(control));
            if (!schemas.artifacts().equals(artifacts))
                throw new IllegalArgumentException("Promoted artifacts differ from assessment union");
            active(control);
            var candidate = DocumentPublicationCandidate.fromAssessment(this::closePromoted, prepared, schemas, opaque);
            synchronized (this) { promoted = true; }
            transferred = true;
            return candidate;
        } finally {
            if (!transferred) for (int i = prepared.size() - 1; i >= 0; i--) prepared.get(i).close();
            finishVerification();
        }
    }

    /**
     * Reproduce all typed verdicts from owned evidence before a terminal decision.
     * No resolver, provider or SQL calls occur here. Scratch uses the original
     * shared budget and is released per member. This grants no commit authority.
     * Close and another verification are refused until this invocation drains.
     */
    void verifySchemas(Runnable control) throws InvalidProtocolBufferException {
        beginVerification();
        try { verifyOwned(control); }
        finally { finishVerification(); }
    }

    /**
     * Encode the exact completed assessment after independent replay. Runtime is
     * declared provenance for codec/projection use, not observed-runtime evidence.
     * This cannot construct an ObservedManifest and is not a durable-decision input. Owner identity
     * is checked locally, not fenced in SQL. Returned bytes own a separate budget
     * lease; they retain neither candidate bytes nor schema assets after parent close.
     */
    DocumentAssessmentManifestCodec.Encoded encodeDeclaredManifest(RepositoryOperationLedger.Owner owner,
            ai.protomolt.proto.repo.v1.DocumentAssessmentRuntime runtime, Runnable control)
            throws InvalidProtocolBufferException {
        beginVerification();
        try {
            return encodeOwned(owner, runtime, control);
        } finally { finishVerification(); }
    }

    /**
     * Replay and encode under an observed runtime. The result owns only encoding bytes;
     * a durable consumer must separately retain candidate/evidence bytes and fence its decision.
     */
    ObservedManifest encodeManifest(RepositoryOperationLedger.Owner owner,
            DocumentAssessmentRuntimeObserver.Observation observation, Runnable control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(observation);
        beginVerification();
        try { return observeOwned(owner, observation, control); }
        finally { finishVerification(); }
    }

    /**
     * Borrow the exact schema and root evidence while the parent remains protected
     * from close/reentrant verification. No independent payload copies are made.
     * The consumer owns all SQL/authorization fences and must check the evidence
     * inside its decision transaction before commit. Borrowed values must not escape.
     */
    <T> T withRetentionEvidence(RepositoryOperationLedger.Owner owner,
            DocumentAssessmentRuntimeObserver.Observation observation, Runnable control,
            java.util.function.Function<DocumentAssessmentEvidence, T> consumer) throws InvalidProtocolBufferException {
        Objects.requireNonNull(observation); Objects.requireNonNull(consumer);
        beginVerification();
        try (var observed = observeOwned(owner, observation, control);
                var evidence = new DocumentAssessmentEvidence(this, owner, observed, reservations(budget), control)) {
            evidence.check(control);
            T result = consumer.apply(evidence);
            evidence.check(control);
            return result;
        } finally { finishVerification(); }
    }

    private ObservedManifest observeOwned(RepositoryOperationLedger.Owner owner,
            DocumentAssessmentRuntimeObserver.Observation observation, Runnable control) throws InvalidProtocolBufferException {
        DocumentAssessmentManifestCodec.Encoded encoded = null;
        try {
            var runtime = observation.identity(control);
            encoded = encodeOwned(owner, runtime, control);
            observation.identity(control);
            var result = new ObservedManifest(encoded, observation);
            encoded = null;
            return result;
        } finally {
            if (encoded != null) encoded.close();
        }
    }

    private DocumentAssessmentManifestCodec.Encoded encodeOwned(RepositoryOperationLedger.Owner owner,
            ai.protomolt.proto.repo.v1.DocumentAssessmentRuntime runtime, Runnable control) throws InvalidProtocolBufferException {
        var manifest = DocumentAssessmentProjection.project(fragments.command(), policy, evaluatedAt, modes, typed,
                failure, owner, runtime, () -> active(control));
        verifyOwned(control);
        return DocumentAssessmentManifestCodec.encode(manifest, reservations(budget), () -> active(control));
    }

    /**
     * Constructible only after observed replay and encoding. Borrowed bytes are not an
     * independent proof and must not outlive this owner. Close always releases the lease,
     * even after the runtime context becomes unsupported.
     */
    static final class ObservedManifest implements AutoCloseable {
        private DocumentAssessmentManifestCodec.Encoded encoded;
        private final DocumentAssessmentRuntimeObserver.Observation observation;
        private ObservedManifest(DocumentAssessmentManifestCodec.Encoded encoded, DocumentAssessmentRuntimeObserver.Observation observation) {
            this.encoded = encoded; this.observation = observation;
        }
        synchronized ByteString bytes(Runnable control) { check(control); return encoded.bytes(); }
        synchronized String sha256(Runnable control) { check(control); return encoded.sha256(); }
        synchronized void check(Runnable control) {
            requireOpen(); observation.identity(control); requireOpen();
        }
        private void requireOpen() { if (encoded == null) throw new IllegalStateException("Observed manifest is closed"); }
        @Override public synchronized void close() {
            if (encoded != null) { encoded.close(); encoded = null; }
        }
    }

    private synchronized void beginVerification() {
        requireOpen();
        if (verifying) throw new IllegalStateException("Publication assessment verification is active");
        verifying = true;
    }
    private synchronized void finishVerification() { verifying = false; }

    private void verifyOwned(Runnable control) throws InvalidProtocolBufferException {
        active(control);
        var command = fragments.command();
        if (typed.size() + opaque.size() != command.intent().getMembersCount()
                || modes.size() != command.intent().getMembersCount())
            throw new IllegalArgumentException("Assessment membership differs from operation");
        var digest = ByteString.copyFrom(HexFormat.of().parseHex(command.sha256()));
        var union = new DocumentSchemaUnion();
        MemberFailure first = null;
        for (var member : command.intent().getMembersList()) {
            active(control);
            var id = member.getMemberId();
            if (modes.get(id) == DocumentPublicationCandidate.Mode.OPAQUE) {
                if (!opaque.containsKey(id) || typed.containsKey(id))
                    throw new IllegalArgumentException("Opaque member has inconsistent assessment mode");
                continue;
            }
            if (modes.get(id) != DocumentPublicationCandidate.Mode.TYPED || opaque.containsKey(id))
                throw new IllegalArgumentException("Typed member has inconsistent assessment mode");
            var view = Objects.requireNonNull(typed.get(id), "Missing typed assessment");
            var request = DocumentSchemaAssessmentReplay.Request.from(view);
            if (!request.candidate().commandSha256().equals(digest) || !request.candidate().member().equals(member)
                    || !request.evaluatedAt().equals(evaluatedAt))
                throw new IllegalArgumentException("Member assessment differs from operation identity");
            DocumentSchemaAssessmentReplay.verify(request, policy.policy(), hash -> Optional.ofNullable(artifacts.get(hash)),
                    reservations(budget), () -> active(control));
            union.add(view.roots(), view.artifacts(), () -> active(control));
            if (first == null && view.failure().isPresent()) first = new MemberFailure(id, view.failure().orElseThrow());
        }
        if (!union.artifacts().equals(artifacts) || !Objects.equals(first, failure))
            throw new IllegalArgumentException("Replayed assessment differs from operation result");
        active(control);
    }

    private static DocumentAdmissionReservations reservations(PayloadBudget budget) {
        return bytes -> {
            try {
                var lease = budget.reserve(bytes);
                return lease::close;
            } catch (PayloadBudget.CapacityExceededException exhausted) {
                throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Publication assessment capacity exhausted");
            }
        };
    }

    synchronized DocumentPublicationCommand command() { requireOpen(); return fragments.command(); }
    synchronized DocumentSchemaPolicies.Selection policy() { requireOpen(); return policy; }
    synchronized Instant evaluatedAt() { requireOpen(); return evaluatedAt; }
    synchronized Map<String, DocumentPublicationCandidate.Mode> modes() { requireOpen(); return modes; }
    /** Borrow only; the parent retains exclusive resource ownership. */
    synchronized Map<String, DocumentSchemaAssessment.View> typed() { requireOpen(); return typed; }
    synchronized Map<String, DocumentCommandContent> opaque() { requireOpen(); return opaque; }
    synchronized Map<String, ByteString> artifacts() { requireOpen(); return artifacts; }
    synchronized Optional<MemberFailure> failure() { requireOpen(); return Optional.ofNullable(failure); }
    private void requireOpen() {
        if (typed == null || promoted) throw new IllegalStateException("Publication assessment is closed or transferred");
    }
    @Override public synchronized void close() {
        if (promoted) return;
        if (typed == null) return;
        if (verifying) throw new IllegalStateException("Publication assessment verification is active");
        releaseOwned();
    }
    private synchronized void closePromoted() {
        if (!promoted) throw new IllegalStateException("Assessment ownership was not transferred");
        promoted = false;
        releaseOwned();
    }
    private void releaseOwned() {
        typed = null; opaque = Map.of(); modes = Map.of(); artifacts = Map.of(); failure = null;
        for (int i = owners.size() - 1; i >= 0; i--) owners.get(i).close();
        owners.clear(); fragments.close();
    }
    private static void active(Runnable control) {
        if (Thread.currentThread().isInterrupted()) throw new java.util.concurrent.CancellationException("Publication assessment interrupted");
        Objects.requireNonNull(control).run();
    }
}
