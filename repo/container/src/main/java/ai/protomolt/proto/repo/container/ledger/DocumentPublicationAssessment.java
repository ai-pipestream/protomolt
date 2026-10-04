package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentAdmissionReservations;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemaAssessment;
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
    private Map<String, DocumentPublicationCandidate.Mode> modes;
    private Map<String, DocumentSchemaAssessment.View> typed;
    private Map<String, DocumentCommandContent> opaque;
    private Map<String, ByteString> artifacts;
    private final List<DocumentSchemaAssessment> owners;
    private MemberFailure failure;

    private DocumentPublicationAssessment(DocumentPublicationFragments fragments, DocumentSchemaPolicies.Selection policy,
            Instant evaluatedAt, Map<String, DocumentPublicationCandidate.Mode> modes,
            Map<String, DocumentSchemaAssessment> typed, Map<String, DocumentCommandContent> opaque,
            Map<String, ByteString> artifacts, MemberFailure failure) {
        this.fragments = fragments; this.policy = policy; this.evaluatedAt = evaluatedAt;
        this.modes = modes; this.opaque = Map.copyOf(opaque);
        var views = new LinkedHashMap<String, DocumentSchemaAssessment.View>();
        typed.forEach((id, owner) -> views.put(id, owner.view()));
        this.typed = Map.copyOf(views);
        this.owners = new ArrayList<>(typed.values()); this.failure = failure;
        this.artifacts = artifacts;
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
        Objects.requireNonNull(container); Objects.requireNonNull(resolver); Objects.requireNonNull(budget);
        Objects.requireNonNull(opaqueLimits); Objects.requireNonNull(evaluatedAt); active(control);
        var selectedModes = DocumentPublicationCandidate.requireModes(command, policy, modes, container, control);
        DocumentAdmissionReservations reservations = bytes -> {
            try {
                var lease = budget.reserve(bytes);
                return lease::close;
            } catch (PayloadBudget.CapacityExceededException exhausted) {
                throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Publication assessment capacity exhausted");
            }
        };
        var typed = new LinkedHashMap<String, DocumentSchemaAssessment>();
        var owners = new ArrayList<DocumentSchemaAssessment>();
        var snapshot = DocumentPublicationFragments.capture(command, supplied, budget, control);
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
                    var assessment = policy.policy().assess(digest, member, bytes, container.orElseThrow(),
                            occurrence -> resolver.select(member, occurrence), reservations, evaluatedAt, () -> active(control));
                    try { owners.add(assessment); }
                    catch (RuntimeException | Error failed) { assessment.close(); throw failed; }
                    typed.put(id, assessment);
                    union.add(assessment.roots(), assessment.artifacts(), () -> active(control));
                    if (failure == null && assessment.failure().isPresent())
                        failure = new MemberFailure(id, assessment.failure().orElseThrow());
                } else opaque.put(id, DocumentCommandContent.check(command, id, bytes, false, opaqueLimits, () -> active(control)));
            }
            active(control);
            var result = new DocumentPublicationAssessment(snapshot, policy, evaluatedAt, selectedModes, typed, opaque,
                    union.artifacts(), failure);
            transferred = true;
            return result;
        } finally {
            if (!transferred) {
                for (int i = owners.size() - 1; i >= 0; i--) owners.get(i).close();
                snapshot.close();
            }
        }
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
    private void requireOpen() { if (typed == null) throw new IllegalStateException("Publication assessment is closed"); }
    @Override public synchronized void close() {
        if (typed == null) return;
        typed = null; opaque = Map.of(); modes = Map.of(); artifacts = Map.of(); failure = null;
        for (int i = owners.size() - 1; i >= 0; i--) owners.get(i).close();
        owners.clear(); fragments.close();
    }
    private static void active(Runnable control) {
        if (Thread.currentThread().isInterrupted()) throw new java.util.concurrent.CancellationException("Publication assessment interrupted");
        Objects.requireNonNull(control).run();
    }
}
