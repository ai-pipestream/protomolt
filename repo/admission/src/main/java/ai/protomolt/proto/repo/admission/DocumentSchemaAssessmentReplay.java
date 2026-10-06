package ai.protomolt.proto.repo.admission;

import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;

/** Reproduces a member assessment from frozen evidence; no registry lookup or durable authority. */
public final class DocumentSchemaAssessmentReplay {
    private DocumentSchemaAssessmentReplay() {}

    /** Parsed inputs are caller-bounded, stable and authenticated separately. */
    public record Request(DocumentSchemaAdmission.Request candidate, Instant evaluatedAt,
                          Optional<DocumentSchemaAssessment.Failure> expectedFailure) {
        public Request { Objects.requireNonNull(candidate); Objects.requireNonNull(evaluatedAt); Objects.requireNonNull(expectedFailure); }
        /** Borrows bytes from the view and its input owner; neither may close until verification finishes. */
        public static Request from(DocumentSchemaAssessment.View view) {
            var input = view.request();
            var evidence = new HashMap<Integer, List<DocumentSchemaAdmission.EncodedEvidence>>();
            for (var root : view.roots()) evidence.computeIfAbsent(root.ordinal(), ignored -> new ArrayList<>()).add(root.encoded());
            evidence.replaceAll((ordinal, roots) -> List.copyOf(roots));
            var references = view.references();
            return new Request(new DocumentSchemaAdmission.Request(input.commandSha256(), input.policySha256(),
                    input.requireStructuredRoot(), input.member(), input.fragments(), Map.copyOf(evidence),
                    references.getFirst(), List.copyOf(references.subList(1, references.size()))), view.evaluatedAt(), view.failure());
        }
    }

    /**
     * Reader allocations and parsed heap remain caller-owned. Reservations cover
     * canonical scratch and reassessment assets/evidence and are released before
     * return. Completion reproduces a value verdict under the supplied runtime,
     * not historical runtime execution, policy authorization or publication.
     */
    public static void verify(Request request, DocumentAdmissionPolicy policy, DocumentSchemaAdmission.Reader reader,
            DocumentAdmissionReservations reservations, Runnable control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(request);
        var actual = replay(request.candidate(), request.evaluatedAt(), policy, reader, reservations, control);
        if (!actual.equals(request.expectedFailure()))
            throw new IllegalArgumentException("reassessed value verdict differs from recorded failure");
    }

    /**
     * Reproduce a member's value result when only the operation's first failure was
     * retained. All roots, occurrences and schema assets are verified before return.
     * Empty means the value passed; infrastructure and integrity failures still throw.
     * The caller binds this result to its command and aggregates members in canonical
     * order. This grants no historical-runtime, authorization or publication claim.
     * Input ownership and reservation requirements are the same as {@link #verify}.
     */
    public static Optional<DocumentSchemaAssessment.Failure> replay(DocumentSchemaAdmission.Request candidate,
            Instant evaluatedAt, DocumentAdmissionPolicy policy, DocumentSchemaAdmission.Reader reader,
            DocumentAdmissionReservations reservations, Runnable control) throws InvalidProtocolBufferException {
        Objects.requireNonNull(candidate); Objects.requireNonNull(evaluatedAt);
        Objects.requireNonNull(policy); Objects.requireNonNull(reader);
        Objects.requireNonNull(control);
        Runnable active = () -> {
            if (Thread.currentThread().isInterrupted()) throw new CancellationException("assessment replay interrupted");
            control.run();
        };
        active.run();
        var limits = policy.limits();
        if (!candidate.policySha256().equals(policy.sha256())
                || candidate.requireStructuredRoot() != policy.definition().getRequireStructuredRoot()
                || !candidate.member().getDestination().getAddress().getAccountId().equals(policy.definition().getAccountId())
                || !candidate.member().getOwnership().getAccountId().equals(policy.definition().getAccountId()))
            throw new IllegalArgumentException("assessment replay differs from policy snapshot");
        if (candidate.commandSha256().size() != 32 || candidate.references().size() >= limits.maxBindings()
                || candidate.evidence().size() > limits.maxFragments())
            throw new IllegalArgumentException("assessment replay identities or counts exceed limits");
        DocumentSchemaAdmission.checkMember(candidate.member(), candidate.fragments(), candidate.requireStructuredRoot(),
                limits, evaluatedAt, active);
        var ordinals = new HashMap<Integer, Integer>();
        for (int i = 0; i < candidate.member().getPartsCount(); i++)
            if (!candidate.member().getParts(i).hasEmpty()) ordinals.put(i, i);
        try (var retained = DocumentRetainedSchemaResolution.open(candidate, ordinals, reader, limits, reservations, active);
                var replayed = policy.assess(candidate.commandSha256(), candidate.member(), candidate.fragments(),
                        retained.container(), retained, reservations, evaluatedAt, active)) {
            retained.requireFullUnion(replayed.view());
            active.run();
            return replayed.failure();
        } catch (DocumentAdmissionResources.ReservationFailure failed) {
            throw failed.original;
        }
    }
}
