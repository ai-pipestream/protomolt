package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.repo.v1.DocumentSchemaRootLocator;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrenceStep;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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

    private record Root(int ordinal, DocumentSchemaRootLocator locator) {}
    private record Selection(int ordinal, DocumentSchemaRootLocator root, String url,
                             List<RepositorySchemaOccurrenceStep> prefix, String hash, long size) {
        Selection { prefix = List.copyOf(prefix); }
        static Selection from(DocumentSchemaAdmission.Selection occurrence) {
            return new Selection(occurrence.ordinal(), occurrence.root(), occurrence.typeUrl(), occurrence.prefix(),
                    occurrence.valueSha256(), occurrence.valueSizeBytes());
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
        try (var scratch = new DocumentAdmissionResources(reservations)) {
            var bundles = DocumentSchemaAdmission.decodeEvidence(candidate.evidence(), limits, scratch, active);
            var expectedRoots = new HashMap<Root, DocumentSchemaAdmission.EncodedEvidence>();
            var selections = new HashSet<Selection>();
            for (var entry : bundles.entrySet()) {
                int ordinal = entry.getKey();
                if (ordinal < 0 || ordinal >= candidate.member().getPartsCount())
                    throw new IllegalArgumentException("replay evidence ordinal outside member");
                for (int i = 0; i < entry.getValue().size(); i++) {
                    active.run();
                    var bundle = entry.getValue().get(i);
                    if (candidate.member().getParts(ordinal).hasEmpty())
                        throw new IllegalArgumentException("replay evidence attached to empty slot");
                    if (!bundle.getRoot().getSlot().equals(candidate.member().getParts(ordinal).getSlot()))
                        throw new IllegalArgumentException("replay evidence slot differs from member part");
                    if (expectedRoots.putIfAbsent(new Root(ordinal, bundle.getRoot()), candidate.evidence().get(ordinal).get(i)) != null)
                        throw new IllegalArgumentException("duplicate replay evidence root");
                    for (var path : bundle.getOccurrencesList()) {
                        active.run();
                        var boundary = path.getSteps(path.getStepsCount() - 1).getAnyBoundary();
                        if (!selections.add(new Selection(ordinal, bundle.getRoot(), boundary.getTypeUrl(),
                                path.getStepsList().subList(0, path.getStepsCount() - 1), boundary.getValueSha256(), boundary.getValueSizeBytes())))
                            throw new IllegalArgumentException("conflicting replay occurrence selection");
                    }
                }
            }
            var references = new ArrayList<DocumentSchemaAdmission.Reference>();
            references.add(candidate.container()); references.addAll(candidate.references());
            var expectedArtifacts = new HashSet<String>();
            var byKey = new HashMap<DocumentPayloadCheck.SchemaKey, DocumentSchemaAdmission.Reference>();
            for (var reference : references) {
                active.run();
                var key = new DocumentPayloadCheck.SchemaKey(reference.typeUrl(), reference.descriptorSha256());
                if (byKey.putIfAbsent(key, reference) != null) throw new IllegalArgumentException("duplicate replay schema association");
                expectedArtifacts.add(reference.descriptorSha256()); expectedArtifacts.add(reference.metadataSha256());
                reference.sourceSha256().ifPresent(expectedArtifacts::add);
            }
            if (expectedArtifacts.size() > 64) throw new IllegalArgumentException("retained artifact count exceeds limit");
            var loaded = new HashMap<String, ByteString>();
            var retained = new DocumentRetainedSchemaAssets(hash -> {
                var cached = loaded.get(hash);
                if (cached != null) return Optional.of(cached);
                var bytes = Objects.requireNonNull(reader.read(hash));
                bytes.ifPresent(value -> loaded.put(hash, value));
                return bytes;
            }, new DocumentRetainedSchemaAssets.Limits(limits.maxBindings(), limits.maxRetainedBytes(),
                    new ClosedDescriptorSet.Limits(16 * 1024 * 1024, 256, 4096, 64)), scratch);
            var definitions = new HashMap<DocumentPayloadCheck.SchemaKey, DocumentSchemaAdmission.Definition>();
            for (var reference : references) {
                active.run();
                var asset = retained.resolve(reference.internal(), active);
                definitions.put(new DocumentPayloadCheck.SchemaKey(reference.typeUrl(), reference.descriptorSha256()),
                        new DocumentSchemaAdmission.Definition(asset.metadata(), loaded.get(reference.descriptorSha256()),
                                reference.sourceSha256().map(loaded::get)));
            }
            var index = new HashMap<Selection, DocumentSchemaAdmission.Definition>();
            for (var entry : bundles.entrySet()) {
                int ordinal = entry.getKey();
                for (int i = 0; i < entry.getValue().size(); i++) {
                    active.run();
                    var bundle = entry.getValue().get(i);
                    for (var path : bundle.getOccurrencesList()) {
                        active.run();
                        var boundary = path.getSteps(path.getStepsCount() - 1).getAnyBoundary();
                        var key = new DocumentPayloadCheck.SchemaKey(boundary.getTypeUrl(), boundary.getResolved().getArtifactSha256());
                        var definition = definitions.get(key);
                        if (definition == null || !definition.metadata().getSchema().equals(boundary.getResolved().getSchema()))
                            throw new IllegalArgumentException("replay occurrence differs from retained schema association");
                        var selection = new Selection(ordinal, bundle.getRoot(), boundary.getTypeUrl(),
                                path.getStepsList().subList(0, path.getStepsCount() - 1), boundary.getValueSha256(), boundary.getValueSizeBytes());
                        if (index.putIfAbsent(selection, definition) != null)
                            throw new IllegalArgumentException("conflicting replay occurrence selection");
                    }
                }
            }
            var used = new HashSet<Selection>();
            var container = definitions.get(new DocumentPayloadCheck.SchemaKey(candidate.container().typeUrl(), candidate.container().descriptorSha256()));
            try (var replayed = policy.assess(candidate.commandSha256(), candidate.member(), candidate.fragments(), container,
                    occurrence -> {
                        var key = Selection.from(occurrence);
                        var selected = index.get(key);
                        if (selected == null) throw new IllegalArgumentException("candidate occurrence has no exact retained schema selection");
                        if (!used.add(key)) throw new IllegalArgumentException("replay occurrence selected more than once");
                        return selected;
                    }, scratch, evaluatedAt, active)) {
                var actualRoots = new HashMap<Root, DocumentSchemaAdmission.EncodedEvidence>();
                for (var root : replayed.roots()) {
                    if (actualRoots.putIfAbsent(new Root(root.ordinal(), root.locator()), root.encoded()) != null)
                        throw new IllegalArgumentException("duplicate reassessed evidence root");
                }
                if (!used.equals(index.keySet()) || !actualRoots.equals(expectedRoots))
                    throw new IllegalArgumentException("reassessed roots or occurrences differ from retained evidence");
                if (!new HashSet<>(replayed.references()).equals(new HashSet<>(references))
                        || !replayed.artifacts().keySet().equals(expectedArtifacts) || !loaded.keySet().equals(expectedArtifacts))
                    throw new IllegalArgumentException("reassessed schema assets differ from complete retained union");
                active.run();
                return replayed.failure();
            }
        } catch (DocumentAdmissionResources.ReservationFailure failed) {
            throw failed.original;
        }
    }
}
