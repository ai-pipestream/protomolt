package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;

/**
 * Owned schema/evidence assets after one member's complete root assessment. This
 * is not an admission Proof, policy authorization or a durable rejection. The
 * host must keep fragment bytes stable and alive until this owner is closed.
 */
public final class DocumentSchemaAssessment implements AutoCloseable {
    /** Internal identity; paths and rule IDs may contain user text and are not public receipt text. */
    public record Failure(int ordinal, DocumentSchemaRootLocator root, List<RepositorySchemaOccurrenceStep> occurrence,
                          String fieldPath, String ruleId, String rulePath) {
        public Failure { occurrence = List.copyOf(occurrence); }
    }

    private final DocumentAdmissionResources resources;
    private final Instant evaluatedAt;
    private DocumentSchemaAdmission.Preparation request;
    private List<DocumentSchemaAdmission.RootEvidence> roots;
    private List<DocumentSchemaAdmission.Reference> references;
    private Map<String, ByteString> artifacts;
    private Failure failure;

    private DocumentSchemaAssessment(DocumentAdmissionResources resources, Instant evaluatedAt,
            DocumentSchemaAdmission.Preparation request, List<DocumentSchemaAdmission.RootEvidence> roots,
            List<DocumentSchemaAdmission.Reference> references, Map<String, ByteString> artifacts, Failure failure) {
        this.resources = resources; this.evaluatedAt = evaluatedAt; this.request = request;
        this.roots = List.copyOf(roots); this.references = List.copyOf(references);
        this.artifacts = Map.copyOf(artifacts); this.failure = failure;
    }

    /**
     * Verifies all member fragment hashes before root assessment, then continues
     * through all roots after a payload violation. Structural document failures,
     * unresolved schemas, unsupported rules and operational failures still throw.
     * Resolver inputs are borrowed during copying. Serialized assets/evidence are
     * reserved before allocation; decoded graphs and resolver allocations remain
     * bounded by the caller. No registry/provider access occurs after return.
     */
    public static DocumentSchemaAssessment prepare(DocumentSchemaAdmission.Preparation input,
            DocumentSchemaAdmission.Resolver resolver, DocumentSchemaAdmission.Limits limits,
            DocumentAdmissionReservations reservations, Instant evaluatedAt, Runnable control)
            throws InvalidProtocolBufferException {
        Objects.requireNonNull(input); Objects.requireNonNull(resolver); Objects.requireNonNull(limits);
        Objects.requireNonNull(evaluatedAt); Objects.requireNonNull(control);
        Runnable active = () -> {
            if (Thread.currentThread().isInterrupted()) throw new CancellationException("member assessment interrupted");
            control.run();
        };
        active.run();
        if (input.commandSha256().size() != 32 || !input.policySha256().matches("[0-9a-f]{64}"))
            throw new IllegalArgumentException("exact command and policy digests required");
        if (input.fragments().size() > limits.maxFragments())
            throw new IllegalArgumentException("member admission count exceeds limit");
        var fragments = Map.copyOf(input.fragments());
        DocumentSchemaAdmission.checkMember(input.member(), fragments, input.requireStructuredRoot(), limits, evaluatedAt, active);
        var resources = new DocumentAdmissionResources(reservations);
        boolean transferred = false;
        try {
            var definitions = new DocumentSchemaDefinitions(limits, resources, active);
            var container = definitions.add(input.container());
            var roots = new ArrayList<DocumentSchemaAdmission.RootEvidence>();
            Failure failure = null;
            long decodedRemaining = limits.maxDecodedBytes();
            long evidenceBytes = 0;
            int rootCount = 0;
            for (int ordinal = 0; ordinal < input.member().getPartsCount(); ordinal++) {
                active.run();
                var part = input.member().getParts(ordinal);
                if (part.hasEmpty() || (part.getSlot().getPart() != DocumentPart.DOCUMENT_PART_CORE
                        && part.getSlot().getPart() != DocumentPart.DOCUMENT_PART_PARSED)) continue;
                var inventory = DocumentAnyRootInventory.inspect(container.schema(), part.getSlot(), fragments.get(ordinal),
                        input.member().getDestination().getAddress().getDocId(),
                        new DocumentAnyRootInventory.Limits(Integer.MAX_VALUE, 1_000_000, 64, limits.maxRoots(), 10000), active);
                if (inventory.roots().size() > limits.maxRoots() - rootCount)
                    throw new IllegalArgumentException("member evidence root limit exceeded");
                rootCount += inventory.roots().size();
                final int rootOrdinal = ordinal;
                for (var root : inventory.roots()) {
                    active.run();
                    if (decodedRemaining < 1) throw new IllegalArgumentException("member decoded payload budget exhausted");
                    var locator = DocumentSchemaRootProjection.project(inventory, root, active);
                    var envelope = root.envelope();
                    var selected = definitions.select(resolver, new DocumentSchemaAdmission.Selection(ordinal, locator,
                            envelope.getTypeUrl(), List.of(), DocumentSchemaOccurrences.sha256(envelope.getValue(), active),
                            envelope.getValue().size()));
                    if (part.getSlot().getPart() == DocumentPart.DOCUMENT_PART_CORE && input.member().hasStructuredSchema()
                            && !selected.schema().condition().equals(input.member().getStructuredSchema()))
                        throw new IllegalArgumentException("structured root differs from required schema");
                    var assessment = DocumentPayloadCheck.assessContextualAssets(selected, envelope, envelope.getTypeUrl(),
                            DocumentSchemaAdmission.VALIDATOR,
                            new DocumentPayloadCheck.Limits((int) decodedRemaining, 1_000_000, 64, 4096, 65536), active,
                            nested -> definitions.select(resolver, new DocumentSchemaAdmission.Selection(rootOrdinal, locator,
                                    nested.typeUrl(), DocumentSchemaOccurrenceProjection.projectSteps(nested.prefix(), definitions.schemas, active),
                                    nested.valueSha256(), nested.valueSizeBytes())), DocumentSchemaOccurrences.Limits.DEFAULT, evaluatedAt);
                    List<DocumentSchemaOccurrences.Occurrence> occurrences;
                    if (assessment instanceof DocumentPayloadAssessment.Accepted accepted) {
                        occurrences = accepted.checked().payload().occurrences();
                        decodedRemaining -= accepted.checked().payload().decodedBytes();
                    } else {
                        var invalid = (DocumentPayloadAssessment.Invalid) assessment;
                        occurrences = invalid.occurrences();
                        decodedRemaining -= invalid.decodedBytes();
                        if (failure == null) {
                            var first = invalid.failure();
                            failure = new Failure(ordinal, locator,
                                    DocumentSchemaOccurrenceProjection.projectSteps(first.occurrence(), definitions.schemas, active),
                                    first.fieldPath(), first.ruleId(), first.rulePath());
                        }
                    }
                    var bundle = DocumentRootSchemaEvidence.newBuilder().setEncodingVersion(1).setRoot(locator)
                            .addAllOccurrences(DocumentSchemaOccurrenceProjection.project(occurrences, definitions.schemas, active)).build();
                    var encoded = resources.retain(DocumentRootSchemaEvidenceCodec.encodeOwned(bundle,
                            limits.maxEvidenceBytes() - evidenceBytes, resources, active));
                    evidenceBytes += encoded.bytes().size();
                    try (var encodedLocator = DocumentSchemaEvidenceCodec.encodeOwned(locator, resources, active)) {
                        roots.add(new DocumentSchemaAdmission.RootEvidence(ordinal, locator, encodedLocator.value().sha256(),
                                new DocumentSchemaAdmission.EncodedEvidence(DocumentRootSchemaEvidenceCodec.CODEC,
                                        DocumentRootSchemaEvidenceCodec.VERSION, encoded.bytes(), encoded.sha256())));
                    }
                }
            }
            active.run();
            if (roots.isEmpty()) throw new IllegalArgumentException("typed assessment requires a payload root");
            // The container definition also points at private retained copies, not resolver buffers.
            var ownedContainer = new DocumentSchemaAdmission.Definition(container.metadata(), container.schema().artifact(),
                    input.container().source().map(ignored -> definitions.artifacts.get(
                            container.metadata().getCompilation().getSourceArtifactSha256())));
            var request = new DocumentSchemaAdmission.Preparation(input.commandSha256(), input.policySha256(),
                    input.requireStructuredRoot(), input.member(), fragments, ownedContainer);
            var result = new DocumentSchemaAssessment(resources, evaluatedAt, request, roots,
                    new ArrayList<>(definitions.references.values()), definitions.artifacts, failure);
            transferred = true;
            return result;
        } catch (DocumentAdmissionResources.ReservationFailure failed) {
            throw failed.original;
        } finally {
            if (!transferred) resources.close();
        }
    }

    /** Borrowed access without the ability to release this owner's resources. */
    public final class View {
        private View() {}
        public DocumentSchemaAdmission.Preparation request() { return DocumentSchemaAssessment.this.request(); }
        public Instant evaluatedAt() { return DocumentSchemaAssessment.this.evaluatedAt(); }
        public List<DocumentSchemaAdmission.RootEvidence> roots() { return DocumentSchemaAssessment.this.roots(); }
        public List<DocumentSchemaAdmission.Reference> references() { return DocumentSchemaAssessment.this.references(); }
        public Map<String, ByteString> artifacts() { return DocumentSchemaAssessment.this.artifacts(); }
        public Optional<Failure> failure() { return DocumentSchemaAssessment.this.failure(); }
    }

    /** The host must still drain consumers before closing; previously borrowed bytes cannot be revoked. */
    public synchronized View view() { requireOpen(); return new View(); }
    public synchronized DocumentSchemaAdmission.Preparation request() { requireOpen(); return request; }
    public synchronized Instant evaluatedAt() { requireOpen(); return evaluatedAt; }
    public synchronized List<DocumentSchemaAdmission.RootEvidence> roots() { requireOpen(); return roots; }
    public synchronized List<DocumentSchemaAdmission.Reference> references() { requireOpen(); return references; }
    public synchronized Map<String, ByteString> artifacts() { requireOpen(); return artifacts; }
    public synchronized Optional<Failure> failure() { requireOpen(); return Optional.ofNullable(failure); }
    private void requireOpen() { if (request == null) throw new IllegalStateException("Member assessment is closed"); }
    @Override public synchronized void close() {
        if (request == null) return;
        request = null; roots = List.of(); references = List.of(); artifacts = Map.of(); failure = null;
        resources.close();
    }
}
