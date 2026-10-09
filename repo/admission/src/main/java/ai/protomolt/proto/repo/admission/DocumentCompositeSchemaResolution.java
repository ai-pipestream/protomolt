package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.repo.v1.DocumentPublicationMember;
import com.google.protobuf.ByteString;
import java.util.*;

/**
 * One member's attempt-local resolver over exact retained sources and ordinary parts.
 * Borrows retained scopes and definition bytes; the host owns source authentication,
 * physical selection, pins and byte lifetimes. Owns only encoded metadata scratch.
 * Not thread-safe. Call requireComplete before using the assessment, then close.
 */
public final class DocumentCompositeSchemaResolution implements DocumentSchemaAdmission.Resolution {
    private final DocumentPublicationMember member;
    private final List<DocumentRetainedSchemaResolution> sources;
    private final DocumentSchemaAdmission.Resolver ordinary;
    private final DocumentSchemaAdmission.Limits limits;
    private final DocumentAdmissionResources resources;
    private final Runnable control;
    private final Map<Integer, DocumentRetainedSchemaResolution> routes = new HashMap<>();
    private final Map<DocumentPayloadCheck.SchemaKey, DocumentSchemaAdmission.Reference> references = new HashMap<>();
    private final Map<String, ByteString> artifacts = new HashMap<>();
    private final Map<DocumentPayloadCheck.SchemaKey, DocumentSchemaAdmission.Definition> definitions = new HashMap<>();
    private final Map<DocumentSchemaAdmission.Selection, DocumentSchemaAdmission.Definition> ordinarySelections = new HashMap<>();
    private DocumentSchemaAdmission.Definition container;
    private long assetBytes;
    private boolean closed;

    private DocumentCompositeSchemaResolution(DocumentPublicationMember member,
            List<DocumentRetainedSchemaResolution> sources, DocumentSchemaAdmission.Resolver ordinary,
            DocumentSchemaAdmission.Limits limits, DocumentAdmissionReservations reservations, Runnable control) {
        this.member = member; this.sources = sources; this.ordinary = ordinary; this.limits = limits;
        this.resources = new DocumentAdmissionResources(reservations); this.control = control;
    }

    /** No SQL, registry lookup or inherited verdict. Every historical ordinal needs one source route. */
    public static DocumentCompositeSchemaResolution open(DocumentPublicationMember member,
            List<DocumentRetainedSchemaResolution> sources, Optional<DocumentSchemaAdmission.Definition> ordinaryContainer,
            DocumentSchemaAdmission.Resolver ordinary, DocumentSchemaAdmission.Limits limits,
            DocumentAdmissionReservations reservations, Runnable control) {
        Objects.requireNonNull(member); Objects.requireNonNull(sources); Objects.requireNonNull(ordinaryContainer);
        Objects.requireNonNull(ordinary); Objects.requireNonNull(limits); Objects.requireNonNull(control);
        if (member.getPartsCount() > limits.maxFragments() || sources.size() > member.getPartsCount())
            throw new IllegalArgumentException("Composite resolution exceeds member bounds");
        var result = new DocumentCompositeSchemaResolution(member, List.copyOf(sources), ordinary, limits, reservations, control);
        boolean delivered = false;
        try {
            result.load(ordinaryContainer);
            result.active(); delivered = true;
            return result;
        } catch (DocumentAdmissionResources.ReservationFailure failure) {
            throw failure.original;
        } finally { if (!delivered) result.close(); }
    }

    private void load(Optional<DocumentSchemaAdmission.Definition> ordinaryContainer) {
        active();
        for (var source : sources) {
            var ordinals = source.targetOrdinals();
            if (ordinals.isEmpty()) throw new IllegalArgumentException("Composite source has no target parts");
            for (int ordinal : ordinals) {
                active();
                if (ordinal < 0 || ordinal >= member.getPartsCount() || !member.getParts(ordinal).hasHistoricalReuse()
                        || routes.putIfAbsent(ordinal, source) != null)
                    throw new IllegalArgumentException("Composite historical routes overlap or differ from member");
            }
            acceptContainer(source.container());
        }
        boolean hasOrdinary = false;
        for (int ordinal = 0; ordinal < member.getPartsCount(); ordinal++) {
            active();
            var part = member.getParts(ordinal);
            if (part.hasHistoricalReuse() && !routes.containsKey(ordinal))
                throw new IllegalArgumentException("Composite historical route is missing");
            if (part.hasUpload() || part.hasReuse()) hasOrdinary = true;
        }
        if (hasOrdinary && ordinaryContainer.isEmpty())
            throw new IllegalArgumentException("Ordinary parts require their container definition");
        ordinaryContainer.ifPresent(this::acceptContainer);
        if (container == null) throw new IllegalArgumentException("Composite container definition is missing");
        for (var source : sources) {
            for (var reference : source.selectedReferences()) addReference(reference);
            source.selectedArtifacts().forEach(this::addArtifact);
        }
        // For ordinary-only members this records the container too. For mixed members
        // equality above permits sharing its exact retained reference and bytes.
        if (sources.isEmpty()) recordDefinition(container);
    }

    private void acceptContainer(DocumentSchemaAdmission.Definition definition) {
        active();
        if (container == null) container = definition;
        else if (!container.equals(definition))
            throw new IllegalArgumentException("Composite container definitions differ");
    }

    public DocumentSchemaAdmission.Definition container() { active(); return container; }

    @Override public DocumentSchemaAdmission.Definition select(DocumentSchemaAdmission.Selection occurrence) {
        active();
        int ordinal = occurrence.ordinal();
        if (ordinal < 0 || ordinal >= member.getPartsCount() || member.getParts(ordinal).hasEmpty())
            throw new IllegalArgumentException("Composite occurrence is outside a payload part");
        var retained = routes.get(ordinal);
        if (retained != null) return retained.select(occurrence);
        if (ordinarySelections.containsKey(occurrence)) throw new IllegalArgumentException("Composite occurrence selected more than once");
        var definition = Objects.requireNonNull(ordinary.select(occurrence), "Unresolved ordinary schema definition");
        active();
        if (!definition.metadata().getTypeUrl().equals(occurrence.typeUrl()))
            throw new IllegalArgumentException("Ordinary definition type URL differs from occurrence");
        try { recordDefinition(definition); }
        catch (DocumentAdmissionResources.ReservationFailure failure) { throw failure.original; }
        ordinarySelections.put(occurrence, definition);
        return definition;
    }

    private void recordDefinition(DocumentSchemaAdmission.Definition definition) {
        active();
        var metadata = definition.metadata();
        var key = new DocumentPayloadCheck.SchemaKey(metadata.getTypeUrl(), metadata.getArtifactSha256());
        var previous = definitions.get(key);
        if (previous != null) {
            if (!previous.equals(definition)) throw new IllegalArgumentException("Conflicting ordinary schema definition");
            return;
        }
        var compilation = metadata.getCompilation();
        if (compilation.hasSourceArtifactSha256() != definition.source().isPresent())
            throw new IllegalArgumentException("Ordinary source bytes differ from metadata presence");
        var encoded = resources.retain(DocumentSchemaAssetCodec.encodeOwned(metadata, resources, this::active));
        var sourceHash = compilation.hasSourceArtifactSha256() ? Optional.of(compilation.getSourceArtifactSha256()) : Optional.<String>empty();
        addReference(new DocumentSchemaAdmission.Reference(metadata.getTypeUrl(), metadata.getArtifactSha256(),
                DocumentSchemaAssetCodec.CODEC, DocumentSchemaAssetCodec.VERSION, encoded.sha256(), sourceHash));
        addArtifact(metadata.getArtifactSha256(), definition.descriptors()); addArtifact(encoded.sha256(), encoded.bytes());
        sourceHash.ifPresent(hash -> addArtifact(hash, definition.source().orElseThrow()));
        definitions.put(key, definition);
    }

    private void addReference(DocumentSchemaAdmission.Reference reference) {
        active();
        var key = new DocumentPayloadCheck.SchemaKey(reference.typeUrl(), reference.descriptorSha256());
        var existing = references.get(key);
        if (existing != null && !existing.equals(reference)) throw new IllegalArgumentException("Conflicting composite schema reference");
        if (existing == null && references.size() >= limits.maxBindings())
            throw new IllegalArgumentException("Composite schema binding limit exceeded");
        references.put(key, reference);
    }

    private void addArtifact(String hash, ByteString bytes) {
        active();
        var existing = artifacts.get(hash);
        if (existing != null) {
            if (!existing.equals(bytes)) throw new IllegalArgumentException("Conflicting composite artifact bytes");
            return;
        }
        if (artifacts.size() >= 64 || bytes.size() > limits.maxRetainedBytes() - assetBytes)
            throw new IllegalArgumentException("Composite retained asset limit exceeded");
        artifacts.put(hash, bytes); assetBytes += bytes.size();
    }

    /** Exact per-source evidence and whole-member union, not command/policy/authorization fencing. */
    public void requireComplete(DocumentSchemaAssessment.View assessment) throws com.google.protobuf.InvalidProtocolBufferException {
        active();
        if (!member.equals(assessment.request().member()) || !container.equals(assessment.request().container()))
            throw new IllegalArgumentException("Composite assessment member or container differs");
        for (var source : sources) source.requireSelectedComplete(assessment);
        try { requireOrdinaryOccurrences(assessment); }
        catch (DocumentAdmissionResources.ReservationFailure failure) { throw failure.original; }
        if (!new HashSet<>(assessment.references()).equals(new HashSet<>(references.values()))
                || !assessment.artifacts().equals(artifacts))
            throw new IllegalArgumentException("Composite assessment differs from exact schema union");
        active();
    }

    private void requireOrdinaryOccurrences(DocumentSchemaAssessment.View assessment)
            throws com.google.protobuf.InvalidProtocolBufferException {
        var actual = new HashSet<DocumentSchemaAdmission.Selection>();
        for (var root : assessment.roots()) {
            active();
            if (routes.containsKey(root.ordinal())) continue;
            var encoded = root.encoded();
            var evidence = DocumentRootSchemaEvidenceCodec.decode(encoded.codec(), encoded.version(), encoded.bytes(),
                    encoded.sha256(), resources, this::active);
            if (!root.locator().equals(evidence.getRoot()))
                throw new IllegalArgumentException("Composite ordinary root differs from evidence");
            for (var occurrence : evidence.getOccurrencesList()) {
                active();
                var steps = occurrence.getStepsList();
                var boundary = steps.getLast().getAnyBoundary();
                var selection = new DocumentSchemaAdmission.Selection(root.ordinal(), root.locator(), boundary.getTypeUrl(),
                        steps.subList(0, steps.size() - 1), boundary.getValueSha256(), boundary.getValueSizeBytes());
                if (!actual.add(selection)) throw new IllegalArgumentException("Duplicate composite ordinary occurrence");
                var selected = ordinarySelections.get(selection);
                if (selected == null) throw new IllegalArgumentException("Composite ordinary occurrences differ from resolver selections");
                if (!boundary.hasResolved() || !boundary.getResolved().getArtifactSha256().equals(selected.metadata().getArtifactSha256())
                        || !boundary.getResolved().getSchema().equals(selected.metadata().getSchema()))
                    throw new IllegalArgumentException("Composite ordinary occurrence uses a different definition");
            }
        }
        if (!ordinarySelections.keySet().equals(actual))
            throw new IllegalArgumentException("Composite ordinary occurrences differ from resolver selections");
    }

    private void active() {
        if (closed) throw new IllegalStateException("Composite schema resolution is closed");
        if (Thread.currentThread().isInterrupted()) throw new java.util.concurrent.CancellationException("Composite resolution interrupted");
        control.run();
    }

    @Override public void close() {
        if (closed) return;
        closed = true; container = null; routes.clear(); references.clear(); artifacts.clear();
        definitions.clear(); ordinarySelections.clear(); resources.close();
    }
}
