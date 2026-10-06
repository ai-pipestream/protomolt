package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.repo.v1.DocumentSchemaRootLocator;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrenceStep;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.*;
import java.util.concurrent.CancellationException;

/**
 * Attempt-local exact occurrence resolution, without registry lookup or an inherited verdict.
 * The host authenticates the source revision, retains its read pins and owns reader bytes
 * and parsed heap through this scope and any consumer copying its borrowed definitions.
 * This scope owns validation scratch, not the supplied artifacts or fragment bytes.
 * Not thread-safe; one scope serves one assessment and is then closed.
 */
public final class DocumentRetainedSchemaResolution implements DocumentSchemaAdmission.Resolution {
    private record Root(int ordinal, DocumentSchemaRootLocator locator) {}
    private record Key(int ordinal, DocumentSchemaRootLocator root, String url,
            List<RepositorySchemaOccurrenceStep> prefix, String hash, long size) {
        Key { prefix = List.copyOf(prefix); }
        static Key from(DocumentSchemaAdmission.Selection occurrence) {
            return new Key(occurrence.ordinal(), occurrence.root(), occurrence.typeUrl(), occurrence.prefix(),
                    occurrence.valueSha256(), occurrence.valueSizeBytes());
        }
        Key at(int target) { return new Key(target, root, url, prefix, hash, size); }
    }
    private final DocumentAdmissionResources resources;
    private final Runnable control;
    private final Map<Key, DocumentSchemaAdmission.Definition> index = new HashMap<>();
    private final Map<Root, DocumentSchemaAdmission.EncodedEvidence> roots = new HashMap<>();
    private final Set<Key> used = new HashSet<>();
    private final Set<String> artifactHashes = new HashSet<>();
    private final Set<DocumentSchemaAdmission.Reference> selectedReferences = new HashSet<>();
    private final Set<String> selectedArtifacts = new HashSet<>();
    private List<DocumentSchemaAdmission.Reference> references = List.of();
    private DocumentSchemaAdmission.Definition container;
    private boolean closed;

    private DocumentRetainedSchemaResolution(DocumentAdmissionReservations reservations, Runnable control) {
        resources = new DocumentAdmissionResources(reservations);
        this.control = Objects.requireNonNull(control);
    }

    /**
     * Map each selected target ordinal to its exact source ordinal. Slot identities and
     * occurrence paths remain unchanged. An explicit empty map is valid only when the
     * consumer needs no payload occurrences. No target fragment or write is authorized.
     * Call requireComplete with the new assessment before using its result.
     * Limits bound source loading; the current assessment policy bounds the destination.
     * Source ordinals cannot repeat. The host verifies destination slot and physical identity.
     */
    public static DocumentRetainedSchemaResolution open(DocumentSchemaAdmission.Request source,
            Map<Integer, Integer> targetToSource, DocumentSchemaAdmission.Reader reader,
            DocumentSchemaAdmission.Limits limits, DocumentAdmissionReservations reservations, Runnable control)
            throws InvalidProtocolBufferException {
        Objects.requireNonNull(source); Objects.requireNonNull(targetToSource); Objects.requireNonNull(reader);
        Objects.requireNonNull(limits); Objects.requireNonNull(reservations);
        var result = new DocumentRetainedSchemaResolution(reservations, control);
        boolean delivered = false;
        try {
            result.load(source, targetToSource, reader, limits);
            result.active();
            delivered = true;
            return result;
        } catch (DocumentAdmissionResources.ReservationFailure failure) {
            throw failure.original;
        } finally {
            if (!delivered) result.close();
        }
    }

    private void load(DocumentSchemaAdmission.Request source, Map<Integer, Integer> mapping,
            DocumentSchemaAdmission.Reader reader, DocumentSchemaAdmission.Limits limits) throws InvalidProtocolBufferException {
        active();
        if (source.references().size() >= limits.maxBindings() || source.evidence().size() > limits.maxFragments()
                || source.member().getPartsCount() > limits.maxFragments() || mapping.size() > limits.maxFragments()
                || source.member().getSerializedSize() > 1024 * 1024)
            throw new IllegalArgumentException("retained resolution input exceeds limits");
        mapping = Map.copyOf(mapping);
        var selectedOrdinals = new HashSet<Integer>();
        for (var entry : mapping.entrySet()) {
            active();
            if (entry.getKey() < 0 || entry.getKey() >= 10_000 || entry.getValue() < 0
                    || entry.getValue() >= source.member().getPartsCount()
                    || source.member().getParts(entry.getValue()).hasEmpty()
                    || !selectedOrdinals.add(entry.getValue()))
                throw new IllegalArgumentException("retained resolution ordinal mapping is invalid");
        }
        var bundles = DocumentSchemaAdmission.decodeEvidence(source.evidence(), limits, resources, this::active);
        var sourceRoots = new HashMap<Root, DocumentSchemaAdmission.EncodedEvidence>();
        var occurrences = new HashMap<Key, DocumentPayloadCheck.SchemaKey>();
        var occurrenceSchemas = new HashMap<Key, ai.protomolt.proto.repo.v1.PublicationSchemaCondition>();
        for (var entry : bundles.entrySet()) {
            int ordinal = entry.getKey();
            if (ordinal < 0 || ordinal >= source.member().getPartsCount())
                throw new IllegalArgumentException("replay evidence ordinal outside member");
            for (int i = 0; i < entry.getValue().size(); i++) {
                active();
                var bundle = entry.getValue().get(i);
                if (source.member().getParts(ordinal).hasEmpty())
                    throw new IllegalArgumentException("replay evidence attached to empty slot");
                if (!bundle.getRoot().getSlot().equals(source.member().getParts(ordinal).getSlot()))
                    throw new IllegalArgumentException("replay evidence slot differs from member part");
                if (sourceRoots.putIfAbsent(new Root(ordinal, bundle.getRoot()), source.evidence().get(ordinal).get(i)) != null)
                    throw new IllegalArgumentException("duplicate replay evidence root");
                for (var path : bundle.getOccurrencesList()) {
                    active();
                    var boundary = path.getSteps(path.getStepsCount() - 1).getAnyBoundary();
                    var key = new Key(ordinal, bundle.getRoot(), boundary.getTypeUrl(),
                            path.getStepsList().subList(0, path.getStepsCount() - 1), boundary.getValueSha256(), boundary.getValueSizeBytes());
                    var schema = new DocumentPayloadCheck.SchemaKey(boundary.getTypeUrl(), boundary.getResolved().getArtifactSha256());
                    if (occurrences.putIfAbsent(key, schema) != null)
                        throw new IllegalArgumentException("conflicting replay occurrence selection");
                    occurrenceSchemas.put(key, boundary.getResolved().getSchema());
                }
            }
        }
        var refs = new ArrayList<DocumentSchemaAdmission.Reference>(); refs.add(source.container()); refs.addAll(source.references());
        references = List.copyOf(refs);
        var byKey = new HashSet<DocumentPayloadCheck.SchemaKey>();
        for (var reference : references) {
            active();
            if (!byKey.add(new DocumentPayloadCheck.SchemaKey(reference.typeUrl(), reference.descriptorSha256())))
                throw new IllegalArgumentException("duplicate replay schema association");
            artifactHashes.add(reference.descriptorSha256()); artifactHashes.add(reference.metadataSha256());
            reference.sourceSha256().ifPresent(artifactHashes::add);
        }
        if (artifactHashes.size() > 64) throw new IllegalArgumentException("retained artifact count exceeds limit");
        var loaded = new HashMap<String, ByteString>();
        var retained = new DocumentRetainedSchemaAssets(hash -> {
            var cached = loaded.get(hash);
            if (cached != null) return Optional.of(cached);
            var bytes = Objects.requireNonNull(reader.read(hash));
            bytes.ifPresent(value -> loaded.put(hash, value));
            return bytes;
        }, new DocumentRetainedSchemaAssets.Limits(limits.maxBindings(), limits.maxRetainedBytes(),
                new ClosedDescriptorSet.Limits(16 * 1024 * 1024, 256, 4096, 64)), resources);
        var definitions = new HashMap<DocumentPayloadCheck.SchemaKey, DocumentSchemaAdmission.Definition>();
        for (var reference : references) {
            active();
            var asset = retained.resolve(reference.internal(), this::active);
            definitions.put(new DocumentPayloadCheck.SchemaKey(reference.typeUrl(), reference.descriptorSha256()),
                    new DocumentSchemaAdmission.Definition(asset.metadata(), loaded.get(reference.descriptorSha256()),
                            reference.sourceSha256().map(loaded::get)));
        }
        if (!loaded.keySet().equals(artifactHashes))
            throw new IllegalArgumentException("retained schema assets differ from complete union");
        // Check even unselected source occurrences against authenticated references.
        for (var entry : occurrences.entrySet()) {
            active();
            var definition = definitions.get(entry.getValue());
            if (definition == null || !definition.metadata().getSchema().equals(occurrenceSchemas.get(entry.getKey())))
                throw new IllegalArgumentException("replay occurrence differs from retained schema association");
        }
        var rootsByOrdinal = new HashMap<Integer, List<Root>>();
        sourceRoots.keySet().forEach(root -> rootsByOrdinal.computeIfAbsent(root.ordinal(), ignored -> new ArrayList<>()).add(root));
        var keysByOrdinal = new HashMap<Integer, List<Key>>();
        occurrences.keySet().forEach(key -> keysByOrdinal.computeIfAbsent(key.ordinal(), ignored -> new ArrayList<>()).add(key));
        var referenceByKey = new HashMap<DocumentPayloadCheck.SchemaKey, DocumentSchemaAdmission.Reference>();
        for (var reference : references)
            referenceByKey.put(new DocumentPayloadCheck.SchemaKey(reference.typeUrl(), reference.descriptorSha256()), reference);
        selectedReferences.add(source.container());
        for (var ordinal : mapping.entrySet()) {
            active();
            for (var root : rootsByOrdinal.getOrDefault(ordinal.getValue(), List.of()))
                roots.put(new Root(ordinal.getKey(), root.locator()), sourceRoots.get(root));
            for (var key : keysByOrdinal.getOrDefault(ordinal.getValue(), List.of())) {
                active();
                var schema = occurrences.get(key);
                index.put(key.at(ordinal.getKey()), definitions.get(schema));
                selectedReferences.add(referenceByKey.get(schema));
            }
        }
        for (var reference : selectedReferences) {
            selectedArtifacts.add(reference.descriptorSha256()); selectedArtifacts.add(reference.metadataSha256());
            reference.sourceSha256().ifPresent(selectedArtifacts::add);
        }
        container = definitions.get(new DocumentPayloadCheck.SchemaKey(source.container().typeUrl(), source.container().descriptorSha256()));
    }

    /** Borrowed exact container definition; keep this scope and its artifact owner open. */
    public DocumentSchemaAdmission.Definition container() { active(); return container; }

    @Override public DocumentSchemaAdmission.Definition select(DocumentSchemaAdmission.Selection occurrence) {
        active();
        var key = Key.from(Objects.requireNonNull(occurrence));
        var definition = index.get(key);
        if (definition == null) throw new IllegalArgumentException("candidate occurrence has no exact retained schema selection");
        if (!used.add(key)) throw new IllegalArgumentException("replay occurrence selected more than once");
        return definition;
    }

    /** Verify that the new assessment used every selected occurrence and reproduced its exact evidence. */
    public void requireComplete(DocumentSchemaAssessment.View assessment) {
        active();
        var actual = new HashMap<Root, DocumentSchemaAdmission.EncodedEvidence>();
        for (var root : assessment.roots()) {
            active();
            if (actual.putIfAbsent(new Root(root.ordinal(), root.locator()), root.encoded()) != null)
                throw new IllegalArgumentException("duplicate reassessed evidence root");
        }
        if (!used.equals(index.keySet()) || !actual.equals(roots))
            throw new IllegalArgumentException("reassessed roots or occurrences differ from retained evidence");
        if (!new HashSet<>(assessment.references()).equals(selectedReferences)
                || !assessment.artifacts().keySet().equals(selectedArtifacts))
            throw new IllegalArgumentException("reassessed schema assets differ from selected retained union");
    }

    /** Historical replay additionally requires the whole source artifact/reference union. */
    void requireFullUnion(DocumentSchemaAssessment.View assessment) {
        requireComplete(assessment);
        if (!new HashSet<>(assessment.references()).equals(new HashSet<>(references))
                || !assessment.artifacts().keySet().equals(artifactHashes))
            throw new IllegalArgumentException("reassessed schema assets differ from complete retained union");
    }

    private void active() {
        if (closed) throw new IllegalStateException("Retained schema resolution is closed");
        if (Thread.currentThread().isInterrupted()) throw new CancellationException("retained schema resolution interrupted");
        control.run();
    }
    @Override public void close() {
        if (closed) return;
        closed = true; index.clear(); roots.clear(); used.clear(); references = List.of(); container = null;
        artifactHashes.clear(); selectedReferences.clear(); selectedArtifacts.clear(); resources.close();
    }
}
