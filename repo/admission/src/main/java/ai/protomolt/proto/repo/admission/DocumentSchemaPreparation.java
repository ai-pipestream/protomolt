package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.DocumentRootSchemaEvidence;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;

/** Initial evidence production; only the independent verifier constructs a public proof. */
final class DocumentSchemaPreparation {
    private static final int MIB = 1024 * 1024;
    private DocumentSchemaPreparation() {}

    static DocumentSchemaAdmission.Proof check(DocumentSchemaAdmission.Preparation request,
            DocumentSchemaAdmission.Resolver resolver, DocumentSchemaAdmission.Limits limits, Runnable control)
            throws InvalidProtocolBufferException {
        Objects.requireNonNull(request); Objects.requireNonNull(resolver); Objects.requireNonNull(limits);
        Objects.requireNonNull(control);
        Runnable active = () -> {
            if (Thread.currentThread().isInterrupted()) throw new CancellationException("schema preparation interrupted");
            control.run();
        };
        active.run();
        if (request.commandSha256().size() != 32 || !request.policySha256().matches("[0-9a-f]{64}"))
            throw new IllegalArgumentException("exact command and policy digests required");
        if (request.fragments().size() > limits.maxFragments())
            throw new IllegalArgumentException("member admission count exceeds limit");
        var fragments = Map.copyOf(request.fragments());
        DocumentSchemaAdmission.checkMember(request.member(), fragments, request.requireStructuredRoot(), limits, active);
        var definitions = new Definitions(limits, active);
        var container = definitions.add(request.container());
        var containerKey = key(request.container());
        var evidence = new LinkedHashMap<Integer, List<DocumentSchemaAdmission.EncodedEvidence>>();
        long decodedRemaining = limits.maxDecodedBytes();
        long evidenceBytes = 0;
        int rootCount = 0;
        for (int ordinal = 0; ordinal < request.member().getPartsCount(); ordinal++) {
            active.run();
            var part = request.member().getParts(ordinal);
            if (part.hasEmpty() || (part.getSlot().getPart() != DocumentPart.DOCUMENT_PART_CORE
                    && part.getSlot().getPart() != DocumentPart.DOCUMENT_PART_PARSED)) continue;
            var inventory = DocumentAnyRootInventory.inspect(container.schema(), part.getSlot(), fragments.get(ordinal),
                    request.member().getDestination().getAddress().getDocId(),
                    new DocumentAnyRootInventory.Limits(Integer.MAX_VALUE, 1_000_000, 64, limits.maxRoots(), 10000), active);
            if (inventory.roots().size() > limits.maxRoots() - rootCount)
                throw new IllegalArgumentException("member evidence root limit exceeded");
            rootCount += inventory.roots().size();
            var bundles = new ArrayList<DocumentSchemaAdmission.EncodedEvidence>();
            final int rootOrdinal = ordinal;
            for (var root : inventory.roots()) {
                active.run();
                if (decodedRemaining < 1) throw new IllegalArgumentException("member decoded payload budget exhausted");
                var locator = DocumentSchemaRootProjection.project(inventory, root, active);
                var envelope = root.envelope();
                var selected = definitions.select(resolver, new DocumentSchemaAdmission.Selection(ordinal, locator,
                        envelope.getTypeUrl(), List.of(), DocumentSchemaOccurrences.sha256(envelope.getValue(), active),
                        envelope.getValue().size()));
                var checked = DocumentPayloadCheck.checkContextualAssets(selected, envelope, envelope.getTypeUrl(),
                        DocumentSchemaAdmission.VALIDATOR,
                        new DocumentPayloadCheck.Limits((int) decodedRemaining, 1_000_000, 64, 4096, 65536), active,
                        nested -> definitions.select(resolver, new DocumentSchemaAdmission.Selection(rootOrdinal, locator,
                                nested.typeUrl(), DocumentSchemaOccurrenceProjection.projectSteps(nested.prefix(), definitions.schemas, active),
                                nested.valueSha256(), nested.valueSizeBytes())), DocumentSchemaOccurrences.Limits.DEFAULT);
                decodedRemaining -= checked.payload().decodedBytes();
                if (part.getSlot().getPart() == DocumentPart.DOCUMENT_PART_CORE && request.member().hasStructuredSchema()
                        && !checked.payload().schema().condition().equals(request.member().getStructuredSchema()))
                    throw new IllegalArgumentException("structured root differs from required schema");
                var bundle = DocumentRootSchemaEvidence.newBuilder().setEncodingVersion(1).setRoot(locator)
                        .addAllOccurrences(DocumentSchemaOccurrenceProjection.project(checked.payload(), active)).build();
                var encoded = DocumentRootSchemaEvidenceCodec.encode(bundle, limits.maxEvidenceBytes() - evidenceBytes, active);
                evidenceBytes += encoded.bytes().size();
                bundles.add(new DocumentSchemaAdmission.EncodedEvidence(DocumentRootSchemaEvidenceCodec.CODEC,
                        DocumentRootSchemaEvidenceCodec.VERSION, encoded.bytes(), encoded.sha256()));
            }
            if (!bundles.isEmpty()) evidence.put(ordinal, List.copyOf(bundles));
        }
        var references = new ArrayList<DocumentSchemaAdmission.Reference>();
        definitions.references.forEach((key, reference) -> { if (!key.equals(containerKey)) references.add(reference); });
        active.run();
        // Fresh independent decoding from frozen assets. Registry selection is never repeated on replay.
        var assets = Map.copyOf(definitions.artifacts);
        return DocumentSchemaAdmission.check(new DocumentSchemaAdmission.Request(request.commandSha256(), request.policySha256(),
                request.requireStructuredRoot(), request.member(), fragments, evidence, definitions.references.get(containerKey), references),
                hash -> Optional.ofNullable(assets.get(hash)), limits, active);
    }

    private static DocumentPayloadCheck.SchemaKey key(DocumentSchemaAdmission.Definition definition) {
        return new DocumentPayloadCheck.SchemaKey(definition.metadata().getTypeUrl(), definition.metadata().getArtifactSha256());
    }

    /** Attempt-local assets; failed preparation never returns a partial proof or writes retained storage. */
    private static final class Definitions {
        private final DocumentSchemaAdmission.Limits limits;
        private final Runnable control;
        private final Map<String, ByteString> artifacts = new HashMap<>();
        private final Map<DocumentPayloadCheck.SchemaKey, DocumentSchemaAdmission.Definition> selected = new HashMap<>();
        private final Map<DocumentPayloadCheck.SchemaKey, DocumentSchemaAssetBinding> bindings = new HashMap<>();
        private final Map<DocumentPayloadCheck.SchemaKey, DocumentSchemaBinding> schemas = new HashMap<>();
        private final Map<DocumentPayloadCheck.SchemaKey, DocumentSchemaAdmission.Reference> references = new LinkedHashMap<>();
        private final DocumentRetainedSchemaAssets retained;
        private long bytes;

        Definitions(DocumentSchemaAdmission.Limits limits, Runnable control) {
            this.limits = limits; this.control = control;
            retained = new DocumentRetainedSchemaAssets(hash -> Optional.ofNullable(artifacts.get(hash)),
                    new DocumentRetainedSchemaAssets.Limits(limits.maxBindings(), limits.maxRetainedBytes(),
                            new ClosedDescriptorSet.Limits(16 * MIB, 256, 4096, 64)));
        }

        DocumentSchemaAssetBinding select(DocumentSchemaAdmission.Resolver resolver, DocumentSchemaAdmission.Selection request) {
            control.run();
            var definition = resolver.select(request);
            control.run();
            if (definition == null) throw new IllegalArgumentException("unresolved Any schema definition: " + request.typeUrl());
            if (!definition.metadata().getTypeUrl().equals(request.typeUrl()))
                throw new IllegalArgumentException("resolved schema asset type URL mismatch");
            return add(definition);
        }

        DocumentSchemaAssetBinding add(DocumentSchemaAdmission.Definition definition) {
            control.run();
            var key = key(definition);
            var previous = selected.get(key);
            if (previous != null) {
                if (!previous.equals(definition)) throw new IllegalArgumentException("conflicting definition for one schema identity");
                return bindings.get(key);
            }
            if (selected.size() >= limits.maxBindings()) throw new IllegalArgumentException("schema binding count exceeds limit");
            if (definition.descriptors().isEmpty() || definition.descriptors().size() > 16 * MIB
                    || definition.source().filter(value -> value.isEmpty() || value.size() > 16 * MIB).isPresent())
                throw new IllegalArgumentException("definition artifact exceeds byte limit");
            var encoded = DocumentSchemaAssetCodec.encode(definition.metadata(), control);
            var compilation = definition.metadata().getCompilation();
            if (compilation.hasSourceArtifactSha256() != definition.source().isPresent())
                throw new IllegalArgumentException("source bytes differ from metadata presence");
            var sourceHash = compilation.hasSourceArtifactSha256() ? Optional.of(compilation.getSourceArtifactSha256()) : Optional.<String>empty();
            var reference = new DocumentSchemaAdmission.Reference(key.typeUrl(), key.artifactSha256(),
                    DocumentSchemaAssetCodec.CODEC, DocumentSchemaAssetCodec.VERSION, encoded.sha256(), sourceHash);
            put(key.artifactSha256(), definition.descriptors());
            put(encoded.sha256(), encoded.bytes());
            sourceHash.ifPresent(hash -> put(hash, definition.source().orElseThrow()));
            var bound = retained.resolve(new DocumentRetainedSchemaAssets.Reference(reference.typeUrl(), reference.descriptorSha256(),
                    reference.metadataCodec(), reference.metadataVersion(), reference.metadataSha256(), reference.sourceSha256()), control);
            selected.put(key, definition); bindings.put(key, bound); schemas.put(key, bound.schema()); references.put(key, reference);
            return bound;
        }

        private void put(String digest, ByteString value) {
            control.run();
            var prior = artifacts.get(digest);
            if (prior != null) {
                if (!prior.equals(value)) throw new IllegalArgumentException("conflicting bytes for artifact digest");
                return;
            }
            if (artifacts.size() >= 64 || value.size() > limits.maxRetainedBytes() - bytes)
                throw new IllegalArgumentException("retained artifact union exceeds limit");
            artifacts.put(digest, value); bytes += value.size();
        }
    }
}
