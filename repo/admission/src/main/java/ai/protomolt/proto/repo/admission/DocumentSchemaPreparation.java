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
        return check(request, resolver, limits, null, null, control);
    }

    static DocumentSchemaAdmission.Proof check(DocumentSchemaAdmission.Preparation request,
            DocumentSchemaAdmission.Resolver resolver, DocumentSchemaAdmission.Limits limits,
            DocumentAdmissionResources resources, DocumentAdmissionResources temporary, Runnable control)
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
        var definitions = new DocumentSchemaDefinitions(limits, resources, active);
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
                if (temporary == null) {
                    var encoded = DocumentRootSchemaEvidenceCodec.encode(bundle, limits.maxEvidenceBytes() - evidenceBytes, active);
                    evidenceBytes += encoded.bytes().size();
                    bundles.add(new DocumentSchemaAdmission.EncodedEvidence(DocumentRootSchemaEvidenceCodec.CODEC,
                            DocumentRootSchemaEvidenceCodec.VERSION, encoded.bytes(), encoded.sha256()));
                } else {
                    var encoded = temporary.retain(DocumentRootSchemaEvidenceCodec.encodeOwned(bundle,
                            limits.maxEvidenceBytes() - evidenceBytes, temporary, active));
                    evidenceBytes += encoded.bytes().size();
                    bundles.add(new DocumentSchemaAdmission.EncodedEvidence(DocumentRootSchemaEvidenceCodec.CODEC,
                            DocumentRootSchemaEvidenceCodec.VERSION, encoded.bytes(), encoded.sha256()));
                }
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
                hash -> Optional.ofNullable(assets.get(hash)), limits, resources, active);
    }

    private static DocumentPayloadCheck.SchemaKey key(DocumentSchemaAdmission.Definition definition) {
        return new DocumentPayloadCheck.SchemaKey(definition.metadata().getTypeUrl(), definition.metadata().getArtifactSha256());
    }

}
