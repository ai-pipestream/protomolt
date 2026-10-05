package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.descriptors.MessageWireBudget;
import ai.protomolt.proto.repo.v1.*;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;

/**
 * Owned decoding of one recorded typed occurrence. No registry lookup or admission verdict.
 * The host authenticates the member, retained row and associations against a pinned revision,
 * bounds reader allocations, and rechecks current READ before delivery. Raw preservation uses
 * the raw reader instead. Inputs must remain stable during this call; returned bytes are private.
 */
public final class DocumentSchemaMaterialization {
    private static final int MIB = 1024 * 1024;
    private DocumentSchemaMaterialization() {}

    /** Both digests identify canonical retained evidence, not caller-supplied replacement paths. */
    public record Selection(String rootSha256, String pathSha256) {
        public Selection { hash(rootSha256); hash(pathSha256); }
    }
    /** A host-authenticated retained row for the selected complete-member ordinal. */
    public record Root(String locatorSha256, String fragmentSha256, long fragmentSize,
            DocumentSchemaAdmission.EncodedEvidence evidence) {
        public Root { hash(locatorSha256); hash(fragmentSha256); Objects.requireNonNull(evidence);
            if (fragmentSize < 0) throw new IllegalArgumentException("negative fragment size"); }
    }
    public record Input(DocumentPublicationMember member, int ordinal, ByteString fragment, Root root,
            DocumentSchemaAdmission.Reference container, List<DocumentSchemaAdmission.Reference> references) {
        public Input { Objects.requireNonNull(member); Objects.requireNonNull(fragment); Objects.requireNonNull(root);
            Objects.requireNonNull(container); Objects.requireNonNull(references); }
    }
    /**
     * Decoded bytes count evidence, fragment and each traversed Any value (including parents).
     * Reservations cover serialized copies and decoded-input allowances, not JVM object overhead.
     * The host separately bounds concurrent parsed heap. Wire values are capped at one million
     * per fragment/boundary and depth at 64; descriptors at 16 MiB/256 files/4096 edges/depth 64.
     */
    public record Limits(long maxFragmentBytes, long maxEvidenceBytes, long maxRetainedBytes,
            int maxReferences, long maxDecodedBytes, int maxBoundaries) {
        public Limits {
            if (maxFragmentBytes < 1 || maxFragmentBytes > 256L * MIB || maxEvidenceBytes < 1 || maxEvidenceBytes > 16L * MIB
                    || maxRetainedBytes < 1 || maxRetainedBytes > 64L * MIB || maxReferences < 1 || maxReferences > 64
                    || maxDecodedBytes < 1 || maxDecodedBytes > 256L * MIB || maxBoundaries < 1 || maxBoundaries > 101)
                throw new IllegalArgumentException("invalid materialization limits");
        }
    }
    public static final class LimitExceeded extends IllegalArgumentException {
        LimitExceeded(String message) { super(message); }
    }
    public static final class DataLoss extends IllegalStateException {
        DataLoss(String message) { super(message); }
        DataLoss(String message, Throwable cause) { super(message, cause); }
    }
    private static final class HostFailure extends RuntimeException {
        final RuntimeException original;
        HostFailure(RuntimeException original) { this.original = original; }
    }

    /** Not thread-safe. Close after the last consumer; extracted messages outliving close need host ownership. */
    public static final class Result implements AutoCloseable {
        private DocumentAnyMaterialization.Decoded decoded;
        private List<ByteString> inputs;
        private final DocumentAdmissionResources resources;
        private final List<DocumentAdmissionReservations.Lease> decodeLeases;
        private Result(DocumentAnyMaterialization.Decoded decoded, List<ByteString> inputs,
                DocumentAdmissionResources resources, List<DocumentAdmissionReservations.Lease> decodeLeases) {
            this.decoded = decoded; this.inputs = List.copyOf(inputs); this.resources = resources;
            this.decodeLeases = decodeLeases;
        }
        public Any original() { return open().original(); }
        public DynamicMessage value() { return open().value(); }
        public RepositoryResolvedSchema schema() { return open().schema(); }
        public DocumentSchemaAdmission.Selection occurrence() { return open().occurrence(); }
        private DocumentAnyMaterialization.Decoded open() {
            if (decoded == null) throw new IllegalStateException("Materialization is closed");
            return decoded;
        }
        @Override public void close() {
            if (decoded == null) return;
            decoded = null; inputs = null;
            for (int i = decodeLeases.size() - 1; i >= 0; i--) decodeLeases.get(i).close();
            resources.close();
        }
    }

    public static Result read(Input input, Selection selection, DocumentSchemaAdmission.Reader reader,
            Limits limits, DocumentAdmissionReservations reservations, Runnable control) {
        Objects.requireNonNull(input); Objects.requireNonNull(selection); Objects.requireNonNull(reader);
        Objects.requireNonNull(limits); Objects.requireNonNull(reservations); Objects.requireNonNull(control);
        var resources = new DocumentAdmissionResources(reservations);
        var leases = new ArrayList<DocumentAdmissionReservations.Lease>();
        boolean transferred = false;
        Runnable active = () -> {
            if (Thread.currentThread().isInterrupted()) throw new CancellationException("Materialization interrupted");
            try { control.run(); } catch (RuntimeException failure) { throw new HostFailure(failure); }
        };
        try {
            active.run();
            bound(input.fragment().size(), limits.maxFragmentBytes(), "fragment bytes");
            bound(input.root().evidence().bytes().size(), limits.maxEvidenceBytes(), "evidence bytes");
            bound(input.references().size() + 1L, limits.maxReferences(), "schema references");
            var references = new ArrayList<>(input.references()); references.add(input.container());
            for (var reference : references) {
                active.run();
                try { DocumentSchemaAdmission.Reference.fromProto(reference.toProto(), active); }
                catch (IllegalArgumentException | ai.protomolt.proto.validate.ValidationResult.ValidationException failure) {
                    throw new DataLoss("invalid retained schema association", failure);
                }
            }
            var member = input.member();
            if (member.getPartsCount() > 10000 || member.getSerializedSize() > MIB
                    || input.ordinal() < 0 || input.ordinal() >= member.getPartsCount())
                throw new IllegalArgumentException("selected member ordinal or member shape exceeds bounds");
            var part = member.getParts(input.ordinal());
            if (!part.hasUpload() && !part.hasReuse()) throw new DataLoss("selected part has no payload");
            if (!input.root().locatorSha256().equals(selection.rootSha256()))
                throw new DataLoss("selected root differs from retained row");
            var fragment = resources.copy(input.fragment(), active);
            long size = part.hasUpload() ? part.getUpload().getSizeBytes() : part.getReuse().getObject().getSizeBytes();
            String sha = part.hasUpload() ? part.getUpload().getSha256() : part.getReuse().getObject().getSha256();
            if (fragment.size() != size || size != input.root().fragmentSize()
                    || !sha.equals(input.root().fragmentSha256()) || !sha.equals(DocumentSchemaOccurrences.sha256(fragment, active)))
                throw new DataLoss("selected fragment differs from command or retained row");
            var encoded = input.root().evidence();
            var evidenceBytes = resources.copy(encoded.bytes(), active);
            bound(evidenceBytes.size(), limits.maxDecodedBytes(), "decoded evidence bytes");
            leases.add(resources.reserve(evidenceBytes.size()));
            final DocumentRootSchemaEvidence evidence;
            try {
                evidence = DocumentRootSchemaEvidenceCodec.decode(encoded.codec(), encoded.version(), evidenceBytes,
                        encoded.sha256(), resources, active);
                try (var root = DocumentSchemaEvidenceCodec.encodeOwned(evidence.getRoot(), resources, active)) {
                    if (!root.value().sha256().equals(selection.rootSha256()))
                        throw new DataLoss("retained root digest differs from evidence");
                }
            } catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
                throw new DataLoss("invalid retained root evidence", failure);
            }
            RepositorySchemaOccurrencePath selected = null;
            for (var path : evidence.getOccurrencesList()) {
                active.run();
                try (var encodedPath = DocumentSchemaEvidenceCodec.encodeOwned(path, resources, active)) {
                    if (encodedPath.value().sha256().equals(selection.pathSha256())) {
                        if (selected != null) throw new DataLoss("ambiguous retained path");
                        selected = path;
                    }
                }
            }
            if (selected == null) throw new DataLoss("selected path is absent from retained evidence");
            long decodedBytes = fragment.size() + (long) evidenceBytes.size();
            int boundaries = 0;
            for (var step : selected.getStepsList()) if (step.hasAnyBoundary()) {
                bound(++boundaries, limits.maxBoundaries(), "decoded boundary count");
                long bytes = step.getAnyBoundary().getValueSizeBytes();
                bound(bytes, limits.maxDecodedBytes() - decodedBytes, "decoded input bytes");
                decodedBytes += bytes;
            }
            bound(decodedBytes, limits.maxDecodedBytes(), "decoded input bytes");
            leases.add(resources.reserve(decodedBytes - evidenceBytes.size()));
            var copies = new HashMap<String, ByteString>();
            long[] retainedBytes = {0};
            var retained = new DocumentRetainedSchemaAssets(hash -> {
                active.run();
                if (copies.containsKey(hash)) return java.util.Optional.of(copies.get(hash));
                final java.util.Optional<ByteString> bytes;
                try { bytes = Objects.requireNonNull(reader.read(hash), "retained reader result"); }
                catch (RuntimeException failure) { throw new HostFailure(failure); }
                return bytes.map(value -> {
                    bound(value.size(), 16L * MIB, "retained artifact bytes");
                    bound(value.size(), limits.maxRetainedBytes() - retainedBytes[0], "retained artifact total");
                    bound(copies.size() + 1L, 64, "retained artifact count");
                    var copy = resources.copy(value, active);
                    copies.put(hash, copy); retainedBytes[0] += copy.size(); return copy;
                });
            }, new DocumentRetainedSchemaAssets.Limits(limits.maxReferences(), limits.maxRetainedBytes(),
                    new ClosedDescriptorSet.Limits(16 * MIB, 256, 4096, 64)), resources);
            var container = retained.resolve(input.container().internal(), active).schema();
            final DocumentAnyRootInventory.Result inventory;
            try {
                inventory = DocumentAnyRootInventory.inspect(container, part.getSlot(), fragment,
                        member.getDestination().getAddress().getDocId(),
                        new DocumentAnyRootInventory.Limits((int) limits.maxFragmentBytes(), 1_000_000, 64, 1024, 10000), active);
            } catch (MessageWireBudget.LimitExceededException | DocumentAnyRootInventory.LimitExceeded failure) {
                throw new LimitExceeded("fragment wire limits exceeded");
            } catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
                throw new DataLoss("invalid retained fragment inventory", failure);
            }
            var decoded = DocumentRetainedPathMaterialization.read(input.ordinal(), inventory, evidence.getRoot(), selected,
                    references, retained, new DocumentRetainedPathMaterialization.Limits(limits.maxDecodedBytes(),
                            limits.maxBoundaries(), limits.maxReferences(),
                            new DocumentAnyMaterialization.Limits((int) limits.maxDecodedBytes(), 1_000_000, 64)), active);
            var inputs = new ArrayList<>(copies.values()); inputs.add(fragment); inputs.add(evidenceBytes);
            active.run();
            var result = new Result(decoded, inputs, resources, leases);
            transferred = true;
            return result;
        } catch (HostFailure failure) {
            throw failure.original;
        } catch (DocumentAdmissionResources.ReservationFailure failure) {
            throw failure.original;
        } catch (DocumentRetainedSchemaAssets.DataLoss failure) {
            throw new DataLoss(failure.getMessage(), failure);
        } catch (DocumentRetainedPathMaterialization.LimitExceeded | DocumentRetainedSchemaAssets.LimitExceeded
                | ClosedDescriptorSet.LimitExceededException failure) {
            throw new LimitExceeded(failure.getMessage());
        } finally {
            if (!transferred) {
                for (int i = leases.size() - 1; i >= 0; i--) leases.get(i).close();
                resources.close();
            }
        }
    }

    private static void bound(long actual, long maximum, String name) {
        if (actual < 0 || actual > maximum) throw new LimitExceeded(name + " exceed materialization limit");
    }
    private static void hash(String value) {
        if (value == null || !value.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("exact SHA-256 digest required");
    }
}
