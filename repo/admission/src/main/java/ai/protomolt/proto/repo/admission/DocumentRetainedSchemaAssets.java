package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.repo.v1.RepositorySchemaAsset;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;

/**
 * Attempt-local retained schema reader. Not thread-safe and not a revision reader.
 * The host selects authenticated metadata/references, scopes the reader and enforces
 * access on every call. No registry, latest-version lookup or compiler fallback exists.
 */
final class DocumentRetainedSchemaAssets {
    private static final int MAX_SOURCE_BYTES = 16 * 1024 * 1024;

    @FunctionalInterface
    interface Reader {
        /** Empty is authoritative absence. Bound allocations before returning; propagate access and I/O failures. */
        Optional<ByteString> read(String artifactSha256);
    }

    /** Storage identities supplied by an authenticated revision reader, not an authorization grant. */
    record Reference(String typeUrl, String descriptorSha256, String metadataCodec, int metadataVersion,
                     String metadataSha256, Optional<String> sourceSha256) {
        Reference {
            Objects.requireNonNull(typeUrl); Objects.requireNonNull(descriptorSha256);
            Objects.requireNonNull(metadataCodec); Objects.requireNonNull(metadataSha256);
            Objects.requireNonNull(sourceSha256);
        }
    }

    record Limits(int maxBindings, long maxRetainedBytes, ClosedDescriptorSet.Limits descriptorLimits) {
        Limits {
            if (maxBindings < 1 || maxRetainedBytes < 1)
                throw new IllegalArgumentException("positive retained schema limits required");
            Objects.requireNonNull(descriptorLimits, "descriptorLimits");
        }
    }

    static final class LimitExceeded extends IllegalArgumentException {
        LimitExceeded(String message) { super(message); }
    }

    static final class DataLoss extends DocumentSchemaAdmission.DataLoss {
        DataLoss(String message) { super(message); }
        DataLoss(String message, Throwable cause) { super(message, cause); }
    }

    private static final class ControlFailure extends RuntimeException {
        private final RuntimeException original;
        ControlFailure(RuntimeException original) { this.original = original; }
    }

    private static final class ReadBatch {
        final Map<String, ByteString> artifacts = new HashMap<>();
        long bytes;
    }

    private final Reader reader;
    private final Limits limits;
    private final DocumentAdmissionReservations reservations;
    private final Map<DocumentPayloadCheck.SchemaKey, DocumentSchemaAssetBinding> bindings = new HashMap<>();
    private final Map<String, ByteString> artifacts = new HashMap<>();
    private long retainedBytes;

    DocumentRetainedSchemaAssets(Reader reader, Limits limits) {
        this(reader, limits, null);
    }

    /** Optional canonical scratch accounting; the reader still owns supplied artifact bytes. */
    DocumentRetainedSchemaAssets(Reader reader, Limits limits, DocumentAdmissionReservations reservations) {
        this.reader = Objects.requireNonNull(reader, "reader");
        this.limits = Objects.requireNonNull(limits, "limits");
        this.reservations = reservations;
    }

    /**
     * Verify a recorded association against canonical metadata, exact descriptors and
     * optional source bytes. Source integrity is not proof of source format, compiler
     * execution or policy. Access control is required even when every byte is cached.
     */
    DocumentSchemaAssetBinding resolve(Reference reference, Runnable control) {
        Objects.requireNonNull(reference, "reference");
        Objects.requireNonNull(control, "control");
        active(control);
        requireHash(reference.descriptorSha256());
        requireHash(reference.metadataSha256());
        reference.sourceSha256().ifPresent(DocumentRetainedSchemaAssets::requireHash);
        if (!DocumentSchemaAssetCodec.CODEC.equals(reference.metadataCodec())
                || reference.metadataVersion() != DocumentSchemaAssetCodec.VERSION)
            throw new DataLoss("unsupported retained schema metadata encoding");
        var batch = new ReadBatch();
        var metadataBytes = load(reference.metadataSha256(), DocumentSchemaAssetCodec.MAX_BYTES, false, batch, control);
        final RepositorySchemaAsset metadata;
        try {
            metadata = reservations == null
                    ? DocumentSchemaAssetCodec.decode(reference.metadataCodec(), reference.metadataVersion(),
                            metadataBytes, reference.metadataSha256(), guarded(control))
                    : DocumentSchemaAssetCodec.decode(reference.metadataCodec(), reference.metadataVersion(),
                            metadataBytes, reference.metadataSha256(), reservations, guarded(control));
        } catch (ControlFailure failure) {
            throw failure.original;
        } catch (InvalidProtocolBufferException | IllegalArgumentException | ValidationResult.ValidationException failure) {
            throw new DataLoss("invalid retained schema metadata encoding", failure);
        }
        var source = metadata.getCompilation().hasSourceArtifactSha256()
                ? Optional.of(metadata.getCompilation().getSourceArtifactSha256()) : Optional.<String>empty();
        if (!metadata.getTypeUrl().equals(reference.typeUrl())
                || !metadata.getArtifactSha256().equals(reference.descriptorSha256())
                || !source.equals(reference.sourceSha256()))
            throw new DataLoss("retained schema association differs from metadata");
        var bound = resolve(metadata, batch, control);
        source.ifPresent(hash -> load(hash, MAX_SOURCE_BYTES, false, batch, control));
        active(control);
        commit(bound, batch);
        return bound;
    }

    /**
     * For already authenticated metadata. Does not establish source retention; use the
     * reference overload for persisted associations. Control enforces current access
     * and cancellation before lookup and delivery. Bounds cover serialized artifacts,
     * not linked descriptors or decoded metadata memory, which the host must budget.
     */
    DocumentSchemaAssetBinding resolve(RepositorySchemaAsset metadata, Runnable control) {
        Objects.requireNonNull(metadata, "metadata");
        Objects.requireNonNull(control, "control");
        active(control);
        var batch = new ReadBatch();
        var bound = resolve(metadata, batch, control);
        active(control);
        commit(bound, batch);
        return bound;
    }

    private DocumentSchemaAssetBinding resolve(RepositorySchemaAsset metadata, ReadBatch batch, Runnable control) {
        var key = new DocumentPayloadCheck.SchemaKey(metadata.getTypeUrl(), metadata.getArtifactSha256());
        var cached = bindings.get(key);
        if (cached != null) {
            if (!cached.metadata().equals(metadata)) throw new DataLoss("conflicting retained schema metadata");
            active(control);
            return cached;
        }
        if (bindings.size() >= limits.maxBindings())
            throw new LimitExceeded("retained schema binding count exceeds limit");
        if (metadata.getSerializedSize() > DocumentSchemaAssetCodec.MAX_BYTES)
            throw new DataLoss("invalid retained schema metadata");
        requireHash(metadata.getArtifactSha256());
        var bytes = load(key.artifactSha256(), limits.descriptorLimits().maxBytes(), true, batch, control);
        try {
            return DocumentSchemaAssetBinding.bind(metadata, bytes, limits.descriptorLimits(), reservations, guarded(control));
        } catch (ControlFailure failure) {
            throw failure.original;
        } catch (ClosedDescriptorSet.LimitExceededException failure) {
            throw failure;
        } catch (IllegalArgumentException failure) {
            throw new DataLoss("invalid retained schema artifact or metadata: " + key.artifactSha256(), failure);
        }
    }

    private ByteString load(String hash, int maxBytes, boolean descriptor, ReadBatch batch, Runnable control) {
        active(control);
        var bytes = artifacts.get(hash);
        if (bytes == null) bytes = batch.artifacts.get(hash);
        boolean newlyRead = bytes == null;
        if (newlyRead) {
            if (limits.maxRetainedBytes() - retainedBytes - batch.bytes < 1)
                throw new LimitExceeded("retained schema byte count exceeds limit");
            bytes = Objects.requireNonNull(reader.read(hash), "reader result")
                    .orElseThrow(() -> new DataLoss("required retained schema artifact is missing: " + hash));
            active(control);
        }
        if (bytes.isEmpty()) throw new DataLoss("retained schema artifact is empty");
        if (bytes.size() > maxBytes) {
            if (descriptor) throw new ClosedDescriptorSet.LimitExceededException("descriptor artifact exceeds byte limit");
            throw new DataLoss("retained schema asset exceeds format byte limit");
        }
        if (newlyRead) {
            if (bytes.size() > limits.maxRetainedBytes() - retainedBytes - batch.bytes)
                throw new LimitExceeded("retained schema byte count exceeds limit");
            if (!DocumentSchemaOccurrences.sha256(bytes, () -> active(control)).equals(hash))
                throw new DataLoss("retained schema artifact digest mismatch: " + hash);
            batch.artifacts.put(hash, bytes);
            batch.bytes += bytes.size();
        }
        active(control);
        return bytes;
    }

    private void commit(DocumentSchemaAssetBinding bound, ReadBatch batch) {
        artifacts.putAll(batch.artifacts);
        retainedBytes += batch.bytes;
        bindings.put(new DocumentPayloadCheck.SchemaKey(bound.metadata().getTypeUrl(), bound.metadata().getArtifactSha256()), bound);
    }

    private static void requireHash(String hash) {
        if (!hash.matches("[0-9a-f]{64}")) throw new DataLoss("invalid retained schema artifact identity");
    }

    private static Runnable guarded(Runnable control) {
        return () -> {
            try { active(control); }
            catch (RuntimeException failure) { throw new ControlFailure(failure); }
        };
    }

    private static void active(Runnable control) {
        if (Thread.currentThread().isInterrupted()) throw new CancellationException("retained schema read interrupted");
        control.run();
    }
}
