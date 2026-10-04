package ai.protomolt.proto.repo.admission;

import ai.protomolt.proto.descriptors.ClosedDescriptorSet;
import ai.protomolt.proto.repo.v1.RepositorySchemaAsset;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;

/**
 * Attempt-local retained descriptor reader. Not thread-safe and not a revision reader.
 * The host selects authenticated metadata, scopes the reader and enforces access on
 * every call. No live registry, latest-version lookup or compiler fallback exists.
 */
final class DocumentRetainedSchemaAssets {
    @FunctionalInterface
    interface Reader {
        /**
         * Empty means an authoritative absence; access and I/O failures must propagate.
         * The scoped implementation must bound its read allocation before returning bytes.
         */
        Optional<ByteString> read(String artifactSha256);
    }

    record Limits(int maxBindings, long maxRetainedBytes, ClosedDescriptorSet.Limits descriptorLimits) {
        Limits {
            if (maxBindings < 1 || maxRetainedBytes < 1)
                throw new IllegalArgumentException("positive retained schema limits required");
            Objects.requireNonNull(descriptorLimits, "descriptorLimits");
        }
    }

    static final class DataLoss extends IllegalStateException {
        DataLoss(String message) { super(message); }
        DataLoss(String message, Throwable cause) { super(message, cause); }
    }

    private static final class ControlFailure extends RuntimeException {
        private final RuntimeException original;
        ControlFailure(RuntimeException original) { this.original = original; }
    }

    private final Reader reader;
    private final Limits limits;
    private final Map<DocumentPayloadCheck.SchemaKey, DocumentSchemaAssetBinding> bindings = new HashMap<>();
    private final Map<String, ByteString> artifacts = new HashMap<>();
    private long retainedBytes;

    DocumentRetainedSchemaAssets(Reader reader, Limits limits) {
        this.reader = Objects.requireNonNull(reader, "reader");
        this.limits = Objects.requireNonNull(limits, "limits");
    }

    /**
     * Control must enforce current access as well as cancellation; it runs before
     * lookup and before delivery, including cache hits. Callback failures propagate.
     * The byte bound covers retained serialized artifacts, not linked descriptor memory.
     */
    DocumentSchemaAssetBinding resolve(RepositorySchemaAsset metadata, Runnable control) {
        Objects.requireNonNull(metadata, "metadata");
        Objects.requireNonNull(control, "control");
        active(control);
        var key = new DocumentPayloadCheck.SchemaKey(metadata.getTypeUrl(), metadata.getArtifactSha256());
        var cached = bindings.get(key);
        if (cached != null) {
            if (!cached.metadata().equals(metadata))
                throw new DataLoss("conflicting retained schema metadata");
            active(control);
            return cached;
        }
        if (bindings.size() >= limits.maxBindings())
            throw new IllegalArgumentException("retained schema binding count exceeds limit");
        // Validate metadata before letting it choose a storage key. Binding later also
        // verifies the full rule contract and exact descriptor closure.
        if (metadata.getSerializedSize() > 512 * 1024
                || !metadata.getArtifactSha256().matches("[0-9a-f]{64}"))
            throw new DataLoss("invalid retained schema metadata");
        var bytes = artifacts.get(key.artifactSha256());
        boolean newlyRead = bytes == null;
        if (newlyRead) {
            bytes = Objects.requireNonNull(reader.read(key.artifactSha256()), "reader result")
                    .orElseThrow(() -> new DataLoss("required retained schema artifact is missing: " + key.artifactSha256()));
            active(control);
            if (bytes.size() > limits.maxRetainedBytes() - retainedBytes)
                throw new IllegalArgumentException("retained schema byte count exceeds limit");
            if (bytes.size() > limits.descriptorLimits().maxBytes())
                throw new ClosedDescriptorSet.LimitExceededException("descriptor artifact exceeds byte limit");
            if (!DocumentSchemaOccurrences.sha256(bytes, () -> active(control)).equals(key.artifactSha256()))
                throw new DataLoss("retained schema artifact digest mismatch: " + key.artifactSha256());
        }
        final DocumentSchemaAssetBinding bound;
        try {
            bound = DocumentSchemaAssetBinding.bind(metadata, bytes, limits.descriptorLimits(), () -> {
                try { active(control); }
                catch (RuntimeException failure) { throw new ControlFailure(failure); }
            });
        } catch (ControlFailure failure) {
            throw failure.original;
        } catch (ClosedDescriptorSet.LimitExceededException failure) {
            throw failure;
        } catch (IllegalArgumentException failure) {
            throw new DataLoss("invalid retained schema artifact or metadata: " + key.artifactSha256(), failure);
        }
        active(control);
        if (newlyRead) {
            artifacts.put(key.artifactSha256(), bytes);
            retainedBytes += bytes.size();
        }
        bindings.put(key, bound);
        return bound;
    }

    private static void active(Runnable control) {
        if (Thread.currentThread().isInterrupted()) throw new CancellationException("retained schema read interrupted");
        control.run();
    }
}
