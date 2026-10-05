package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.DocumentSchemaRootLocator;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.RepositoryResolvedSchema;
import ai.protomolt.proto.repo.v1.RepositorySchemaAsset;
import ai.protomolt.proto.repo.v1.RepositorySchemaAssetReference;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrenceStep;
import com.google.protobuf.Any;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Optional selected decoding of retained historical content. Uses exact retained schemas,
 * never registry latest. This is not a new validation verdict or expanded JSON contract.
 * Raw preservation remains available independently through HistoricalDocumentRepository.
 */
public interface HistoricalMaterializationRepository {
    /**
     * Unknown selection returns NOT_FOUND; corrupt required retained data returns DATA_LOSS.
     * Capacity refusal returns RESOURCE_EXHAUSTED. No failure falls back to raw delivery.
     */
    Result readMaterialized(RepositoryCaller caller, NodeAddress address, UUID revision,
            Selection selection, Limits limits, RepositoryReadControl control);

    /** Digests select canonical retained evidence, never caller-supplied replacement paths. */
    record Selection(int revisionOrdinal, String rootSha256, String pathSha256) {
        public Selection {
            if (revisionOrdinal < 0 || revisionOrdinal >= 10000)
                throw new IllegalArgumentException("Historical fragment ordinal outside member bound");
            requireDigest(rootSha256); requireDigest(pathSha256);
        }
    }

    /**
     * Serialized-byte and traversal ceilings, not total parsed heap or SQL snapshot bounds.
     * Hosts also bound provider reads, snapshot allocation, concurrency and shared reservations.
     * Decoded bytes include evidence, fragment and each traversed Any value including parents.
     */
    record Limits(long maxFragmentBytes, long maxEvidenceBytes, long maxRetainedBytes,
            int maxReferences, long maxDecodedBytes, int maxBoundaries) {
        public Limits {
            if (maxFragmentBytes < 1 || maxFragmentBytes > 256L * 1024 * 1024
                    || maxEvidenceBytes < 1 || maxEvidenceBytes > 16L * 1024 * 1024
                    || maxRetainedBytes < 1 || maxRetainedBytes > 64L * 1024 * 1024
                    || maxReferences < 1 || maxReferences > 64
                    || maxDecodedBytes < 1 || maxDecodedBytes > 256L * 1024 * 1024
                    || maxBoundaries < 1 || maxBoundaries > 101)
                throw new IllegalArgumentException("Invalid historical materialization limits");
        }
    }

    /** Exact occurrence identity, with immutable steps preceding the selected Any boundary. */
    record Occurrence(int revisionOrdinal, DocumentSchemaRootLocator root, String typeUrl,
            List<RepositorySchemaOccurrenceStep> prefix, String valueSha256, long valueSizeBytes) {
        public Occurrence {
            Objects.requireNonNull(root); Objects.requireNonNull(typeUrl);
            prefix = List.copyOf(prefix); requireDigest(valueSha256);
            if (revisionOrdinal < 0 || revisionOrdinal >= 10000 || valueSizeBytes < 0)
                throw new IllegalArgumentException("Invalid historical occurrence identity");
        }
    }

    /** Borrowed content and request identity; keep the owning Result open through consumption. */
    record View(Selection selection, Any original, DynamicMessage value,
            RepositoryResolvedSchema schema, Occurrence occurrence, Definition definition) {
        public View {
            Objects.requireNonNull(selection); Objects.requireNonNull(original); Objects.requireNonNull(value);
            Objects.requireNonNull(schema); Objects.requireNonNull(occurrence);
            Objects.requireNonNull(definition);
        }
    }

    /**
     * Exact retained descriptor bytes and verified compiler provenance for the selected boundary.
     * The artifact is a complete FileDescriptorSet, not regenerated from linked Java descriptors.
     * Source hashes describe verified retained source; source bytes are not included here.
     * Borrowed until the owning result closes; carries no new validation or authorization grant.
     */
    record Definition(RepositorySchemaAsset metadata, ByteString descriptorArtifact,
            RepositorySchemaAssetReference reference, ByteString metadataArtifact) {
        public Definition {
            Objects.requireNonNull(metadata); Objects.requireNonNull(descriptorArtifact);
            Objects.requireNonNull(reference); Objects.requireNonNull(metadataArtifact);
        }
    }

    interface Result extends AutoCloseable {
        /**
         * Rechecks current READ before each exposure. Reauthorize at final delivery by calling
         * this again. Previously returned Java objects cannot be revoked; do not retain them
         * beyond close. Access after close fails. Close releases owned bytes and revision pins.
         */
        View view(RepositoryReadControl control);
        @Override void close();
    }

    private static void requireDigest(String value) {
        if (value == null || !value.matches("[0-9a-f]{64}"))
            throw new IllegalArgumentException("Expected lowercase SHA-256 digest");
    }
}
