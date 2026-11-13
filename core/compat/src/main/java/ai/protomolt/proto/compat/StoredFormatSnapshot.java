package ai.protomolt.proto.compat;

import ai.protomolt.proto.descriptors.ApiStability;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.descriptors.MessageFingerprints;
import ai.protomolt.proto.descriptors.StabilityTier;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Snapshot of retained repository and persistence schemas (stored formats), their format
 * versions, compatibility policies, and descriptor closures.
 *
 * @param formats sorted list of retained stored format declarations
 */
@ApiStability(StabilityTier.STABLE)
public record StoredFormatSnapshot(List<StoredFormatEntry> formats) {

    public StoredFormatSnapshot {
        Objects.requireNonNull(formats, "formats");
        List<StoredFormatEntry> copy = new ArrayList<>(formats);
        copy.sort(Comparator.comparing(StoredFormatEntry::formatId));
        formats = List.copyOf(copy);
    }

    /**
     * Indexes the stored formats by their stable {@code formatId}.
     *
     * @return immutable map of formatId to {@link StoredFormatEntry}
     */
    public Map<String, StoredFormatEntry> formatsById() {
        Map<String, StoredFormatEntry> map = new LinkedHashMap<>();
        for (StoredFormatEntry entry : formats) {
            map.put(entry.formatId(), entry);
        }
        return Map.copyOf(map);
    }

    /**
     * A single retained stored format contract.
     *
     * @param formatId           unique stored format identifier (e.g. {@code "repo.document.v1"})
     * @param rootTypeName       fully-qualified protobuf root message name
     * @param formatVersion      monotonically non-decreasing format version number
     * @param stability          stability tier of this stored format
     * @param compatibilityMode  required schema evolution mode (typically {@link CompatibilityMode#BACKWARD} or {@link CompatibilityMode#FULL})
     * @param closureFingerprint canonical descriptor closure fingerprint from {@link DescriptorFingerprints#fingerprint(FileDescriptorSet)}
     * @param pinnedClosureSha256 optional exact SHA-256 pin for immutable container closures (empty when compatible evolution is allowed)
     * @param descriptorClosure  transitive protobuf descriptor closure for the stored root message
     */
    @ApiStability(StabilityTier.STABLE)
    public record StoredFormatEntry(
            String formatId,
            String rootTypeName,
            int formatVersion,
            StabilityTier stability,
            CompatibilityMode compatibilityMode,
            String closureFingerprint,
            String pinnedClosureSha256,
            FileDescriptorSet descriptorClosure) {

        public StoredFormatEntry {
            if (formatId == null || formatId.trim().isEmpty()) {
                throw new IllegalArgumentException("formatId must not be blank");
            }
            formatId = formatId.trim();
            if (rootTypeName == null || rootTypeName.trim().isEmpty()) {
                throw new IllegalArgumentException("rootTypeName must not be blank");
            }
            rootTypeName = rootTypeName.trim();
            if (formatVersion < 1) {
                throw new IllegalArgumentException("formatVersion must be >= 1: " + formatVersion);
            }
            Objects.requireNonNull(stability, "stability");
            Objects.requireNonNull(compatibilityMode, "compatibilityMode");
            Objects.requireNonNull(descriptorClosure, "descriptorClosure");
            closureFingerprint = (closureFingerprint == null || closureFingerprint.isEmpty())
                    ? DescriptorFingerprints.fingerprint(descriptorClosure)
                    : closureFingerprint;
            pinnedClosureSha256 = pinnedClosureSha256 == null ? "" : pinnedClosureSha256.trim();
        }

        /**
         * Creates a stored format entry from a linked root message descriptor.
         *
         * @param formatId          unique stored format identifier
         * @param rootDescriptor    root protobuf message descriptor
         * @param formatVersion     format version number
         * @param stability         stability tier
         * @param compatibilityMode schema compatibility policy
         * @param pinExactClosure   whether to pin the exact serialized closure SHA-256
         * @return the captured stored format entry
         */
        public static StoredFormatEntry fromDescriptor(
                String formatId,
                Descriptor rootDescriptor,
                int formatVersion,
                StabilityTier stability,
                CompatibilityMode compatibilityMode,
                boolean pinExactClosure) {
            Objects.requireNonNull(rootDescriptor, "rootDescriptor");
            FileDescriptorSet closure = DescriptorFingerprints.closure(rootDescriptor);
            String fingerprint = DescriptorFingerprints.fingerprint(closure);
            String pinnedSha = pinExactClosure
                    ? MessageFingerprints.sha256Hex(closure.toByteArray())
                    : "";
            return new StoredFormatEntry(
                    formatId,
                    rootDescriptor.getFullName(),
                    formatVersion,
                    stability,
                    compatibilityMode,
                    fingerprint,
                    pinnedSha,
                    closure);
        }
    }
}
