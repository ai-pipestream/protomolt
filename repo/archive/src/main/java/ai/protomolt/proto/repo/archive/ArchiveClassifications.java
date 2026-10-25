package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.asset.bridge.Bridges;
import ai.protomolt.proto.asset.characterize.ByteWindows;
import ai.protomolt.proto.asset.characterize.Characterizer;
import ai.protomolt.proto.asset.characterize.Classifications;
import ai.protomolt.proto.asset.v1.Attribution;
import ai.protomolt.proto.asset.v1.Classification;
import ai.protomolt.proto.asset.v1.ContentProfile;
import ai.protomolt.proto.asset.v1.FormatFact;
import ai.protomolt.proto.asset.v1.ObjectStoreOrigin;
import ai.protomolt.proto.descriptors.MessageFingerprints;
import ai.protomolt.proto.repo.archive.v1.RenditionContent;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.WriteAttribution;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v2.GetDocumentResponse;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Value;
import java.time.Instant;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Format characterization, classification state-machine resolution, and binary
 * protobuf encoding of {@link Classification} and {@link ContentProfile} on v2
 * archive documents.
 */
final class ArchiveClassifications {

    /** Prefix reserved in {@code SearchMetadata.metadata} for archive-owned entry facts. */
    static final String RESERVED_METADATA_PREFIX = "archive.";

    /** {@code SearchMetadata.metadata} key holding the Base64-encoded binary {@link Classification}. */
    static final String CLASSIFICATION_METADATA_KEY = "archive.classification";

    /** {@code SearchMetadata.metadata} key holding the {@code ClassificationState} enum name for filtering. */
    static final String CLASSIFICATION_STATE_METADATA_KEY = "archive.classification_state";

    /** {@code Blob.metadata} struct field holding the Base64-encoded binary {@link ContentProfile}. */
    static final String CONTENT_PROFILE_FIELD = "archive.content_profile";

    private static final Comparator<String> BLOB_SLOT_ORDER = (left, right) -> {
        int nameCompare = ArchiveOperationKeys.compareUtf8(renditionName(left), renditionName(right));
        if (nameCompare != 0) {
            return nameCompare;
        }
        return ArchiveOperationKeys.compareUtf8(subKey(left), subKey(right));
    };

    private ArchiveClassifications() {
    }

    /**
     * Validates an entry's format declaration at the door: {@code validate.v1} rules
     * on the {@link FormatFact} plus the archive door rule that a declaration must
     * carry a non-blank {@code filename} on its active format arm.
     */
    static FormatFact declared(boolean hasDeclared, FormatFact declared) {
        if (!hasDeclared || declared.getFormatCase() == FormatFact.FormatCase.FORMAT_NOT_SET) {
            return null;
        }
        DocumentArchive.requireValid(declared);
        Descriptors.OneofDescriptor arm = declared.getDescriptorForType().getOneofs().getFirst();
        Descriptors.FieldDescriptor active = declared.getOneofFieldDescriptor(arm);
        Message inner = (Message) declared.getField(active);
        Descriptors.FieldDescriptor filename = inner.getDescriptorForType().findFieldByName("filename");
        if (filename != null && ((String) inner.getField(filename)).isBlank()) {
            throw ArchiveErrors.invalid(
                    "declared." + active.getName() + ".filename",
                    "a declaration names its file: declared." + active.getName() + ".filename is required");
        }
        return declared;
    }

    /** Validates an entry's object-store origin at the door when present. */
    static ObjectStoreOrigin origin(boolean hasOrigin, ObjectStoreOrigin origin) {
        if (!hasOrigin || origin.equals(ObjectStoreOrigin.getDefaultInstance())) {
            return null;
        }
        DocumentArchive.requireValid(origin);
        return origin;
    }

    /** Refuses user-supplied metadata keys that collide with the reserved {@code archive.} namespace. */
    static void requireUserMetadataKeys(Map<String, String> metadata) {
        for (String key : metadata.keySet()) {
            if (key.startsWith(RESERVED_METADATA_PREFIX)) {
                throw ArchiveErrors.invalid(
                        "metadata",
                        "keys starting with '" + RESERVED_METADATA_PREFIX
                                + "' are reserved by the archive: " + key);
            }
        }
    }

    /**
     * Resolves an entry's {@link Classification} from the producer's declaration,
     * the primary rendition's byte windows, and the write attribution.
     */
    static Classification classify(
            FormatFact declared,
            ObjectStoreOrigin origin,
            ByteWindows windows,
            String filename,
            WriteAttribution writtenBy,
            Instant now) {
        Characterizer.Identification identification = windows != null && windows.head().length > 0
                ? Characterizer.identify(windows, filename != null ? filename : "")
                : new Characterizer.Identification(null, List.of());
        return Classifications.resolve(
                declared,
                identification,
                attribution(writtenBy),
                origin,
                now);
    }

    /**
     * Retains the stored {@link Classification} (including its {@code classified_at}
     * timestamp) when the primary rendition's bytes and every non-timestamp field of
     * the resolved classification are unchanged, so an unchanged save produces a
     * byte-identical CORE fragment and deduplicates.
     */
    static Classification preserveIfUnchanged(
            Classification resolved, Optional<Classification> stored, boolean primaryUnchanged) {
        if (primaryUnchanged && stored.isPresent()) {
            Classification previous = stored.get();
            if (previous.toBuilder().clearClassifiedAt().build()
                    .equals(resolved.toBuilder().clearClassifiedAt().build())) {
                return previous;
            }
        }
        return resolved;
    }

    static Attribution attribution(WriteAttribution writtenBy) {
        if (writtenBy == null
                || (writtenBy.getModule().isBlank() && writtenBy.getActor().isBlank())) {
            return null;
        }
        return Attribution.newBuilder()
                .setModule(writtenBy.getModule())
                .setActor(writtenBy.getActor())
                .build();
    }

    /** Encodes a validated {@link Classification} as Base64 deterministic protobuf bytes. */
    static String encodeClassification(Classification classification) {
        DocumentArchive.requireValidView(classification);
        return Base64.getEncoder().encodeToString(MessageFingerprints.deterministicBytes(classification));
    }

    /** Decodes a stored Base64 protobuf {@link Classification}, failing with {@code DATA_LOSS} if corrupt. */
    static Classification decodeClassification(String encoded) {
        try {
            return Classification.parseFrom(Base64.getDecoder().decode(encoded));
        } catch (IllegalArgumentException | InvalidProtocolBufferException e) {
            throw ArchiveErrors.dataLoss("Stored entry classification does not decode", e);
        }
    }

    /** Reads the stored {@link Classification} from a document read, if present. */
    static Optional<Classification> storedClassification(GetDocumentResponse read) {
        String encoded = read.getDocument().getSearchMetadata()
                .getMetadataOrDefault(CLASSIFICATION_METADATA_KEY, "");
        if (encoded.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(decodeClassification(encoded));
    }

    /** Encodes a validated {@link ContentProfile} into a {@link Value} for {@code Blob.metadata}. */
    static Value encodeContentProfile(ContentProfile profile) {
        DocumentArchive.requireValidView(profile);
        String encoded = Base64.getEncoder().encodeToString(MessageFingerprints.deterministicBytes(profile));
        return Value.newBuilder().setStringValue(encoded).build();
    }

    /** Decodes a stored {@link ContentProfile} from {@code Blob.metadata}, failing with {@code DATA_LOSS} if corrupt. */
    static ContentProfile decodeContentProfile(Value encoded) {
        if (encoded.getKindCase() != Value.KindCase.STRING_VALUE) {
            throw ArchiveErrors.dataLoss(
                    "Stored rendition content_profile is not a string value: " + encoded.getKindCase(), null);
        }
        try {
            return ContentProfile.parseFrom(Base64.getDecoder().decode(encoded.getStringValue()));
        } catch (IllegalArgumentException | InvalidProtocolBufferException e) {
            throw ArchiveErrors.dataLoss("Stored rendition content_profile does not decode", e);
        }
    }

    /**
     * Selects the primary blob ID of a candidate save: {@code "original"} when
     * present with non-empty bytes, otherwise the first present non-{@code "parsed"},
     * non-bridge-derived rendition in {@code (name, sub_key)} order.
     */
    static Optional<String> primaryBlobId(List<Blob> currentBlobs, Map<String, RenditionContent> written) {
        String best = null;
        for (Blob blob : currentBlobs) {
            String blobId = blob.getBlobId();
            RenditionContent replacement = written.get(blobId);
            long size = replacement != null ? replacement.getData().size() : blob.getSizeBytes();
            if (size <= 0 || !eligiblePrimaryBlobId(blobId)) {
                continue;
            }
            if (blobId.equals(ArchiveCoordinates.ORIGINAL)) {
                return Optional.of(blobId);
            }
            if (best == null || BLOB_SLOT_ORDER.compare(blobId, best) < 0) {
                best = blobId;
            }
        }
        for (Map.Entry<String, RenditionContent> entry : written.entrySet()) {
            String blobId = entry.getKey();
            if (entry.getValue().getData().isEmpty() || !eligiblePrimaryBlobId(blobId)) {
                continue;
            }
            if (blobId.equals(ArchiveCoordinates.ORIGINAL)) {
                return Optional.of(blobId);
            }
            if (best == null || BLOB_SLOT_ORDER.compare(blobId, best) < 0) {
                best = blobId;
            }
        }
        return Optional.ofNullable(best);
    }

    /** Selects the primary {@link Blob} from a committed revision's blobs. */
    static Optional<Blob> primaryBlob(List<Blob> currentBlobs) {
        Optional<String> primaryId = primaryBlobId(currentBlobs, Map.of());
        if (primaryId.isEmpty()) {
            return Optional.empty();
        }
        String target = primaryId.get();
        return currentBlobs.stream().filter(b -> b.getBlobId().equals(target)).findFirst();
    }

    /** Whether an {@code UploadRendition} of {@code uploaded} writes the entry's primary rendition. */
    static boolean isPrimaryUpload(List<Blob> currentBlobs, RenditionDescriptor uploaded) {
        String uploadedId = ArchiveCoordinates.blobId(uploaded.getName(), uploaded.getSubKey());
        if (!eligiblePrimaryBlobId(uploadedId)) {
            return false;
        }
        if (uploadedId.equals(ArchiveCoordinates.ORIGINAL)) {
            return true;
        }
        for (Blob blob : currentBlobs) {
            if (blob.getSizeBytes() <= 0 || !eligiblePrimaryBlobId(blob.getBlobId())) {
                continue;
            }
            if (blob.getBlobId().equals(ArchiveCoordinates.ORIGINAL)) {
                return false;
            }
            if (!blob.getBlobId().equals(uploadedId) && BLOB_SLOT_ORDER.compare(blob.getBlobId(), uploadedId) < 0) {
                return false;
            }
        }
        return true;
    }

    private static boolean eligiblePrimaryBlobId(String blobId) {
        String name = renditionName(blobId);
        return !name.equals(ArchiveCoordinates.PARSED) && !Bridges.derivedName(name);
    }

    static String renditionName(String blobId) {
        int slash = blobId.indexOf('/');
        return slash < 0 ? blobId : blobId.substring(0, slash);
    }

    static String subKey(String blobId) {
        int slash = blobId.indexOf('/');
        return slash < 0 ? "" : blobId.substring(slash + 1);
    }
}
