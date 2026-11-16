package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.emit.v1.RendererCapability;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.sink.v1.ConversionSpec;
import ai.protomolt.proto.repo.sink.v1.PublishRepositorySinkRequest;
import ai.protomolt.proto.repo.sink.v1.RepositoryRevisionSource;
import ai.protomolt.proto.repo.sink.v1.RepositorySinkJob;
import ai.protomolt.proto.repo.sink.v1.SinkDestination;
import ai.protomolt.proto.repo.sink.v1.SinkSource;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.DocumentManifest;
import ai.protomolt.proto.repo.v2.PartManifestEntry;
import ai.protomolt.proto.repo.v2.PartSchemaBinding;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.RevisionSelector;
import ai.protomolt.proto.repo.v2.RevisionValidation;
import com.google.protobuf.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;

/**
 * Request, capability and address helpers for {@link RepositorySink}: destination rendition rules,
 * revision selectors, manifest lookups and stable entry identifiers.
 */
final class RepositorySinkRequests {
    static final String ENTRY_UUID_KIND = "repository-sink.entry-uuid";

    private RepositorySinkRequests() {}

    static void validateDestinationRenditionRules(PublishRepositorySinkRequest request) {
        SinkDestination dest = request.getDestination();
        RenditionDescriptor rend = dest.getRendition();
        if (!dest.getFilename().isEmpty() && !rend.getName().equals(ArchiveCoordinates.ORIGINAL)) {
            throw ArchiveErrors.invalid(
                    "destination.filename", "describes only the original rendition");
        }
        if (rend.getName().equals(ArchiveCoordinates.PARSED)) {
            if (!rend.getSubKey().isEmpty()) {
                throw ArchiveErrors.invalid(
                        "destination.rendition.sub_key",
                        "the parsed rendition is the document's PARSED part and takes no sub-key");
            }
            if (request.hasConversion()
                    && !RepositorySink.PROTOBUF_MEDIA_TYPE.equals(request.getConversion().getMediaType())) {
                throw ArchiveErrors.invalid(
                        "destination.rendition.name",
                        "non-protobuf converted renditions cannot overwrite the 'parsed' protobuf part");
            }
        }
        if (!request.hasConversion()
                && !rend.getMediaType().isEmpty()
                && !RepositorySink.PROTOBUF_MEDIA_TYPE.equals(rend.getMediaType())) {
            throw ArchiveErrors.invalid(
                    "destination.rendition.media_type",
                    "a publication with no conversion writes protobuf; leave it empty or set '"
                            + RepositorySink.PROTOBUF_MEDIA_TYPE + "'");
        }
        if (request.hasConversion()
                && !rend.getMediaType().isEmpty()
                && !rend.getMediaType().equals(request.getConversion().getMediaType())) {
            throw ArchiveErrors.invalid(
                    "destination.rendition.media_type",
                    "must match conversion.media_type '"
                            + request.getConversion().getMediaType()
                            + "' when specified");
        }
    }

    static RenditionDescriptor effectiveDestinationRendition(
            PublishRepositorySinkRequest request) {
        RenditionDescriptor rend = request.getDestination().getRendition();
        if (!rend.getMediaType().isEmpty()) {
            return rend;
        }
        String inferredMediaType = request.hasConversion()
                ? request.getConversion().getMediaType()
                : RepositorySink.PROTOBUF_MEDIA_TYPE;
        return rend.toBuilder().setMediaType(inferredMediaType).build();
    }

    static Optional<RendererCapability> findCapability(
            RepositorySinkRenderers activeRenderers, String providerId) {
        for (RendererCapability capability : activeRenderers.registry().capabilities()) {
            if (capability.getProviderId().equals(providerId)) {
                return Optional.of(capability);
            }
        }
        return Optional.empty();
    }

    static Optional<String> verifyCapabilityMatchesSpec(
            RendererCapability capability, ConversionSpec spec) {
        if (!capability.getProviderVersion().equals(spec.getProviderVersion())) {
            return Optional.of("conversion.provider_version '" + spec.getProviderVersion()
                    + "' does not match enabled '" + capability.getProviderVersion() + "'");
        }
        if (!capability.getSourceProfile().equals(spec.getSourceProfile())) {
            return Optional.of("conversion.source_profile '" + spec.getSourceProfile()
                    + "' does not match enabled '" + capability.getSourceProfile() + "'");
        }
        if (!capability.getMediaType().equals(spec.getMediaType())) {
            return Optional.of("conversion.media_type '" + spec.getMediaType()
                    + "' does not match enabled '" + capability.getMediaType() + "'");
        }
        if (!capability.getExtension().equals(spec.getExtension())) {
            return Optional.of("conversion.extension '" + spec.getExtension()
                    + "' does not match enabled '" + capability.getExtension() + "'");
        }
        if (!capability.getOptionsType().equals(spec.getOptionsType())) {
            return Optional.of("conversion.options_type '" + spec.getOptionsType()
                    + "' does not match enabled '" + capability.getOptionsType() + "'");
        }
        if (!capability.getOptionsFingerprint().equals(spec.getOptionsFingerprint())) {
            return Optional.of("conversion.options_fingerprint '" + spec.getOptionsFingerprint()
                    + "' does not match enabled '" + capability.getOptionsFingerprint() + "'");
        }
        return Optional.empty();
    }

    static PublishRepositorySinkRequest pinRequestSourceRevision(
            PublishRepositorySinkRequest request, String operationId, long frozenSourceRevision) {
        PublishRepositorySinkRequest.Builder builder =
                request.toBuilder().setOperationId(operationId);
        if (frozenSourceRevision > 0L
                && request.getSource().getKindCase()
                        == SinkSource.KindCase.REPOSITORY_REVISION) {
            RevisionSelector pinned = DocumentArchive.revision(frozenSourceRevision);
            RepositoryRevisionSource repoSource = request.getSource().getRepositoryRevision();
            RepositoryRevisionSource.Builder pinnedRepo = repoSource.toBuilder();
            if (repoSource.hasArchiveRendition()) {
                pinnedRepo.setArchiveRendition(
                        repoSource.getArchiveRendition().toBuilder().setRevision(pinned));
            } else if (repoSource.hasDocumentPart()) {
                pinnedRepo.setDocumentPart(
                        repoSource.getDocumentPart().toBuilder().setRevision(pinned));
            }
            builder.setSource(SinkSource.newBuilder().setRepositoryRevision(pinnedRepo));
        }
        return builder.build();
    }

    static long resolveRequestedVersion(RevisionSelector selector, Long pinnedOverride) {
        if (pinnedOverride != null && pinnedOverride > 0L) {
            return pinnedOverride;
        }
        if (selector != null && selector.hasRevision()) {
            return selector.getRevision();
        }
        return 0L;
    }

    static RevisionSelector resolveRevisionSelector(
            RevisionSelector selector, Long pinnedOverride) {
        if (pinnedOverride != null && pinnedOverride > 0L) {
            return DocumentArchive.revision(pinnedOverride);
        }
        if (selector != null && selector.hasRevision()) {
            return DocumentArchive.revision(selector.getRevision());
        }
        return DocumentArchive.current();
    }

    static void requireCompleteNodeAddress(NodeAddress address, String field) {
        if (address.getAccountId().isBlank()
                || address.getGraphId().isBlank()
                || address.getGraphAddressId().isBlank()
                || address.getDocId().isBlank()) {
            throw ArchiveErrors.invalid(
                    field,
                    "requires non-blank account_id, graph_id, graph_address_id, and doc_id");
        }
    }

    static Optional<PartManifestEntry> findPartManifestEntry(
            DocumentManifest manifest, PartSelector selector) {
        for (PartManifestEntry entry : manifest.getPartsList()) {
            if (entry.getSelector().equals(selector)) {
                return Optional.of(entry);
            }
        }
        return Optional.empty();
    }

    static Optional<PartSchemaBinding> findPartSchemaBinding(
            RevisionValidation validation, PartSelector selector) {
        for (PartSchemaBinding binding : validation.getPartSchemasList()) {
            if (binding.getPart().equals(selector)) {
                return Optional.of(binding);
            }
        }
        return Optional.empty();
    }

    static String formatPartSelector(PartSelector selector) {
        return switch (selector.getSelectorCase()) {
            case PART -> selector.getPart().name();
            case SEGMENTATION_ID -> "chunks:" + selector.getSegmentationId();
            case EMBEDDING_SET -> "embeddings:"
                    + selector.getEmbeddingSet().getSegmentationId()
                    + "/"
                    + selector.getEmbeddingSet().getEmbeddingSetId();
            case SELECTOR_NOT_SET -> "unspecified";
        };
    }

    static String typeNameFromUrl(String typeUrl) {
        int slash = typeUrl.lastIndexOf('/');
        return slash < 0 ? typeUrl : typeUrl.substring(slash + 1);
    }

    static String entryUuid(EntryAddress address) {
        return RepositorySinkKeys.deriveUuidV5(
                        ENTRY_UUID_KIND,
                        address.getAccountId(),
                        address.getArchive() + '/' + address.getEntryId())
                .toString();
    }

    static String storedJobIntentSha(RepositorySinkJob job) {
        return job.getIntentSha256();
    }


    static Timestamp timestamp(Clock clock) {
        Instant instant = clock.instant();
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}
