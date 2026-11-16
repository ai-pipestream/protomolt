package ai.protomolt.proto.repo.archive;

import static ai.protomolt.proto.repo.archive.RepositorySinkDescriptors.MAX_DESCRIPTOR_BYTES;
import static ai.protomolt.proto.repo.archive.RepositorySinkDescriptors.decodeMessage;
import static ai.protomolt.proto.repo.archive.RepositorySinkDescriptors.fingerprintOfRawSet;
import static ai.protomolt.proto.repo.archive.RepositorySinkDescriptors.linkDescriptor;
import static ai.protomolt.proto.repo.archive.RepositorySinkDescriptors.readBoundedBytes;
import static ai.protomolt.proto.repo.archive.RepositorySinkRequests.findPartManifestEntry;
import static ai.protomolt.proto.repo.archive.RepositorySinkRequests.findPartSchemaBinding;
import static ai.protomolt.proto.repo.archive.RepositorySinkRequests.formatPartSelector;
import static ai.protomolt.proto.repo.archive.RepositorySinkRequests.requireCompleteNodeAddress;
import static ai.protomolt.proto.repo.archive.RepositorySinkRequests.resolveRequestedVersion;
import static ai.protomolt.proto.repo.archive.RepositorySinkRequests.resolveRevisionSelector;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.emit.renderer.RendererProvider;
import ai.protomolt.proto.emit.v1.RendererCapability;
import ai.protomolt.proto.repo.archive.RepositorySinkOccurrences.ResolvedOccurrenceValue;
import ai.protomolt.proto.repo.archive.RepositorySinkOccurrences.RetainedSourceSchema;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.sink.v1.ArchiveRenditionSource;
import ai.protomolt.proto.repo.sink.v1.ConversionSpec;
import ai.protomolt.proto.repo.sink.v1.DocumentPartSource;
import ai.protomolt.proto.repo.sink.v1.ProcessingStateSource;
import ai.protomolt.proto.repo.sink.v1.PublishRepositorySinkRequest;
import ai.protomolt.proto.repo.sink.v1.RenditionProvenance;
import ai.protomolt.proto.repo.sink.v1.RepositoryRevisionSource;
import ai.protomolt.proto.repo.sink.v1.SinkSource;
import ai.protomolt.proto.repo.sink.v1.SourcePartDigest;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.DocumentManifest;
import ai.protomolt.proto.repo.v2.GetManifestRequest;
import ai.protomolt.proto.repo.v2.GetManifestResponse;
import ai.protomolt.proto.repo.v2.PartManifestEntry;
import ai.protomolt.proto.repo.v2.PartSchemaBinding;
import ai.protomolt.proto.repo.v2.PartSelection;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartState;
import ai.protomolt.proto.repo.v2.ReadDocumentRequest;
import ai.protomolt.proto.repo.v2.ReadDocumentResponse;
import ai.protomolt.proto.repo.v2.RevisionSelector;
import ai.protomolt.proto.repo.v2.RevisionValidation;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Resolves and decodes {@link RepositorySink} sources (processing state, archive rendition or
 * document part) at a frozen revision, and builds or inspects the provenance recorded with a
 * published rendition.
 */
final class RepositorySinkSources {
    static final int MAX_SOURCE_PAYLOAD_BYTES = 64 * 1024 * 1024;

    private final DocumentArchive archive;
    private final RenditionReads reads;
    private final RepositorySinkOccurrences occurrences;

    RepositorySinkSources(
            DocumentArchive archive,
            RenditionReads reads,
            RepositorySinkOccurrences occurrences) {
        this.archive = Objects.requireNonNull(archive, "archive");
        this.reads = Objects.requireNonNull(reads, "reads");
        this.occurrences = Objects.requireNonNull(occurrences, "occurrences");
    }

    ResolvedSource resolveSource(SinkSource source, Long pinnedRevisionOverride) {
        return switch (source.getKindCase()) {
            case PROCESSING_STATE -> resolveProcessingState(source.getProcessingState());
            case REPOSITORY_REVISION -> resolveRepositoryRevision(
                    source.getRepositoryRevision(), pinnedRevisionOverride);
            case KIND_NOT_SET -> throw ArchiveErrors.invalid("source", "must select one source arm");
        };
    }

    ResolvedSource resolveProcessingState(ProcessingStateSource ps) {
        Descriptor rootDescriptor = linkDescriptor(
                ps.getDescriptors(),
                ps.getMessageType(),
                "source.processing_state.descriptors",
                "source.processing_state.message_type");
        FileDescriptorSet closureSet = DescriptorFingerprints.closure(rootDescriptor);
        String closureFingerprint = DescriptorFingerprints.fingerprint(closureSet);
        String suppliedSetFingerprint = fingerprintOfRawSet(
                ps.getDescriptors(), "source.processing_state.descriptors");
        if (!ps.getSchemaFingerprint().equals(closureFingerprint)
                && !ps.getSchemaFingerprint().equals(suppliedSetFingerprint)) {
            throw ArchiveErrors.invalid(
                    "source.processing_state.schema_fingerprint",
                    "supplied fingerprint '"
                            + ps.getSchemaFingerprint()
                            + "' does not match descriptor closure fingerprint '"
                            + closureFingerprint
                            + "'");
        }

        RendererProvider.Cancellation cancellation =
                () -> Context.current().isCancelled() || Thread.currentThread().isInterrupted();
        Message parsed = decodeMessage(ps.getPayload(), rootDescriptor, cancellation);
        ByteString closureBytes = closureSet.toByteString();
        String descriptorSha256 = RenditionReads.sha256Hex(closureBytes);

        List<SourcePartDigest> digests = new ArrayList<>();
        String primaryIdentity = ps.getStepName().isEmpty()
                ? "processing_state:" + rootDescriptor.getFullName()
                : "step:" + ps.getStepName();
        digests.add(SourcePartDigest.newBuilder()
                .setIdentity(primaryIdentity)
                .setSha256(RenditionReads.sha256Hex(ps.getPayload()))
                .setSizeBytes(ps.getPayload().size())
                .build());
        digests.addAll(ps.getInputDigestsList());

        return new ResolvedSource(
                0L,
                rootDescriptor,
                parsed,
                ps.getPayload(),
                closureBytes,
                descriptorSha256,
                rootDescriptor.getFullName(),
                closureFingerprint,
                List.copyOf(digests),
                null,
                null,
                null,
                null,
                "",
                ps.getRunId(),
                ps.getStepName(),
                ps.getInputId());
    }

    ResolvedSource resolveRepositoryRevision(
            RepositoryRevisionSource repoSource, Long pinnedRevisionOverride) {
        return switch (repoSource.getTargetCase()) {
            case ARCHIVE_RENDITION -> resolveArchiveRendition(
                    repoSource.getArchiveRendition(), pinnedRevisionOverride);
            case DOCUMENT_PART -> resolveDocumentPart(
                    repoSource.getDocumentPart(), pinnedRevisionOverride);
            case TARGET_NOT_SET -> throw ArchiveErrors.invalid(
                    "source.repository_revision",
                    "must select archive_rendition or document_part");
        };
    }

    ResolvedSource resolveArchiveRendition(
            ArchiveRenditionSource ars, Long pinnedRevisionOverride) {
        EntryAddress entryAddress = ars.getAddress();
        long requestedVersion = resolveRequestedVersion(ars.getRevision(), pinnedRevisionOverride);
        DocumentArchive.ServedEntry served =
                archive.readEntryDocument(entryAddress, requestedVersion);
        long resolvedVersion = served.served().getManifest().getRevision();
        RenditionDescriptor want = ars.getRendition();

        ReadRenditionRequest readReq = ReadRenditionRequest.newBuilder()
                .setAddress(entryAddress)
                .setVersion(resolvedVersion)
                .setRendition(want.getName())
                .setSubKey(want.getSubKey())
                .build();
        RenditionReads.OpenedRead opened = reads.open(readReq);
        ByteString payloadBytes;
        String payloadSha256;
        RenditionDescriptor storedRendition = opened.header().getRendition();
        try (InputStream body = opened.body()) {
            if (!want.getMediaType().isEmpty()
                    && !storedRendition.getMediaType().isEmpty()
                    && !want.getMediaType().equals(storedRendition.getMediaType())) {
                throw ArchiveErrors.failedPrecondition(
                        "Source rendition media_type is '"
                                + storedRendition.getMediaType()
                                + "', not requested '"
                                + want.getMediaType()
                                + "'");
            }
            payloadBytes = readBoundedBytes(body, MAX_SOURCE_PAYLOAD_BYTES, "source rendition");
            payloadSha256 = opened.header().getSha256();
        } catch (IOException ioFailure) {
            throw Status.INTERNAL
                    .withDescription("Failed to stream source rendition bytes")
                    .withCause(ioFailure)
                    .asRuntimeException();
        }

        RetainedSourceSchema retainedSchema =
                loadArchiveRenditionSchema(served, resolvedVersion, want);
        Descriptor rootDescriptor = linkDescriptor(
                retainedSchema.descriptorSetBytes(),
                retainedSchema.messageType(),
                "retained_descriptors",
                "retained_message_type");
        FileDescriptorSet closureSet = DescriptorFingerprints.closure(rootDescriptor);
        String schemaFingerprint = DescriptorFingerprints.fingerprint(closureSet);
        if (!ars.getExpectedSchemaFingerprint().isEmpty()
                && !ars.getExpectedSchemaFingerprint().equals(schemaFingerprint)) {
            throw ArchiveErrors.failedPrecondition(
                    "Source rendition schema fingerprint '"
                            + schemaFingerprint
                            + "' does not match expected_schema_fingerprint '"
                            + ars.getExpectedSchemaFingerprint()
                            + "'");
        }

        RendererProvider.Cancellation cancellation =
                () -> Context.current().isCancelled() || Thread.currentThread().isInterrupted();
        Message message = decodeMessage(payloadBytes, rootDescriptor, cancellation);
        ByteString closureBytes = closureSet.toByteString();

        SourcePartDigest digest = SourcePartDigest.newBuilder()
                .setIdentity(
                        "rendition:" + ArchiveCoordinates.blobId(want.getName(), want.getSubKey()))
                .setSha256(payloadSha256)
                .setSizeBytes(payloadBytes.size())
                .build();

        return new ResolvedSource(
                resolvedVersion,
                rootDescriptor,
                message,
                payloadBytes,
                closureBytes,
                retainedSchema.descriptorSha256(),
                rootDescriptor.getFullName(),
                schemaFingerprint,
                List.of(digest),
                entryAddress,
                storedRendition,
                null,
                null,
                "",
                "",
                "",
                "");
    }

    RetainedSourceSchema loadArchiveRenditionSchema(
            DocumentArchive.ServedEntry served,
            long resolvedVersion,
            RenditionDescriptor want) {
        if (want.getName().equals(ArchiveCoordinates.PARSED)) {
            SearchMetadata search = served.served().getDocument().getSearchMetadata();
            String encodedDesc = search.getMetadataOrDefault(
                    RepositorySinkProvenanceCodec.PARSED_RETAINED_DESCRIPTORS_METADATA_KEY, "");
            String msgType = search.getMetadataOrDefault(
                    RepositorySinkProvenanceCodec.PARSED_RETAINED_MESSAGE_TYPE_METADATA_KEY, "");
            if (!encodedDesc.isEmpty() && !msgType.isEmpty()) {
                ByteString bytes = RepositorySinkProvenanceCodec.decodeDescriptors(
                        encodedDesc, MAX_DESCRIPTOR_BYTES);
                return new RetainedSourceSchema(
                        msgType, bytes, RenditionReads.sha256Hex(bytes));
            }
            PartSchemaBinding partBinding = findPartSchemaBinding(
                            served.served().getValidation(), ArchiveCoordinates.PARSED_PART)
                    .orElseThrow(() -> ArchiveErrors.failedPrecondition(
                            "Source entry parsed rendition at version "
                                    + resolvedVersion
                                    + " has no retained schema binding"));
            return occurrences.fetchRetainedSchemaFromDocument(
                    served.address(), resolvedVersion, partBinding.getFragmentSchema());
        }

        String blobId = ArchiveCoordinates.blobId(want.getName(), want.getSubKey());
        for (Blob blob : served.served().getDocument().getBlobBag().getBlobs().getBlobList()) {
            if (blob.getBlobId().equals(blobId)) {
                RepositorySinkProvenanceCodec.RetainedBlobSchema retained =
                        RepositorySinkProvenanceCodec.readBlobRetainedSchema(
                                        blob, MAX_DESCRIPTOR_BYTES)
                                .orElseThrow(() -> ArchiveErrors.failedPrecondition(
                                        "Rendition '"
                                                + blobId
                                                + "' at version "
                                                + resolvedVersion
                                                + " does not carry retained authoritative protobuf descriptors;"
                                                + " derived non-protobuf renditions cannot be re-projected as authoritative sources"));
                return new RetainedSourceSchema(
                        retained.messageType(),
                        retained.descriptorSetBytes(),
                        RenditionReads.sha256Hex(retained.descriptorSetBytes()));
            }
        }
        throw ArchiveErrors.notFound(
                "Source entry version " + resolvedVersion + " has no rendition " + blobId);
    }

    ResolvedSource resolveDocumentPart(
            DocumentPartSource dps, Long pinnedRevisionOverride) {
        NodeAddress docAddress = dps.getAddress();
        requireCompleteNodeAddress(docAddress, "source.repository_revision.document_part.address");
        RevisionSelector selector =
                resolveRevisionSelector(dps.getRevision(), pinnedRevisionOverride);

        StreamedPartRead partRead = readDocumentPartBytes(docAddress, selector, dps.getSelector());
        long resolvedRevision = partRead.manifest().getRevision();
        RevisionSelector pinnedSelector = DocumentArchive.revision(resolvedRevision);

        GetManifestResponse manifestResp = archive.documents().getManifest(
                GetManifestRequest.newBuilder()
                        .setAddress(docAddress)
                        .setRevision(pinnedSelector)
                        .build());
        RevisionValidation validation = manifestResp.getValidation();
        PartSchemaBinding partBinding = findPartSchemaBinding(validation, dps.getSelector())
                .orElseThrow(() -> ArchiveErrors.failedPrecondition(
                        "Source document revision "
                                + resolvedRevision
                                + " has no retained fragment schema for "
                                + formatPartSelector(dps.getSelector())));

        RetainedSourceSchema fragmentSchema = occurrences.fetchRetainedSchemaFromDocument(
                docAddress, resolvedRevision, partBinding.getFragmentSchema());
        Descriptor fragmentDescriptor = linkDescriptor(
                fragmentSchema.descriptorSetBytes(),
                fragmentSchema.messageType(),
                "retained_fragment_schema",
                "retained_fragment_type");
        RendererProvider.Cancellation cancellation =
                () -> Context.current().isCancelled() || Thread.currentThread().isInterrupted();
        Message fragmentMessage =
                decodeMessage(partRead.bytes(), fragmentDescriptor, cancellation);

        SourcePartDigest partDigest = SourcePartDigest.newBuilder()
                .setIdentity("part:" + formatPartSelector(dps.getSelector()))
                .setSha256(partRead.entry().getSha256())
                .setSizeBytes(partRead.bytes().size())
                .build();

        Descriptor selectedDescriptor;
        Message selectedMessage;
        ByteString selectedPayload;
        ByteString selectedDescriptorSet;
        String retainedDescriptorSha256;
        List<SourcePartDigest> digests;

        if (dps.getOccurrenceFieldPath().isEmpty()) {
            selectedDescriptor = fragmentDescriptor;
            selectedMessage = fragmentMessage;
            selectedPayload = partRead.bytes();
            selectedDescriptorSet = DescriptorFingerprints.closure(fragmentDescriptor).toByteString();
            retainedDescriptorSha256 = fragmentSchema.descriptorSha256();
            digests = List.of(partDigest);
        } else {
            ResolvedOccurrenceValue occ = occurrences.resolveOccurrenceInFragment(
                    docAddress,
                    resolvedRevision,
                    dps.getSelector(),
                    dps.getOccurrenceFieldPath(),
                    fragmentDescriptor,
                    fragmentMessage,
                    fragmentSchema.descriptorSha256(),
                    validation,
                    cancellation);
            selectedDescriptor = occ.descriptor();
            selectedMessage = occ.message();
            selectedPayload = occ.payloadBytes();
            selectedDescriptorSet = DescriptorFingerprints.closure(selectedDescriptor).toByteString();
            retainedDescriptorSha256 = occ.retainedDescriptorSha256();
            digests = List.of(
                    partDigest,
                    SourcePartDigest.newBuilder()
                            .setIdentity("occurrence:" + dps.getOccurrenceFieldPath())
                            .setSha256(RenditionReads.sha256Hex(selectedPayload))
                            .setSizeBytes(selectedPayload.size())
                            .build());
        }

        String schemaFingerprint = DescriptorFingerprints.fingerprintOf(selectedDescriptor);
        if (!dps.getExpectedSchemaFingerprint().isEmpty()
                && !dps.getExpectedSchemaFingerprint().equals(schemaFingerprint)) {
            throw ArchiveErrors.failedPrecondition(
                    "Source document schema fingerprint '"
                            + schemaFingerprint
                            + "' does not match expected_schema_fingerprint '"
                            + dps.getExpectedSchemaFingerprint()
                            + "'");
        }

        return new ResolvedSource(
                resolvedRevision,
                selectedDescriptor,
                selectedMessage,
                selectedPayload,
                selectedDescriptorSet,
                retainedDescriptorSha256,
                selectedDescriptor.getFullName(),
                schemaFingerprint,
                digests,
                null,
                null,
                docAddress,
                dps.getSelector(),
                dps.getOccurrenceFieldPath(),
                "",
                "",
                "");
    }

    StreamedPartRead readDocumentPartBytes(
            NodeAddress docAddress, RevisionSelector revision, PartSelector partSelector) {
        List<ByteString> chunks = new ArrayList<>();
        DocumentManifest[] manifestHolder = new DocumentManifest[1];
        boolean[] completed = {false};
        try (Context.CancellableContext callContext = Context.current().withCancellation()) {
            callContext.run(() -> {
                Iterator<ReadDocumentResponse> frames = archive.documents().readDocument(
                        ReadDocumentRequest.newBuilder()
                                .setAddress(docAddress)
                                .setRevision(revision)
                                .setSelection(
                                        PartSelection.newBuilder().addSelectors(partSelector))
                                .build());
                long total = 0L;
                while (frames.hasNext()) {
                    ReadDocumentResponse frame = frames.next();
                    if (frame.hasManifest()) {
                        manifestHolder[0] = frame.getManifest();
                    } else if (frame.hasPartData()) {
                        ByteString data = frame.getPartData().getData();
                        if (data.size() > MAX_SOURCE_PAYLOAD_BYTES - total) {
                            throw Status.RESOURCE_EXHAUSTED
                                    .withDescription(
                                            "Source document part exceeds maximum sink input budget of "
                                                    + MAX_SOURCE_PAYLOAD_BYTES
                                                    + " bytes")
                                    .asRuntimeException();
                        }
                        total += data.size();
                        chunks.add(data);
                    } else if (frame.hasComplete()) {
                        completed[0] = true;
                    }
                }
            });
        }
        if (manifestHolder[0] == null || !completed[0]) {
            throw ArchiveErrors.dataLoss("ReadDocument ended before ReadComplete", null);
        }
        PartManifestEntry entry = findPartManifestEntry(manifestHolder[0], partSelector)
                .orElseThrow(() -> ArchiveErrors.notFound(
                        "Source document revision "
                                + manifestHolder[0].getRevision()
                                + " has no part "
                                + formatPartSelector(partSelector)));
        if (entry.getState() != PartState.PART_STATE_PRESENT) {
            throw ArchiveErrors.failedPrecondition(
                    "Source document part "
                            + formatPartSelector(partSelector)
                            + " at revision "
                            + manifestHolder[0].getRevision()
                            + " is "
                            + entry.getState());
        }
        ByteString assembled = ByteString.copyFrom(chunks);
        if (assembled.size() != entry.getSizeBytes()
                || !RenditionReads.sha256Hex(assembled).equals(entry.getSha256())) {
            throw ArchiveErrors.dataLoss(
                    "Source document part bytes do not match manifest digest", null);
        }
        return new StreamedPartRead(manifestHolder[0], entry, assembled);
    }

    RenditionProvenance buildProvenance(
            String operationId,
            ResolvedSource source,
            PublishRepositorySinkRequest request,
            RenditionDescriptor effectiveRendition,
            RendererCapability usedCapability,
            String optionsSha256,
            long bytesRendered,
            long bytesCommitted,
            String outputSha256,
            long destinationVersion,
            boolean replayed,
            Timestamp committedAt) {
        RenditionProvenance.Builder builder = RenditionProvenance.newBuilder()
                .setOperationId(operationId)
                .setSourceRevision(source.frozenSourceRevision())
                .addAllSourceDigests(source.sourceDigests())
                .setSchemaType(source.schemaType())
                .setSchemaFingerprint(source.schemaFingerprint())
                .setRetainedDescriptorSha256(source.descriptorSha256())
                .setOutputMediaType(effectiveRendition.getMediaType())
                .setBytesRendered(bytesRendered)
                .setBytesCommitted(bytesCommitted)
                .setOutputSha256(outputSha256)
                .setDestinationAddress(request.getDestination().getAddress())
                .setDestinationRendition(effectiveRendition)
                .setDestinationVersion(destinationVersion)
                .setReplayed(replayed)
                .setCommittedAt(committedAt);

        if (source.sourceEntry() != null) {
            builder.setSourceEntry(source.sourceEntry());
        }
        if (source.sourceRendition() != null) {
            builder.setSourceRendition(source.sourceRendition());
        }
        if (source.sourceDocument() != null) {
            builder.setSourceDocument(source.sourceDocument());
        }
        if (source.sourcePart() != null) {
            builder.setSourcePart(source.sourcePart());
        }
        if (!source.sourceOccurrenceFieldPath().isEmpty()) {
            builder.setSourceOccurrenceFieldPath(source.sourceOccurrenceFieldPath());
        }
        if (!source.processingRunId().isEmpty()) {
            builder.setProcessingRunId(source.processingRunId());
        }
        if (!source.processingStepName().isEmpty()) {
            builder.setProcessingStepName(source.processingStepName());
        }
        if (!source.processingInputId().isEmpty()) {
            builder.setProcessingInputId(source.processingInputId());
        }

        if (usedCapability != null) {
            ConversionSpec conv = request.getConversion();
            builder.setAuthoritativeProtobuf(false)
                    .setProviderId(usedCapability.getProviderId())
                    .setProviderVersion(usedCapability.getProviderVersion())
                    .setSourceProfile(usedCapability.getSourceProfile())
                    .setOptionsType(usedCapability.getOptionsType())
                    .setOptionsFingerprint(usedCapability.getOptionsFingerprint())
                    .setOptionsSha256(optionsSha256)
                    .setMappingId(conv.getMappingId())
                    .setPolicyId(conv.getPolicyId())
                    .setLossDescription(usedCapability.getLossDescription());
        } else {
            builder.setAuthoritativeProtobuf(true);
        }

        RenditionProvenance built = builder.build();
        DocumentArchive.requireValidView(built);
        return built;
    }

    SourceCurrency inspectSourceCurrency(RenditionProvenance provenance) {
        if (provenance.getSourceRevision() == 0L) {
            return new SourceCurrency(0L, false);
        }
        NodeAddress sourceAddress;
        if (provenance.hasSourceEntry()) {
            sourceAddress = ArchiveCoordinates.address(provenance.getSourceEntry());
        } else if (provenance.hasSourceDocument()) {
            sourceAddress = provenance.getSourceDocument();
        } else {
            return new SourceCurrency(0L, false);
        }
        try {
            GetManifestResponse live = archive.documents().getManifest(
                    GetManifestRequest.newBuilder()
                            .setAddress(sourceAddress)
                            .setRevision(DocumentArchive.current())
                            .build());
            long currentRev = live.getManifest().getRevision();
            return new SourceCurrency(currentRev, currentRev != provenance.getSourceRevision());
        } catch (StatusRuntimeException failure) {
            Status.Code code = failure.getStatus().getCode();
            if (code == Status.Code.NOT_FOUND) {
                return new SourceCurrency(0L, true);
            }
            if (code == Status.Code.PERMISSION_DENIED) {
                return new SourceCurrency(0L, false);
            }
            throw failure;
        }
    }

    record ResolvedSource(
            long frozenSourceRevision,
            Descriptor rootDescriptor,
            Message message,
            ByteString rawPayload,
            ByteString descriptorSetBytes,
            String descriptorSha256,
            String schemaType,
            String schemaFingerprint,
            List<SourcePartDigest> sourceDigests,
            EntryAddress sourceEntry,
            RenditionDescriptor sourceRendition,
            NodeAddress sourceDocument,
            PartSelector sourcePart,
            String sourceOccurrenceFieldPath,
            String processingRunId,
            String processingStepName,
            String processingInputId) {}

    record StreamedPartRead(
            DocumentManifest manifest, PartManifestEntry entry, ByteString bytes) {}

    record SourceCurrency(long currentSourceRevision, boolean sourceStale) {}
}
