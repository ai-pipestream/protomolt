package ai.protomolt.proto.repo.accession.external;

import static ai.protomolt.proto.repo.accession.external.ExternalLedgerStore.sha256Hex;

import ai.protomolt.proto.repo.external.v1.ExternalPartDigest;
import ai.protomolt.proto.repo.external.v1.ExternalProcessingIdentity;
import ai.protomolt.proto.repo.semantic.v1.EmbeddingSetKey;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.RepositorySchemaAssetReference;
import ai.protomolt.proto.repo.v2.DocumentManifest;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.GetDocumentResponse;
import ai.protomolt.proto.repo.v2.GetManifestRequest;
import ai.protomolt.proto.repo.v2.GetManifestResponse;
import ai.protomolt.proto.repo.v2.GetSchemaDescriptorRequest;
import ai.protomolt.proto.repo.v2.GetSchemaDescriptorResponse;
import ai.protomolt.proto.repo.v2.ListRevisionsRequest;
import ai.protomolt.proto.repo.v2.ListRevisionsResponse;
import ai.protomolt.proto.repo.v2.PartData;
import ai.protomolt.proto.repo.v2.PartManifestEntry;
import ai.protomolt.proto.repo.v2.PartSelection;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartState;
import ai.protomolt.proto.repo.v2.ReadDocumentRequest;
import ai.protomolt.proto.repo.v2.ReadDocumentResponse;
import ai.protomolt.proto.repo.v2.RevisionSelector;
import ai.protomolt.proto.repo.v2.RevisionValidation;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Reads source documents through {@code DocumentService} on the caller's authority:
 * verified exact-revision reads with streamed part digests, tombstone checks, and the
 * read, mutation and reconcile access checks the coordinator applies before touching a
 * ledger.
 */
final class ExternalSourceAccess {

    private static final PartSelector CORE_SELECTOR =
            PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE).build();

    private static final PartSelection CORE_SELECTION =
            PartSelection.newBuilder().addSelectors(CORE_SELECTOR).build();

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    private final ExternalLedgerStore ledgers;

    ExternalSourceAccess(DocumentServiceGrpc.DocumentServiceBlockingStub documents,
            ExternalLedgerStore ledgers) {
        this.documents = documents;
        this.ledgers = ledgers;
    }

    ExternalPublicationCoordinator.VerifiedSourceSnapshot inspectSourceRevision(
            NodeAddress sourceAddress, long sourceRevision) {
        ExternalPublicationCoordinates.requireSourceAddress(sourceAddress);
        if (sourceRevision <= 0L) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("sourceRevision must be > 0")
                    .asRuntimeException();
        }
        RevisionSelector exact = RevisionSelector.newBuilder()
                .setRevision(sourceRevision)
                .build();

        GetManifestResponse manifestResp = documents.getManifest(
                GetManifestRequest.newBuilder()
                        .setAddress(sourceAddress)
                        .setRevision(exact)
                        .build());
        DocumentManifest manifest = manifestResp.getManifest();
        if (manifest.getTombstone()) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Source revision " + sourceRevision + " is a tombstone")
                    .asRuntimeException();
        }

        Iterator<ReadDocumentResponse> stream = documents.readDocument(
                ReadDocumentRequest.newBuilder()
                        .setAddress(sourceAddress)
                        .setRevision(exact)
                        .build());

        DocumentManifest streamedManifest = null;
        boolean sawComplete = false;
        // Digest each part as its chunks arrive; a part's bytes are never held in memory.
        Map<PartSelector, StreamedPartDigest> digests = new HashMap<>();
        while (stream.hasNext()) {
            ReadDocumentResponse frame = stream.next();
            if (frame.hasManifest()) {
                streamedManifest = frame.getManifest();
            } else if (frame.hasPartData()) {
                PartData data = frame.getPartData();
                StreamedPartDigest digest = digests.computeIfAbsent(
                        data.getSelector(), ignored -> new StreamedPartDigest());
                if (digest.length != data.getOffset()) {
                    throw Status.DATA_LOSS
                            .withDescription("Non-contiguous part stream offset")
                            .asRuntimeException();
                }
                digest.update(data.getData());
            } else if (frame.hasComplete()) {
                sawComplete = true;
            }
        }
        if (streamedManifest == null || !sawComplete) {
            throw Status.DATA_LOSS
                    .withDescription("ReadDocument ended without manifest and ReadComplete")
                    .asRuntimeException();
        }

        List<ExternalPartDigest> verifiedParts = new ArrayList<>();
        for (PartManifestEntry entry : streamedManifest.getPartsList()) {
            if (entry.getState() != PartState.PART_STATE_PRESENT) {
                continue;
            }
            StreamedPartDigest digest = digests.computeIfAbsent(
                    entry.getSelector(), ignored -> new StreamedPartDigest());
            if (digest.length != entry.getSizeBytes()) {
                throw Status.DATA_LOSS
                        .withDescription("Streamed part byte length mismatch on " + entry.getSelector())
                        .asRuntimeException();
            }
            String actualSha = digest.sha256Hex();
            if (!actualSha.equals(entry.getSha256())) {
                throw Status.DATA_LOSS
                        .withDescription("Streamed part SHA-256 mismatch on " + entry.getSelector())
                        .asRuntimeException();
            }
            verifiedParts.add(toExternalPartDigest(entry.getSelector(), actualSha, digest.length));
        }

        Set<String> retainedSchemas = collectRetainedSchemaDigests(manifestResp.getValidation());
        List<String> sortedSchemas = new ArrayList<>(retainedSchemas);
        sortedSchemas.sort(Comparator.naturalOrder());
        for (String schemaDigest : sortedSchemas) {
            GetSchemaDescriptorResponse schemaResp = documents.getSchemaDescriptor(
                    GetSchemaDescriptorRequest.newBuilder()
                            .setAddress(sourceAddress)
                            .setRevision(exact)
                            .setDescriptorSha256(schemaDigest)
                            .build());
            String verifiedDigest = sha256Hex(schemaResp.getDescriptorSet().toByteArray());
            if (!verifiedDigest.equals(schemaDigest)) {
                throw Status.DATA_LOSS
                        .withDescription("Retained schema descriptor digest mismatch: " + schemaDigest)
                        .asRuntimeException();
            }
        }

        GetDocumentResponse coreAtRevision = documents.getDocument(
                GetDocumentRequest.newBuilder()
                        .setAddress(sourceAddress)
                        .setRevision(exact)
                        .setSelection(CORE_SELECTION)
                        .build());
        DocumentSecurity security = coreAtRevision.getDocument()
                .getOwnership()
                .getSecurity();

        return new ExternalPublicationCoordinator.VerifiedSourceSnapshot(
                sourceAddress,
                sourceRevision,
                verifiedParts,
                sortedSchemas,
                security);
    }

    /** Running SHA-256 and byte count of one streamed part. */
    private static final class StreamedPartDigest {
        private final MessageDigest sha256;
        private long length;

        private StreamedPartDigest() {
            try {
                sha256 = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException impossible) {
                throw new IllegalStateException("SHA-256 unavailable", impossible);
            }
        }

        private void update(ByteString chunk) {
            for (java.nio.ByteBuffer buffer : chunk.asReadOnlyByteBufferList()) {
                sha256.update(buffer);
            }
            length += chunk.size();
        }

        private String sha256Hex() {
            return HexFormat.of().formatHex(sha256.digest());
        }
    }

    DocumentSecurity verifyCallerSourceAccessForMutation(NodeAddress sourceAddress) {
        ListRevisionsResponse revisions = documents.listRevisions(
                ListRevisionsRequest.newBuilder()
                        .setAddress(sourceAddress)
                        .setPageSize(1)
                        .build());
        if (revisions.getRevisionsCount() == 0) {
            throw Status.NOT_FOUND
                    .withDescription("Source document has no revisions")
                    .asRuntimeException();
        }
        long latestRev = revisions.getRevisions(0).getRevision();
        if (revisions.getRevisions(0).getTombstone()) {
            return DocumentSecurity.getDefaultInstance();
        }
        GetDocumentResponse latestCore = documents.getDocument(
                GetDocumentRequest.newBuilder()
                        .setAddress(sourceAddress)
                        .setRevision(RevisionSelector.newBuilder().setRevision(latestRev))
                        .setSelection(CORE_SELECTION)
                        .build());
        return latestCore.getDocument().getOwnership().getSecurity();
    }

    /** Access rules of the latest live source revision, and that revision. */
    record SourceAccessRules(long sourceRevision, DocumentSecurity security) {
    }

    /**
     * Reads the access rules a purge must leave on the ledger: those of the source's latest live
     * revision, or, when the latest revision is a tombstone, of the live revision before it. The
     * read is made as the caller, so a caller who may not read the source is refused by the
     * repository.
     *
     * @throws StatusRuntimeException FAILED_PRECONDITION once the source is purged, or when no
     *         live revision is within reach of the latest
     */
    SourceAccessRules accessRulesBeforePurge(NodeAddress sourceAddress) {
        ListRevisionsResponse revisions;
        try {
            revisions = documents.listRevisions(ListRevisionsRequest.newBuilder()
                    .setAddress(sourceAddress)
                    .setPageSize(2)
                    .build());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                throw Status.FAILED_PRECONDITION
                        .withDescription("Source document is already purged; a purge is prepared "
                                + "before PurgeDocument removes it")
                        .asRuntimeException();
            }
            throw e;
        }
        for (var summary : revisions.getRevisionsList()) {
            if (summary.getTombstone()) {
                continue;
            }
            GetDocumentResponse live = documents.getDocument(GetDocumentRequest.newBuilder()
                    .setAddress(sourceAddress)
                    .setRevision(RevisionSelector.newBuilder().setRevision(summary.getRevision()))
                    .setSelection(CORE_SELECTION)
                    .build());
            return new SourceAccessRules(
                    summary.getRevision(), live.getDocument().getOwnership().getSecurity());
        }
        throw Status.FAILED_PRECONDITION
                .withDescription("Source document has no live revision next to its latest one; "
                        + "there are no access rules to prepare the purge with")
                .asRuntimeException();
    }

    /** Whether the source's latest revision is a tombstone: the document is deleted now. */
    boolean isCurrentlyTombstoned(NodeAddress sourceAddress) {
        ListRevisionsResponse revisions = documents.listRevisions(
                ListRevisionsRequest.newBuilder()
                        .setAddress(sourceAddress)
                        .setPageSize(1)
                        .build());
        return revisions.getRevisionsCount() > 0 && revisions.getRevisions(0).getTombstone();
    }

    DocumentSecurity verifyTombstoneRevision(
            NodeAddress sourceAddress, long tombstoneRevision) {
        GetManifestResponse manifestResp = documents.getManifest(
                GetManifestRequest.newBuilder()
                        .setAddress(sourceAddress)
                        .setRevision(RevisionSelector.newBuilder()
                                .setRevision(tombstoneRevision))
                        .build());
        if (!manifestResp.getManifest().getTombstone()) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Source revision " + tombstoneRevision
                            + " is not a tombstone")
                    .asRuntimeException();
        }
        NodeAddress ledgercheck = sourceAddress;
        ListRevisionsResponse revs = documents.listRevisions(
                ListRevisionsRequest.newBuilder()
                        .setAddress(ledgercheck)
                        .setPageSize(2)
                        .build());
        for (var summary : revs.getRevisionsList()) {
            if (!summary.getTombstone()) {
                GetDocumentResponse prior = documents.getDocument(
                        GetDocumentRequest.newBuilder()
                                .setAddress(sourceAddress)
                                .setRevision(RevisionSelector.newBuilder()
                                        .setRevision(summary.getRevision()))
                                .setSelection(CORE_SELECTION)
                                .build());
                return prior.getDocument().getOwnership().getSecurity();
            }
        }
        return DocumentSecurity.getDefaultInstance();
    }

    /**
     * Requires that the source document no longer exists. Handling a purge deletes the external
     * projection and fences the ledger for good, so it runs only once the repository has
     * actually removed the document; a caller-supplied purge ID proves nothing by itself.
     *
     * @throws StatusRuntimeException FAILED_PRECONDITION while the document still has
     *         revisions, or the repository's own refusal when the caller may not ask
     */
    void requireSourcePurged(NodeAddress sourceAddress) {
        try {
            documents.listRevisions(ListRevisionsRequest.newBuilder()
                    .setAddress(sourceAddress)
                    .setPageSize(1)
                    .build());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return;
            }
            throw e;
        }
        throw Status.FAILED_PRECONDITION
                .withDescription("Source document still exists; a purge is handled only after "
                        + "PurgeDocument has removed it")
                .asRuntimeException();
    }

    DocumentSecurity authorizeReadOrReconcile(
            NodeAddress sourceAddress, String bindingId) {
        try {
            ListRevisionsResponse revs = documents.listRevisions(
                    ListRevisionsRequest.newBuilder()
                            .setAddress(sourceAddress)
                            .setPageSize(1)
                            .build());
            if (revs.getRevisionsCount() > 0 && !revs.getRevisions(0).getTombstone()) {
                GetDocumentResponse latest = documents.getDocument(
                        GetDocumentRequest.newBuilder()
                                .setAddress(sourceAddress)
                                .setRevision(RevisionSelector.newBuilder()
                                        .setRevision(revs.getRevisions(0).getRevision()))
                                .setSelection(CORE_SELECTION)
                                .build());
                return latest.getDocument().getOwnership().getSecurity();
            }
            return DocumentSecurity.getDefaultInstance();
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
                throw e;
            }
            NodeAddress ledgerAddr = ExternalPublicationCoordinates.ledgerAddress(
                    sourceAddress.getAccountId(), bindingId, sourceAddress);
            Optional<ExternalLedgerStore.LoadedLedger> retained = ledgers.loadOptionalLedger(ledgerAddr, sourceAddress);
            if (retained.isPresent() && retained.get().ledger().getSourcePurged()) {
                return retained.get().security();
            }
            throw e;
        }
    }

    boolean canCallerReadSource(NodeAddress sourceAddress) {
        try {
            documents.listRevisions(ListRevisionsRequest.newBuilder()
                    .setAddress(sourceAddress)
                    .setPageSize(1)
                    .build());
            return true;
        } catch (StatusRuntimeException failure) {
            Status.Code code = failure.getStatus().getCode();
            if (code == Status.Code.PERMISSION_DENIED || code == Status.Code.NOT_FOUND) {
                return false;
            }
            throw failure;
        }
    }

    static void verifyDeclaredProcessingMatchesSource(
            ExternalProcessingIdentity declared,
            ExternalPublicationCoordinator.VerifiedSourceSnapshot verifiedSource) {
        if (declared == null) {
            return;
        }
        if (declared.getSourcePartDigestsCount() > 0) {
            Map<String, ExternalPartDigest> actualByKey = new HashMap<>();
            for (ExternalPartDigest part : verifiedSource.partDigests()) {
                actualByKey.put(part.getPartValue() + ":" + part.getSubKey(), part);
            }
            for (ExternalPartDigest claimed : declared.getSourcePartDigestsList()) {
                ExternalPartDigest actual = actualByKey.get(
                        claimed.getPartValue() + ":" + claimed.getSubKey());
                if (actual == null
                        || !actual.getSha256().equals(claimed.getSha256())
                        || actual.getSizeBytes() != claimed.getSizeBytes()) {
                    throw Status.FAILED_PRECONDITION
                            .withDescription("Declared source_part_digests mismatch verified source revision "
                                    + verifiedSource.sourceRevision())
                            .asRuntimeException();
                }
            }
        }
        if (declared.getRetainedSchemaSha256SCount() > 0) {
            Set<String> actualSchemas = Set.copyOf(verifiedSource.retainedSchemaSha256s());
            for (String claimedSchema : declared.getRetainedSchemaSha256SList()) {
                if (!actualSchemas.contains(claimedSchema)) {
                    throw Status.FAILED_PRECONDITION
                            .withDescription("Declared retained_schema_sha256 '" + claimedSchema
                                    + "' is not retained on source revision "
                                    + verifiedSource.sourceRevision())
                            .asRuntimeException();
                }
            }
        }
    }

    static ExternalPartDigest toExternalPartDigest(
            PartSelector selector, String sha256, long sizeBytes) {
        if (selector.hasPart()) {
            ai.protomolt.proto.repo.v1.DocumentPart v1Part = switch (selector.getPart()) {
                case DOCUMENT_PART_CORE -> ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_CORE;
                case DOCUMENT_PART_BLOBS -> ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_BLOBS;
                case DOCUMENT_PART_PARSED -> ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_PARSED;
                default -> throw new IllegalArgumentException("Unexpected direct part: " + selector);
            };
            return ExternalPartDigest.newBuilder()
                    .setPart(v1Part)
                    .setSubKey("")
                    .setSha256(sha256)
                    .setSizeBytes(sizeBytes)
                    .build();
        }
        if (selector.hasSegmentationId()) {
            return ExternalPartDigest.newBuilder()
                    .setPart(ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_CHUNKS)
                    .setSubKey(selector.getSegmentationId())
                    .setSha256(sha256)
                    .setSizeBytes(sizeBytes)
                    .build();
        }
        if (selector.hasEmbeddingSet()) {
            EmbeddingSetKey key = selector.getEmbeddingSet();
            return ExternalPartDigest.newBuilder()
                    .setPart(ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_EMBEDDINGS)
                    .setSubKey(key.getSegmentationId() + ":" + key.getEmbeddingSetId())
                    .setSha256(sha256)
                    .setSizeBytes(sizeBytes)
                    .build();
        }
        throw new IllegalArgumentException("Unrecognized PartSelector: " + selector);
    }

    static Set<String> collectRetainedSchemaDigests(RevisionValidation validation) {
        Set<String> digests = new LinkedHashSet<>();
        walkSchemaRefs(validation, digests);
        return digests;
    }

    static void walkSchemaRefs(Message message, Set<String> digests) {
        if (message instanceof RepositorySchemaAssetReference ref) {
            digests.add(ref.getDescriptorSha256());
            return;
        }
        for (Map.Entry<Descriptors.FieldDescriptor, Object> entry :
                message.getAllFields().entrySet()) {
            if (entry.getKey().getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                continue;
            }
            if (entry.getKey().isRepeated()) {
                for (Object elem : (List<?>) entry.getValue()) {
                    walkSchemaRefs((Message) elem, digests);
                }
            } else {
                walkSchemaRefs((Message) entry.getValue(), digests);
            }
        }
    }
}
