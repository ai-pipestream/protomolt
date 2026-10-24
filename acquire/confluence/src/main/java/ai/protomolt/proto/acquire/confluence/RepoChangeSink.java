package ai.protomolt.proto.acquire.confluence;

import ai.protomolt.proto.acquire.confluence.v1.Attachment;
import ai.protomolt.proto.acquire.confluence.v1.Body;
import ai.protomolt.proto.acquire.confluence.v1.ChangeOperation;
import ai.protomolt.proto.acquire.confluence.v1.ConfluenceChange;
import ai.protomolt.proto.acquire.confluence.v1.ConfluenceEntity;
import ai.protomolt.proto.acquire.confluence.v1.ConfluenceSnapshot;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.DocIdDerivation;
import ai.protomolt.proto.repo.v1.DocIdDerivationMethod;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.DeleteDocumentRequest;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.DocumentTarget;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartWrite;
import ai.protomolt.proto.repo.v2.RevisionConflict;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.WriteAction;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * The dogfood sink: mirrors Confluence content into the protomolt repo
 * service as {@link Document}s over the v2 {@code DocumentService} gRPC API.
 *
 * <p>Entity-to-Document mapping (kept deliberately small; documented here as
 * the contract):</p>
 * <ul>
 *   <li>Identity: {@code doc_id} is a deterministic name-based UUID of
 *   {@code "confluence|<entityId>"} (the repo service's own
 *   {@code UUID.nameUUIDFromBytes} convention), so re-crawls target the same
 *   document address instead of duplicating it. The derivation is stamped on
 *   {@code doc_id_derivation} as SOURCE_DOC_ID.</li>
 *   <li>Pages and blog posts become Documents whose
 *   {@code search_metadata.body} is the first populated body slot (storage,
 *   then atlas_doc_format, view, raw), whose {@code title} /
 *   {@code source_uri} / dates / author come off the entity, and whose
 *   {@code document_type} is {@code "confluence-page"} or
 *   {@code "confluence-blogpost"}.</li>
 *   <li>Attachments carrying content bytes populate {@code blob_bag} with an
 *   inline {@link Blob} ({@code data}, {@code drive_id}, {@code mime_type},
 *   {@code filename}, {@code size_bytes}) and include a {@code DOCUMENT_PART_BLOBS}
 *   {@code WRITE_ACTION_PUT} alongside {@code DOCUMENT_PART_CORE} so the
 *   repository server writes the raw blob to the stack and binds its claim check.
 *   Attachments without content save as metadata-only Documents with only
 *   {@code DOCUMENT_PART_CORE}.</li>
 *   <li>Confluence ids ride in {@code search_metadata.metadata}:
 *   {@code confluence.entity_id}, {@code confluence.space_id},
 *   {@code confluence.version}.</li>
 *   <li>Other entity arms (comments, labels, properties, ...) are skipped:
 *   the repo mirror stores content, not crawl exhaust.</li>
 *   <li>DELETE changes issue a tombstone {@code DeleteDocument} for the derived
 *   {@link NodeAddress}; UPSERTs save against the full intake {@link NodeAddress}
 *   ({@code graph_address_id = datasourceId}, {@code graph_id = "intake:<accountId>"})
 *   with {@link SaveOwner} set to {@link IntakeOrigin} and the crawl cursor
 *   recorded under request metadata key {@link #METADATA_CRAWL_ID}. When an
 *   existing revision is reported via {@link RevisionConflict}, the sink retries
 *   with that revision as {@link ExpectedRevision}.</li>
 * </ul>
 *
 * <p>Failure policy: unlike {@link KafkaChangeSink} this sink is fail-loud.
 * A repo outage is a wiring problem the operator must fix, and the crawler
 * already aborts cleanly on a sink exception; silently dropping documents
 * would leave the mirror lying. Calls block on the calling virtual thread,
 * which is the module-wide contract.</p>
 *
 * <p>{@code snapshot()} markers carry nothing the repo needs; they are
 * logged and dropped.</p>
 */
public final class RepoChangeSink implements ChangeSink, AutoCloseable {

    /** The connector id stamped on saves and ownership. */
    public static final String CONNECTOR_ID = "confluence";

    /** The request metadata key carrying the crawl cursor when non-blank. */
    public static final String METADATA_CRAWL_ID = "crawl_id";

    private static final String DEFAULT_MIME_TYPE = "application/octet-stream";
    private static final String INTAKE_GRAPH_PREFIX = "intake:";
    private static final int MAX_CAS_ATTEMPTS = 4;

    private static final PartWrite PUT_CORE = PartWrite.newBuilder()
            .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
            .setAction(WriteAction.WRITE_ACTION_PUT)
            .build();

    private static final PartWrite PUT_BLOBS = PartWrite.newBuilder()
            .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_BLOBS))
            .setAction(WriteAction.WRITE_ACTION_PUT)
            .build();

    private static final System.Logger LOG = System.getLogger(RepoChangeSink.class.getName());

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    private final String drive;
    private final String accountId;
    private final String datasourceId;
    private final ManagedChannel channel;

    /**
     * A sink over an existing stub; the caller owns the channel and
     * {@link #close()} is a no-op.
     *
     * @param documents the v2 DocumentService blocking stub
     * @param drive the repo drive documents save to
     * @param accountId the owning account on saved documents
     * @param datasourceId the datasource id on saved documents
     */
    public RepoChangeSink(DocumentServiceGrpc.DocumentServiceBlockingStub documents, String drive,
            String accountId, String datasourceId) {
        this(documents, drive, accountId, datasourceId, null);
    }

    private RepoChangeSink(DocumentServiceGrpc.DocumentServiceBlockingStub documents, String drive,
            String accountId, String datasourceId, ManagedChannel channel) {
        this.documents = Objects.requireNonNull(documents, "documents");
        if (drive == null || drive.isBlank()) {
            throw new IllegalArgumentException("drive cannot be null or blank");
        }
        if (accountId == null || accountId.isBlank()) {
            throw new IllegalArgumentException("accountId cannot be null or blank");
        }
        if (datasourceId == null || datasourceId.isBlank()) {
            throw new IllegalArgumentException("datasourceId cannot be null or blank");
        }
        this.drive = drive;
        this.accountId = accountId;
        this.datasourceId = datasourceId;
        this.channel = channel;
    }

    /**
     * Builds the sink from the connector config: a plaintext channel to
     * {@code CONFLUENCE_REPO_TARGET} that this sink owns and closes.
     *
     * @param config the connector config (must have {@code repoEnabled()})
     * @return the ready sink
     */
    public static RepoChangeSink create(ConfluenceConnectorConfig config) {
        Objects.requireNonNull(config, "config");
        if (!config.repoEnabled()) {
            throw new IllegalArgumentException("repoTarget cannot be null or blank");
        }
        ManagedChannel channel = ManagedChannelBuilder.forTarget(config.repoTarget())
                .usePlaintext()
                .build();
        try {
            return new RepoChangeSink(DocumentServiceGrpc.newBlockingStub(channel),
                    config.repoDrive(), config.repoAccountId(), config.repoDatasourceId(), channel);
        } catch (RuntimeException e) {
            channel.shutdownNow();
            throw e;
        }
    }

    /** The deterministic doc id one Confluence entity id maps to. */
    static String docIdFor(String entityId) {
        return UUID.nameUUIDFromBytes(("confluence|" + entityId)
                .getBytes(StandardCharsets.UTF_8)).toString();
    }

    /** The canonical intake NodeAddress for a derived document id. */
    NodeAddress addressFor(String docId) {
        return NodeAddress.newBuilder()
                .setDocId(docId)
                .setGraphAddressId(datasourceId)
                .setAccountId(accountId)
                .setGraphId(INTAKE_GRAPH_PREFIX + accountId)
                .build();
    }

    private static final int MAX_OPERATION_ID_LENGTH = 200;

    private static String operationIdFor(ConfluenceChange change, String entityId, String action) {
        if (!change.getChangeId().isBlank()) {
            String candidate = "confluence-" + action + "-" + entityId + "-" + change.getChangeId();
            if (candidate.length() <= MAX_OPERATION_ID_LENGTH) {
                return candidate;
            }
            UUID hashed = UUID.nameUUIDFromBytes(candidate.getBytes(StandardCharsets.UTF_8));
            return "confluence-" + action + "-" + hashed;
        }
        return UUID.randomUUID().toString();
    }

    @Override
    public void emit(ConfluenceChange change) {
        ConfluenceEntity entity = change.getEntity();
        if (change.getOperation() == ChangeOperation.CHANGE_OPERATION_DELETE) {
            delete(change, entity);
            return;
        }
        switch (entity.getEntityCase()) {
            case PAGE -> save(entity.getEntityId(), toDocument(entity, entity.getPage().getTitle(),
                    "confluence-page", bodyText(entity.getPage().getBody()),
                    entity.getPage().getWebUrl(), entity.getPage().getAuthorId(),
                    entity.getPage().getCreatedAt(), entity.getPage().getVersion().getCreatedAt(),
                    entity.getPage().getSpaceId(), entity.getPage().getVersion().getNumber(),
                    null), change);
            case BLOG_POST -> save(entity.getEntityId(), toDocument(entity, entity.getBlogPost().getTitle(),
                    "confluence-blogpost", bodyText(entity.getBlogPost().getBody()),
                    entity.getBlogPost().getWebUrl(), entity.getBlogPost().getAuthorId(),
                    entity.getBlogPost().getCreatedAt(),
                    entity.getBlogPost().getVersion().getCreatedAt(),
                    entity.getBlogPost().getSpaceId(), entity.getBlogPost().getVersion().getNumber(),
                    null), change);
            case ATTACHMENT -> saveAttachment(entity, entity.getAttachment(), change);
            default -> LOG.log(System.Logger.Level.DEBUG,
                    "confluence repo sink: skipping entity kind {0} (id {1})",
                    entity.getEntityCase(), entity.getEntityId());
        }
    }

    @Override
    public void snapshot(ConfluenceSnapshot snapshot) {
        LOG.log(System.Logger.Level.DEBUG,
                "confluence repo sink: snapshot {0} of space {1} needs no repo write",
                snapshot.getSnapshotId(), snapshot.getSpaceKey());
    }

    /**
     * Issues a tombstone DeleteDocument request, retrying with the current revision on conflict.
     * Nonexistent documents (v2 {@code NOT_FOUND}) and already-tombstoned documents are treated
     * as idempotent no-ops. Reusing {@code operation_id} across CAS retries is safe because
     * v2 {@code ABORTED} conflicts do not record an operation outcome.
     */
    private void delete(ConfluenceChange change, ConfluenceEntity entity) {
        String entityId = entity.getEntityId();
        DeleteDocumentRequest.Builder request = DeleteDocumentRequest.newBuilder()
                .setOperationId(operationIdFor(change, entityId, "delete"))
                .setAddress(addressFor(docIdFor(entityId)))
                .setExpected(ExpectedRevision.newBuilder().setRevision(1))
                .setReason("confluence delete " + entityId);
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            try {
                documents.deleteDocument(request.build());
                return;
            } catch (StatusRuntimeException failure) {
                if (failure.getStatus().getCode() == Status.Code.NOT_FOUND) {
                    return;
                }
                OptionalLong currentRevision = currentRevisionFromConflict(failure, true);
                if (currentRevision.isEmpty() || attempt + 1 >= MAX_CAS_ATTEMPTS) {
                    throw failure;
                }
                if (currentRevision.getAsLong() <= 0) {
                    return;
                }
                request.setExpected(ExpectedRevision.newBuilder().setRevision(currentRevision.getAsLong()));
            }
        }
    }

    /** Attachments with bytes populate BlobBag inline and write both CORE and BLOBS parts. */
    private void saveAttachment(ConfluenceEntity entity, Attachment attachment,
            ConfluenceChange change) {
        BlobBag blobBag = null;
        if (!attachment.getContent().isEmpty()) {
            String mimeType = attachment.getMediaType().isBlank()
                    ? DEFAULT_MIME_TYPE : attachment.getMediaType();
            blobBag = BlobBag.newBuilder()
                    .setBlob(Blob.newBuilder()
                            .setBlobId(docIdFor(entity.getEntityId()))
                            .setDriveId(drive)
                            .setData(attachment.getContent())
                            .setMimeType(mimeType)
                            .setFilename(attachment.getTitle())
                            .setSizeBytes(attachment.getContent().size())
                            .build())
                    .build();
        }
        save(entity.getEntityId(), toDocument(entity, attachment.getTitle(), "confluence-attachment", "",
                attachment.getWebUrl(), "", attachment.getCreatedAt(),
                attachment.getVersion().getCreatedAt(), "",
                attachment.getVersion().getNumber(), blobBag), change);
    }

    /** The Document for one entity, per the mapping in the class javadoc. */
    private Document toDocument(ConfluenceEntity entity, String title, String documentType,
            String body, String webUrl, String authorId, Timestamp createdAt,
            Timestamp lastModifiedAt, String spaceId, int versionNumber, BlobBag blobBag) {
        SearchMetadata.Builder metadata = SearchMetadata.newBuilder()
                .setTitle(title)
                .setDocumentType(documentType)
                .putMetadata("confluence.entity_id", entity.getEntityId())
                .putMetadata("confluence.version", String.valueOf(versionNumber));
        if (!body.isBlank()) {
            metadata.setBody(body);
        }
        if (!webUrl.isBlank()) {
            metadata.setSourceUri(webUrl);
        }
        if (!authorId.isBlank()) {
            metadata.setAuthor(authorId);
        }
        if (createdAt.getSeconds() != 0 || createdAt.getNanos() != 0) {
            metadata.setCreationDate(createdAt);
        }
        if (lastModifiedAt.getSeconds() != 0 || lastModifiedAt.getNanos() != 0) {
            metadata.setLastModifiedDate(lastModifiedAt);
        }
        if (!spaceId.isBlank()) {
            metadata.putMetadata("confluence.space_id", spaceId);
        }
        Document.Builder document = Document.newBuilder()
                .setDocId(docIdFor(entity.getEntityId()))
                .setSearchMetadata(metadata)
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId(accountId)
                        .setDatasourceId(datasourceId)
                        .setConnectorId(CONNECTOR_ID))
                .setDocIdDerivation(DocIdDerivation.newBuilder()
                        .setMethod(DocIdDerivationMethod.DOC_ID_DERIVATION_METHOD_SOURCE_DOC_ID)
                        .setSourceValue("confluence:" + entity.getEntityId()));
        if (blobBag != null) {
            document.setBlobBag(blobBag);
        }
        return document.build();
    }

    /**
     * One intake save; the crawl run id (the change cursor on CRAWL) rides in request metadata.
     * Reusing {@code operation_id} across CAS retries is safe because v2 {@code ABORTED} conflicts
     * are not recorded in {@code catalog_operation_outcomes}.
     */
    private void save(String entityId, Document document, ConfluenceChange change) {
        SaveDocumentRequest.Builder request = SaveDocumentRequest.newBuilder()
                .setOperationId(operationIdFor(change, entityId, "save"))
                .setTarget(DocumentTarget.newBuilder().setAddress(addressFor(document.getDocId())))
                .setExpected(ExpectedRevision.newBuilder().setNone(true))
                .setDocument(document)
                .addWrites(PUT_CORE)
                .setDrive(drive)
                .setOwner(SaveOwner.newBuilder()
                        .setIntake(IntakeOrigin.newBuilder()
                                .setDatasourceId(datasourceId)
                                .setConnectorId(CONNECTOR_ID)));
        if (document.hasBlobBag()) {
            request.addWrites(PUT_BLOBS);
        }
        if (!change.getCursor().isBlank()) {
            request.putMetadata(METADATA_CRAWL_ID, change.getCursor());
        }
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            try {
                documents.saveDocument(request.build());
                return;
            } catch (StatusRuntimeException failure) {
                OptionalLong currentRevision = currentRevisionFromConflict(failure, false);
                if (currentRevision.isEmpty() || attempt + 1 >= MAX_CAS_ATTEMPTS) {
                    throw failure;
                }
                request.setExpected(currentRevision.getAsLong() == 0
                        ? ExpectedRevision.newBuilder().setNone(true).build()
                        : ExpectedRevision.newBuilder().setRevision(currentRevision.getAsLong()).build());
            }
        }
    }

    private static OptionalLong currentRevisionFromConflict(StatusRuntimeException failure, boolean ignoreIfTombstone) {
        if (failure.getStatus().getCode() != Status.Code.ABORTED) {
            return OptionalLong.empty();
        }
        com.google.rpc.Status status = StatusProto.fromThrowable(failure);
        if (status == null) {
            return OptionalLong.empty();
        }
        for (com.google.protobuf.Any detail : status.getDetailsList()) {
            if (detail.is(RevisionConflict.class)) {
                try {
                    RevisionConflict conflict = detail.unpack(RevisionConflict.class);
                    if (ignoreIfTombstone && conflict.getCurrentIsTombstone()) {
                        return OptionalLong.of(0L);
                    }
                    return OptionalLong.of(conflict.getCurrentRevision());
                } catch (InvalidProtocolBufferException ignored) {
                    return OptionalLong.empty();
                }
            }
        }
        return OptionalLong.empty();
    }

    /** The first populated body slot, or "" for a metadata-only record. */
    private static String bodyText(Body body) {
        if (!body.getStorage().getValue().isBlank()) {
            return body.getStorage().getValue();
        }
        if (!body.getAtlasDocFormat().getValue().isBlank()) {
            return body.getAtlasDocFormat().getValue();
        }
        if (!body.getView().getValue().isBlank()) {
            return body.getView().getValue();
        }
        return body.getRaw().getValue();
    }

    /**
     * Shuts the channel down when this sink created it; a no-op otherwise.
     *
     * @throws InterruptedException when interrupted while waiting for channel termination
     */
    @Override
    public void close() throws InterruptedException {
        if (channel != null) {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                channel.shutdownNow();
                throw e;
            }
        }
    }
}
