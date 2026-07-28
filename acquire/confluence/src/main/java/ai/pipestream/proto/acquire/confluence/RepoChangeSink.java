package ai.pipestream.proto.acquire.confluence;

import ai.pipestream.proto.acquire.confluence.v1.Attachment;
import ai.pipestream.proto.acquire.confluence.v1.Body;
import ai.pipestream.proto.acquire.confluence.v1.ChangeOperation;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceChange;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceEntity;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceSnapshot;
import ai.pipestream.proto.repo.v1.Blob;
import ai.pipestream.proto.repo.v1.BlobBag;
import ai.pipestream.proto.repo.v1.ChecksumType;
import ai.pipestream.proto.repo.v1.DeleteDocumentRequest;
import ai.pipestream.proto.repo.v1.DeleteLogicalDocumentCommand;
import ai.pipestream.proto.repo.v1.DocIdDerivation;
import ai.pipestream.proto.repo.v1.DocIdDerivationMethod;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.DocumentServiceGrpc;
import ai.pipestream.proto.repo.v1.OwnershipContext;
import ai.pipestream.proto.repo.v1.PutBlobRequest;
import ai.pipestream.proto.repo.v1.PutBlobResponse;
import ai.pipestream.proto.repo.v1.SaveDocumentRequest;
import ai.pipestream.proto.repo.v1.SearchMetadata;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * The dogfood sink: mirrors Confluence content into the protomolt repo
 * service as {@link Document}s over the {@code DocumentService} gRPC API.
 *
 * <p>Entity-to-Document mapping (kept deliberately small; documented here as
 * the contract):</p>
 * <ul>
 *   <li>Identity: {@code doc_id} is a deterministic name-based UUID of
 *   {@code "confluence|<entityId>"} (the repo service's own
 *   {@code UUID.nameUUIDFromBytes} convention), so re-crawls upsert the same
 *   row instead of duplicating it. The derivation is stamped on
 *   {@code doc_id_derivation} as SOURCE_DOC_ID.</li>
 *   <li>Pages and blog posts become Documents whose
 *   {@code search_metadata.body} is the first populated body slot (storage,
 *   then atlas_doc_format, view, raw), whose {@code title} /
 *   {@code source_uri} / dates / author come off the entity, and whose
 *   {@code document_type} is {@code "confluence-page"} or
 *   {@code "confluence-blogpost"}.</li>
 *   <li>Attachments carrying content bytes go through {@code PutBlob} first
 *   (blank object key: the server mints a content-addressed key, so identical
 *   bytes land idempotently) and the Document's {@code blob_bag} references
 *   the returned {@code FileStorageReference}. Attachments without content
 *   save as metadata-only Documents.</li>
 *   <li>Confluence ids ride in {@code search_metadata.metadata}:
 *   {@code confluence.entity_id}, {@code confluence.space_id},
 *   {@code confluence.version}.</li>
 *   <li>Other entity arms (comments, labels, properties, ...) are skipped:
 *   the repo mirror stores content, not crawl exhaust.</li>
 *   <li>DELETE changes issue a logical {@code DeleteDocument} for the derived
 *   doc id; UPSERTs save with {@code use_datasource_id} and
 *   {@code graph_id = "intake:<accountId>"} (an intake save, per the
 *   DocumentService contract).</li>
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
     * @param documents the DocumentService blocking stub
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
        ManagedChannel channel = ManagedChannelBuilder.forTarget(config.repoTarget())
                .usePlaintext()
                .build();
        return new RepoChangeSink(DocumentServiceGrpc.newBlockingStub(channel),
                config.repoDrive(), config.repoAccountId(), config.repoDatasourceId(), channel);
    }

    /** The deterministic doc id one Confluence entity id maps to. */
    static String docIdFor(String entityId) {
        return UUID.nameUUIDFromBytes(("confluence|" + entityId)
                .getBytes(StandardCharsets.UTF_8)).toString();
    }

    @Override
    public void emit(ConfluenceChange change) {
        ConfluenceEntity entity = change.getEntity();
        if (change.getOperation() == ChangeOperation.CHANGE_OPERATION_DELETE) {
            documents.deleteDocument(DeleteDocumentRequest.newBuilder()
                    .setLogicalDocument(DeleteLogicalDocumentCommand.newBuilder()
                            .setDocId(docIdFor(entity.getEntityId()))
                            .setAccountId(accountId)
                            .setDatasourceId(datasourceId))
                    .build());
            return;
        }
        switch (entity.getEntityCase()) {
            case PAGE -> save(toDocument(entity, entity.getPage().getTitle(),
                    "confluence-page", bodyText(entity.getPage().getBody()),
                    entity.getPage().getWebUrl(), entity.getPage().getAuthorId(),
                    entity.getPage().getCreatedAt(), entity.getPage().getVersion().getCreatedAt(),
                    entity.getPage().getSpaceId(), entity.getPage().getVersion().getNumber(),
                    null), change);
            case BLOG_POST -> save(toDocument(entity, entity.getBlogPost().getTitle(),
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

    /** Attachments with bytes: PutBlob first, then a Document referencing the stored object. */
    private void saveAttachment(ConfluenceEntity entity, Attachment attachment,
            ConfluenceChange change) {
        BlobBag blobBag = null;
        if (!attachment.getContent().isEmpty()) {
            PutBlobResponse stored = documents.putBlob(PutBlobRequest.newBuilder()
                    .setDriveName(drive)
                    .setData(attachment.getContent())
                    .setMimeType(attachment.getMediaType().isBlank()
                            ? "application/octet-stream" : attachment.getMediaType())
                    .build());
            blobBag = BlobBag.newBuilder().setBlob(Blob.newBuilder()
                    .setBlobId(docIdFor(entity.getEntityId()))
                    .setDriveId(drive)
                    .setStorageRef(stored.getStorageRef())
                    .setMimeType(attachment.getMediaType())
                    .setFilename(attachment.getTitle())
                    .setSizeBytes(stored.getSizeBytes())
                    .setChecksum(stored.getSha256())
                    .setChecksumType(ChecksumType.CHECKSUM_TYPE_SHA256)
                    .build())
                    .build();
        }
        save(toDocument(entity, attachment.getTitle(), "confluence-attachment", "",
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

    /** One intake save; the crawl run id (the change cursor on CRAWL) rides as crawl_id. */
    private void save(Document document, ConfluenceChange change) {
        SaveDocumentRequest.Builder request = SaveDocumentRequest.newBuilder()
                .setDocument(document)
                .setDrive(drive)
                .setConnectorId(CONNECTOR_ID)
                .setUseDatasourceId(true)
                .setGraphId("intake:" + accountId);
        if (!change.getCursor().isBlank()) {
            request.setCrawlId(change.getCursor());
        }
        documents.saveDocument(request.build());
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

    /** Shuts the channel down when this sink created it; a no-op otherwise. */
    @Override
    public void close() throws InterruptedException {
        if (channel != null) {
            channel.shutdown();
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        }
    }
}
