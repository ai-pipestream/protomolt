package ai.protomolt.proto.acquire.confluence;

import ai.protomolt.proto.acquire.confluence.v1.Attachment;
import ai.protomolt.proto.acquire.confluence.v1.Body;
import ai.protomolt.proto.acquire.confluence.v1.BodyFormat;
import ai.protomolt.proto.acquire.confluence.v1.BodyType;
import ai.protomolt.proto.acquire.confluence.v1.ChangeOperation;
import ai.protomolt.proto.acquire.confluence.v1.ChangeSource;
import ai.protomolt.proto.acquire.confluence.v1.ConfluenceChange;
import ai.protomolt.proto.acquire.confluence.v1.ConfluenceEntity;
import ai.protomolt.proto.acquire.confluence.v1.Label;
import ai.protomolt.proto.acquire.confluence.v1.Page;
import ai.protomolt.proto.acquire.confluence.v1.Version;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.Accession;
import ai.protomolt.proto.repo.v2.DeleteDocumentRequest;
import ai.protomolt.proto.repo.v2.DeleteDocumentResponse;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartWrite;
import ai.protomolt.proto.repo.v2.RevisionConflict;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveDocumentResponse;
import ai.protomolt.proto.repo.v2.WriteAction;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The entity-to-Document mapping of {@link RepoChangeSink}, verified against
 * a fake v2 {@code DocumentService} on the in-process transport: deterministic
 * doc ids (re-crawls upsert), intake-save request shape, inline attachment bytes
 * written as {@code DOCUMENT_PART_BLOBS}, tombstone deletes, and the skip list
 * for non-content arms.
 */
class RepoChangeSinkTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private Server server;
    private ManagedChannel channel;
    private FakeDocumentService fake;
    private RepoChangeSink sink;

    /** Records every request the sink makes; responses are canned with optional CAS conflict simulation. */
    private static final class FakeDocumentService extends DocumentServiceGrpc.DocumentServiceImplBase {
        private final List<SaveDocumentRequest> saves = new CopyOnWriteArrayList<>();
        private final List<DeleteDocumentRequest> deletes = new CopyOnWriteArrayList<>();
        private final Map<String, Long> currentRevisions = new ConcurrentHashMap<>();
        private boolean enforceCas;

        @Override
        public void saveDocument(SaveDocumentRequest request,
                StreamObserver<SaveDocumentResponse> observer) {
            saves.add(request);
            String docId = request.getTarget().getAddress().getDocId();
            long current = currentRevisions.getOrDefault(docId, 0L);
            if (enforceCas) {
                boolean matches = request.getExpected().hasNone()
                        ? current == 0L
                        : request.getExpected().getRevision() == current;
                if (!matches) {
                    com.google.rpc.Status status = com.google.rpc.Status.newBuilder()
                            .setCode(Status.Code.ABORTED.value())
                            .setMessage("Expected revision is stale")
                            .addDetails(Any.pack(RevisionConflict.newBuilder()
                                    .setAddress(request.getTarget().getAddress())
                                    .setExpected(request.getExpected())
                                    .setCurrentRevision(current)
                                    .build()))
                            .build();
                    observer.onError(StatusProto.toStatusRuntimeException(status));
                    return;
                }
            }
            long next = current + 1L;
            currentRevisions.put(docId, next);
            observer.onNext(SaveDocumentResponse.newBuilder()
                    .setOperationId(request.getOperationId())
                    .setAccession(Accession.newBuilder()
                            .setAddress(request.getTarget().getAddress())
                            .setRevision(next)
                            .setPreviousRevision(current))
                    .build());
            observer.onCompleted();
        }

        @Override
        public void deleteDocument(DeleteDocumentRequest request,
                StreamObserver<DeleteDocumentResponse> observer) {
            deletes.add(request);
            observer.onNext(DeleteDocumentResponse.newBuilder()
                    .setOperationId(request.getOperationId())
                    .setAccession(Accession.newBuilder()
                            .setAddress(request.getAddress())
                            .setRevision(request.getExpected().getRevision() + 1L)
                            .setPreviousRevision(request.getExpected().getRevision())
                            .setTombstone(true))
                    .build());
            observer.onCompleted();
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        fake = new FakeDocumentService();
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).addService(fake).directExecutor()
                .build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        sink = new RepoChangeSink(DocumentServiceGrpc.newBlockingStub(channel),
                "drive-1", "account-1", "datasource-1");
    }

    @AfterEach
    void tearDown() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    private static Timestamp at(long seconds) {
        return Timestamp.newBuilder().setSeconds(seconds).build();
    }

    private static ConfluenceChange.Builder change(ConfluenceEntity.Builder entity) {
        return ConfluenceChange.newBuilder()
                .setChangeId("change-1")
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setEntity(entity)
                .setSource(ChangeSource.CHANGE_SOURCE_CRAWL)
                .setCursor("run-1")
                .setOccurredAt(at(1_753_000_001));
    }

    private static ConfluenceEntity.Builder pageEntity(String id, String title, String body) {
        return ConfluenceEntity.newBuilder()
                .setEntityId(id)
                .setIngestedAt(at(1_753_000_000))
                .setPage(Page.newBuilder()
                        .setId(id)
                        .setSpaceId("456")
                        .setTitle(title)
                        .setAuthorId("acc-1")
                        .setWebUrl("https://example.atlassian.net/wiki/spaces/ENG/pages/" + id)
                        .setCreatedAt(at(1_752_000_000))
                        .setVersion(Version.newBuilder().setNumber(3).setCreatedAt(at(1_753_000_000)))
                        .setBody(Body.newBuilder().setStorage(BodyType.newBuilder()
                                .setFormat(BodyFormat.BODY_FORMAT_STORAGE_XHTML)
                                .setValue(body))));
    }

    private static PartWrite put(DocumentPart part) {
        return PartWrite.newBuilder()
                .setSelector(PartSelector.newBuilder().setPart(part))
                .setAction(WriteAction.WRITE_ACTION_PUT)
                .build();
    }

    @Test
    void pageUpsertSavesAnIntakeDocument() {
        sink.emit(change(pageEntity("123", "Hello Repo", "<p>Hello Repo</p>")).build());

        assertThat(fake.saves).hasSize(1);
        SaveDocumentRequest request = fake.saves.get(0);
        assertThat(VALIDATOR.validate(request).valid()).isTrue();
        assertThat(request.getOperationId()).isNotBlank();
        assertThat(request.getDrive()).isEqualTo("drive-1");
        assertThat(request.getTarget().getAddress()).isEqualTo(NodeAddress.newBuilder()
                .setDocId(RepoChangeSink.docIdFor("123"))
                .setGraphAddressId("datasource-1")
                .setAccountId("account-1")
                .setGraphId("intake:account-1")
                .build());
        assertThat(request.getExpected().hasNone()).isTrue();
        assertThat(request.getExpected().getNone()).isTrue();
        assertThat(request.getWritesList()).containsExactly(put(DocumentPart.DOCUMENT_PART_CORE));
        assertThat(request.getOwner().getIntake().getDatasourceId()).isEqualTo("datasource-1");
        assertThat(request.getOwner().getIntake().getConnectorId()).isEqualTo("confluence");
        assertThat(request.getMetadataMap()).containsEntry("crawl_id", "run-1");

        var document = request.getDocument();
        assertThat(document.getDocId()).isEqualTo(RepoChangeSink.docIdFor("123"));
        assertThat(document.getOwnership().getAccountId()).isEqualTo("account-1");
        assertThat(document.getOwnership().getDatasourceId()).isEqualTo("datasource-1");
        assertThat(document.getOwnership().getConnectorId()).isEqualTo("confluence");
        assertThat(document.getDocIdDerivation().getSourceValue()).isEqualTo("confluence:123");

        var metadata = document.getSearchMetadata();
        assertThat(metadata.getTitle()).isEqualTo("Hello Repo");
        assertThat(metadata.getBody()).isEqualTo("<p>Hello Repo</p>");
        assertThat(metadata.getDocumentType()).isEqualTo("confluence-page");
        assertThat(metadata.getSourceUri()).contains("/pages/123");
        assertThat(metadata.getAuthor()).isEqualTo("acc-1");
        assertThat(metadata.getCreationDate().getSeconds()).isEqualTo(1_752_000_000L);
        assertThat(metadata.getLastModifiedDate().getSeconds()).isEqualTo(1_753_000_000L);
        assertThat(metadata.getMetadataMap())
                .containsEntry("confluence.entity_id", "123")
                .containsEntry("confluence.space_id", "456")
                .containsEntry("confluence.version", "3");
    }

    @Test
    void reCrawlUpsertsTheSameDocumentId() {
        fake.enforceCas = true;
        sink.emit(change(pageEntity("123", "v1", "<p>one</p>")).build());
        sink.emit(change(pageEntity("123", "v2", "<p>two</p>")).build());

        assertThat(fake.saves).hasSize(3);
        SaveDocumentRequest firstSave = fake.saves.get(0);
        SaveDocumentRequest secondInitial = fake.saves.get(1);
        SaveDocumentRequest secondRetry = fake.saves.get(2);

        assertThat(firstSave.getDocument().getDocId())
                .isEqualTo(secondRetry.getDocument().getDocId());
        assertThat(firstSave.getExpected().getNone()).isTrue();
        assertThat(secondInitial.getExpected().getNone()).isTrue();
        assertThat(secondRetry.getExpected().getRevision()).isEqualTo(1L);
        assertThat(firstSave.getOperationId()).isNotEqualTo(secondRetry.getOperationId());
        assertThat(secondInitial.getOperationId()).isEqualTo(secondRetry.getOperationId());
        assertThat(RepoChangeSink.docIdFor("123")).isEqualTo(RepoChangeSink.docIdFor("123"));
        assertThat(RepoChangeSink.docIdFor("123")).isNotEqualTo(RepoChangeSink.docIdFor("124"));
    }

    @Test
    void attachmentBytesSaveAsBlobsPartWithInlineData() {
        ConfluenceEntity entity = ConfluenceEntity.newBuilder()
                .setEntityId("att-9")
                .setIngestedAt(at(1_753_000_000))
                .setAttachment(Attachment.newBuilder()
                        .setId("att-9")
                        .setTitle("diagram.png")
                        .setMediaType("image/png")
                        .setContent(ByteString.copyFromUtf8("png-bytes")))
                .build();
        sink.emit(change(entity.toBuilder()).build());

        assertThat(fake.saves).hasSize(1);
        SaveDocumentRequest request = fake.saves.get(0);
        assertThat(VALIDATOR.validate(request).valid()).isTrue();
        assertThat(request.getWritesList()).containsExactly(
                put(DocumentPart.DOCUMENT_PART_CORE),
                put(DocumentPart.DOCUMENT_PART_BLOBS));

        var blob = request.getDocument().getBlobBag().getBlob();
        assertThat(blob.getBlobId()).isEqualTo(RepoChangeSink.docIdFor("att-9"));
        assertThat(blob.getDriveId()).isEqualTo("drive-1");
        assertThat(blob.getData().toStringUtf8()).isEqualTo("png-bytes");
        assertThat(blob.getMimeType()).isEqualTo("image/png");
        assertThat(blob.getFilename()).isEqualTo("diagram.png");
        assertThat(blob.getSizeBytes()).isEqualTo(9L);
        assertThat(blob.hasStorageRef()).isFalse();
        assertThat(request.getDocument().getSearchMetadata().getDocumentType())
                .isEqualTo("confluence-attachment");
    }

    @Test
    void attachmentWithoutContentSavesMetadataOnly() {
        ConfluenceEntity entity = ConfluenceEntity.newBuilder()
                .setEntityId("att-10")
                .setIngestedAt(at(1_753_000_000))
                .setAttachment(Attachment.newBuilder().setId("att-10").setTitle("spec.pdf"))
                .build();
        sink.emit(change(entity.toBuilder()).build());

        assertThat(fake.saves).hasSize(1);
        SaveDocumentRequest request = fake.saves.get(0);
        assertThat(VALIDATOR.validate(request).valid()).isTrue();
        assertThat(request.getWritesList()).containsExactly(put(DocumentPart.DOCUMENT_PART_CORE));
        assertThat(request.getDocument().hasBlobBag()).isFalse();
    }

    @Test
    void deleteIssuesATombstoneDeleteForTheDerivedId() {
        ConfluenceEntity identityOnly = ConfluenceEntity.newBuilder()
                .setEntityId("123")
                .setIngestedAt(at(1_753_000_000))
                .build();
        sink.emit(change(identityOnly.toBuilder())
                .setOperation(ChangeOperation.CHANGE_OPERATION_DELETE)
                .build());

        assertThat(fake.saves).isEmpty();
        assertThat(fake.deletes).hasSize(1);
        DeleteDocumentRequest delete = fake.deletes.get(0);
        assertThat(VALIDATOR.validate(delete).valid()).isTrue();
        assertThat(delete.getOperationId()).isNotBlank();
        assertThat(delete.getAddress()).isEqualTo(NodeAddress.newBuilder()
                .setDocId(RepoChangeSink.docIdFor("123"))
                .setGraphAddressId("datasource-1")
                .setAccountId("account-1")
                .setGraphId("intake:account-1")
                .build());
        assertThat(delete.getExpected().hasRevision()).isTrue();
        assertThat(delete.getExpected().getRevision()).isEqualTo(1L);
        assertThat(delete.getReason()).isEqualTo("confluence delete 123");
    }

    @Test
    void nonContentArmsAreSkipped() {
        ConfluenceEntity label = ConfluenceEntity.newBuilder()
                .setEntityId("label-1")
                .setIngestedAt(at(1_753_000_000))
                .setLabel(Label.newBuilder().setId("label-1").setName("team"))
                .build();
        sink.emit(change(label.toBuilder()).build());

        assertThat(fake.saves).isEmpty();
        assertThat(fake.deletes).isEmpty();
    }
}
