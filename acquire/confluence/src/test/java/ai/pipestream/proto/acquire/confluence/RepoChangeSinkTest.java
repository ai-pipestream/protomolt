package ai.pipestream.proto.acquire.confluence;

import ai.pipestream.proto.acquire.confluence.v1.Attachment;
import ai.pipestream.proto.acquire.confluence.v1.Body;
import ai.pipestream.proto.acquire.confluence.v1.BodyFormat;
import ai.pipestream.proto.acquire.confluence.v1.BodyType;
import ai.pipestream.proto.acquire.confluence.v1.ChangeOperation;
import ai.pipestream.proto.acquire.confluence.v1.ChangeSource;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceChange;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceEntity;
import ai.pipestream.proto.acquire.confluence.v1.Label;
import ai.pipestream.proto.acquire.confluence.v1.Page;
import ai.pipestream.proto.acquire.confluence.v1.Version;
import ai.pipestream.proto.repo.v1.DeleteDocumentRequest;
import ai.pipestream.proto.repo.v1.DeleteDocumentResponse;
import ai.pipestream.proto.repo.v1.DocumentServiceGrpc;
import ai.pipestream.proto.repo.v1.FileStorageReference;
import ai.pipestream.proto.repo.v1.PutBlobRequest;
import ai.pipestream.proto.repo.v1.PutBlobResponse;
import ai.pipestream.proto.repo.v1.SaveDocumentRequest;
import ai.pipestream.proto.repo.v1.SaveDocumentResponse;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The entity-to-Document mapping of {@link RepoChangeSink}, verified against
 * a fake {@code DocumentService} on the in-process transport: deterministic
 * doc ids (re-crawls upsert), intake-save request shape, attachment bytes
 * through PutBlob, logical deletes, and the skip list for non-content arms.
 */
class RepoChangeSinkTest {

    private Server server;
    private ManagedChannel channel;
    private FakeDocumentService fake;
    private RepoChangeSink sink;

    /** Records every request the sink makes; responses are canned. */
    private static final class FakeDocumentService extends DocumentServiceGrpc.DocumentServiceImplBase {
        private final List<SaveDocumentRequest> saves = new CopyOnWriteArrayList<>();
        private final List<PutBlobRequest> puts = new CopyOnWriteArrayList<>();
        private final List<DeleteDocumentRequest> deletes = new CopyOnWriteArrayList<>();

        @Override
        public void saveDocument(SaveDocumentRequest request,
                StreamObserver<SaveDocumentResponse> observer) {
            saves.add(request);
            observer.onNext(SaveDocumentResponse.newBuilder()
                    .setNodeId("node-" + saves.size())
                    .setDrive(request.getDrive())
                    .build());
            observer.onCompleted();
        }

        @Override
        public void putBlob(PutBlobRequest request, StreamObserver<PutBlobResponse> observer) {
            puts.add(request);
            observer.onNext(PutBlobResponse.newBuilder()
                    .setStorageRef(FileStorageReference.newBuilder()
                            .setDriveName(request.getDriveName())
                            .setObjectKey("blobs/blob-" + puts.size()))
                    .setSizeBytes(request.getData().size())
                    .setSha256("deadbeef")
                    .build());
            observer.onCompleted();
        }

        @Override
        public void deleteDocument(DeleteDocumentRequest request,
                StreamObserver<DeleteDocumentResponse> observer) {
            deletes.add(request);
            observer.onNext(DeleteDocumentResponse.getDefaultInstance());
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

    @Test
    void pageUpsertSavesAnIntakeDocument() {
        sink.emit(change(pageEntity("123", "Hello Repo", "<p>Hello Repo</p>")).build());

        assertThat(fake.saves).hasSize(1);
        SaveDocumentRequest request = fake.saves.get(0);
        assertThat(request.getDrive()).isEqualTo("drive-1");
        assertThat(request.getConnectorId()).isEqualTo("confluence");
        assertThat(request.getUseDatasourceId()).isTrue();
        assertThat(request.getGraphId()).isEqualTo("intake:account-1");
        assertThat(request.getCrawlId()).isEqualTo("run-1");

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
        sink.emit(change(pageEntity("123", "v1", "<p>one</p>")).build());
        sink.emit(change(pageEntity("123", "v2", "<p>two</p>")).build());

        assertThat(fake.saves).hasSize(2);
        assertThat(fake.saves.get(0).getDocument().getDocId())
                .isEqualTo(fake.saves.get(1).getDocument().getDocId());
        assertThat(RepoChangeSink.docIdFor("123")).isEqualTo(RepoChangeSink.docIdFor("123"));
        assertThat(RepoChangeSink.docIdFor("123")).isNotEqualTo(RepoChangeSink.docIdFor("124"));
    }

    @Test
    void attachmentBytesGoThroughPutBlobAndAreReferenced() {
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

        assertThat(fake.puts).hasSize(1);
        PutBlobRequest put = fake.puts.get(0);
        assertThat(put.getDriveName()).isEqualTo("drive-1");
        assertThat(put.getData().toStringUtf8()).isEqualTo("png-bytes");
        assertThat(put.getMimeType()).isEqualTo("image/png");

        assertThat(fake.saves).hasSize(1);
        var blob = fake.saves.get(0).getDocument().getBlobBag().getBlob();
        assertThat(blob.getStorageRef().getDriveName()).isEqualTo("drive-1");
        assertThat(blob.getStorageRef().getObjectKey()).isEqualTo("blobs/blob-1");
        assertThat(blob.getChecksum()).isEqualTo("deadbeef");
        assertThat(blob.getFilename()).isEqualTo("diagram.png");
        assertThat(fake.saves.get(0).getDocument().getSearchMetadata().getDocumentType())
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

        assertThat(fake.puts).isEmpty();
        assertThat(fake.saves).hasSize(1);
        assertThat(fake.saves.get(0).getDocument().hasBlobBag()).isFalse();
    }

    @Test
    void deleteIssuesALogicalDeleteForTheDerivedId() {
        ConfluenceEntity identityOnly = ConfluenceEntity.newBuilder()
                .setEntityId("123")
                .setIngestedAt(at(1_753_000_000))
                .build();
        sink.emit(change(identityOnly.toBuilder())
                .setOperation(ChangeOperation.CHANGE_OPERATION_DELETE)
                .build());

        assertThat(fake.saves).isEmpty();
        assertThat(fake.deletes).hasSize(1);
        var command = fake.deletes.get(0).getLogicalDocument();
        assertThat(command.getDocId()).isEqualTo(RepoChangeSink.docIdFor("123"));
        assertThat(command.getAccountId()).isEqualTo("account-1");
        assertThat(command.getDatasourceId()).isEqualTo("datasource-1");
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
        assertThat(fake.puts).isEmpty();
        assertThat(fake.deletes).isEmpty();
    }
}
