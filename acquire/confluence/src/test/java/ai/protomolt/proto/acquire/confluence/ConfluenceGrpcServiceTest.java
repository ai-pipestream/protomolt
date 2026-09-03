package ai.protomolt.proto.acquire.confluence;

import ai.protomolt.proto.acquire.confluence.v1.BodyFormat;
import ai.protomolt.proto.acquire.confluence.v1.ConfluenceChange;
import ai.protomolt.proto.acquire.confluence.v1.ConfluenceEntity;
import ai.protomolt.proto.acquire.confluence.v1.ConfluenceServiceGrpc;
import ai.protomolt.proto.acquire.confluence.v1.ConfluenceSnapshot;
import ai.protomolt.proto.acquire.confluence.v1.GetAttachmentRequest;
import ai.protomolt.proto.acquire.confluence.v1.GetAttachmentResponse;
import ai.protomolt.proto.acquire.confluence.v1.GetBlogPostRequest;
import ai.protomolt.proto.acquire.confluence.v1.GetBlogPostResponse;
import ai.protomolt.proto.acquire.confluence.v1.GetPageRequest;
import ai.protomolt.proto.acquire.confluence.v1.GetPageResponse;
import ai.protomolt.proto.acquire.confluence.v1.ListBlogPostsRequest;
import ai.protomolt.proto.acquire.confluence.v1.ListBlogPostsResponse;
import ai.protomolt.proto.acquire.confluence.v1.ListPagesRequest;
import ai.protomolt.proto.acquire.confluence.v1.ListPagesResponse;
import ai.protomolt.proto.acquire.confluence.v1.ListSpacesRequest;
import ai.protomolt.proto.acquire.confluence.v1.ListSpacesResponse;
import ai.protomolt.proto.acquire.confluence.v1.SyncRequest;
import ai.protomolt.proto.acquire.confluence.v1.SyncResponse;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.DescriptorProtos;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import io.grpc.reflection.v1.ServerReflectionGrpc;
import io.grpc.reflection.v1.ServerReflectionRequest;
import io.grpc.reflection.v1.ServerReflectionResponse;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The facade end to end against the fake REST server: an in-process gRPC
 * server (reflection on, virtual-thread executor like the real launcher) plus
 * the generated blocking stub, so every test exercises the full wire path.
 */
class ConfluenceGrpcServiceTest {

    private static final Instant T1 = Instant.parse("2024-03-01T00:00:00Z");
    private static final Instant T2 = Instant.parse("2024-03-02T00:00:00Z");
    private static final Instant T3 = Instant.parse("2024-03-03T00:00:00Z");
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private FakeConfluenceServer fake;
    private ConfluenceConnectorConfig config;
    private ConfluenceClient client;
    private Server server;
    private ManagedChannel channel;
    private ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub;

    @BeforeEach
    void startStack() throws Exception {
        fake = FakeConfluenceServer.start();
        config = ConfluenceConnectorConfig.builder()
                .baseUrl(fake.baseUrl())
                .email("bot@pipestream.ai")
                .apiToken("token-123")
                .build();
        client = new ConfluenceClient(config.baseUrl(), config.email(), config.apiToken(),
                Duration.ZERO);
        startServer(new ConfluenceGrpcService(config, client,
                ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES));
    }

    private void startServer(ConfluenceGrpcService service) throws Exception {
        server = InProcessServerBuilder.forName("confluence-facade-test")
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(service)
                .addService(ProtoReflectionServiceV1.newInstance())
                .build().start();
        channel = InProcessChannelBuilder.forName("confluence-facade-test").build();
        stub = ConfluenceServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void stopStack() {
        channel.shutdownNow();
        server.shutdownNow();
        fake.close();
    }

    // ======================================================================
    // READ RPCS
    // ======================================================================

    @Test
    void listSpacesReturnsAllOrFilteredByKeys() {
        fake.stub("/wiki/api/v2/spaces",
                ConfluenceFixtures.spaceListJson(null,
                        ConfluenceFixtures.spaceJson("100", "ENG", "Engineering"),
                        ConfluenceFixtures.spaceJson("101", "DOCS", "Documentation")));
        // The API applies the keys filter server-side; the fake mirrors that
        // with an exact-query stub.
        fake.stub("/wiki/api/v2/spaces?description-format=view&keys=ENG&limit=100",
                ConfluenceFixtures.spaceListJson(null,
                        ConfluenceFixtures.spaceJson("100", "ENG", "Engineering")));

        ListSpacesResponse all = stub.listSpaces(ListSpacesRequest.getDefaultInstance());
        assertThat(all.getSpacesList()).extracting(s -> s.getKey())
                .containsExactlyInAnyOrder("ENG", "DOCS");
        all.getSpacesList().forEach(space -> assertThat(VALIDATOR.validate(space).violations())
                .as("space %s validates", space.getKey()).isEmpty());

        ListSpacesResponse filtered = stub.listSpaces(ListSpacesRequest.newBuilder()
                .addKeys("ENG").build());
        assertThat(filtered.getSpacesList()).extracting(s -> s.getKey()).containsExactly("ENG");
        assertThat(fake.requestsTo("/wiki/api/v2/spaces").getLast().query()).contains("keys=ENG");
    }

    @Test
    void getPageReturnsBodyInStorageFormatByDefault() {
        fake.stub("/wiki/api/v2/pages/200",
                ConfluenceFixtures.pageJson("200", "100", "Design Doc", T2.toString()));

        GetPageResponse response = stub.getPage(GetPageRequest.newBuilder().setId("200").build());

        assertThat(response.getPage().getTitle()).isEqualTo("Design Doc");
        assertThat(response.getPage().getBody().getStorage().getValue())
                .isEqualTo("<p>Hello Design Doc</p>");
        assertThat(response.getPage().getBody().getStorage().getFormat())
                .isEqualTo(BodyFormat.BODY_FORMAT_STORAGE_XHTML);
        assertThat(VALIDATOR.validate(response.getPage()).violations()).isEmpty();
        assertThat(fake.requestsTo("/wiki/api/v2/pages/200").getLast().query())
                .contains("body-format=storage");
    }

    @Test
    void getBlogPostReturnsTheWrappedPost() {
        fake.stub("/wiki/api/v2/blogposts/300",
                ConfluenceFixtures.blogPostJson("300", "100", "Release notes", T1.toString()));

        GetBlogPostResponse response = stub.getBlogPost(
                GetBlogPostRequest.newBuilder().setId("300").build());

        assertThat(response.getBlogPost().getTitle()).isEqualTo("Release notes");
        assertThat(response.getBlogPost().getBody().getStorage().getValue())
                .isEqualTo("<p>Post Release notes</p>");
        assertThat(VALIDATOR.validate(response.getBlogPost()).violations()).isEmpty();
    }

    @Test
    void unknownPageIsNotFound() {
        assertThatThrownBy(() -> stub.getPage(GetPageRequest.newBuilder().setId("nope").build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
    }

    @Test
    void listPagesStreamsAcrossRestPagesWithoutBodies() {
        // No body-format requested: the API answers metadata-only entries.
        fake.stub("/wiki/api/v2/pages?limit=100&space-id=100",
                ConfluenceFixtures.pageListJson("cursor-2",
                        ConfluenceFixtures.pageJsonMetadataOnly("201", "100", "One", T1.toString()),
                        ConfluenceFixtures.pageJsonMetadataOnly("202", "100", "Two", T2.toString())));
        fake.stub("/wiki/api/v2/pages?cursor=cursor-2",
                ConfluenceFixtures.pageListJson(null,
                        ConfluenceFixtures.pageJsonMetadataOnly("203", "100", "Three",
                                T3.toString())));

        List<ListPagesResponse> pages = new ArrayList<>();
        stub.listPages(ListPagesRequest.newBuilder().setSpaceId("100").build())
                .forEachRemaining(pages::add);

        // The stream hid the REST pagination: three pages from two REST responses.
        assertThat(pages).hasSize(3);
        assertThat(pages).extracting(p -> p.getPage().getTitle())
                .containsExactly("One", "Two", "Three");
        // Metadata-only listing: no body-format requested, so bodies stay unset.
        assertThat(pages).allSatisfy(p -> assertThat(p.getPage().hasBody()).isFalse());
        assertThat(fake.requestsTo("/wiki/api/v2/pages").getFirst().query())
                .doesNotContain("body-format");
    }

    @Test
    void listBlogPostsStreamsWithBodiesWhenRequested() {
        fake.stub("/wiki/api/v2/blogposts",
                ConfluenceFixtures.blogPostListJson(null,
                        ConfluenceFixtures.blogPostJson("300", "100", "Release notes",
                                T1.toString())));

        List<ListBlogPostsResponse> posts = new ArrayList<>();
        stub.listBlogPosts(ListBlogPostsRequest.newBuilder()
                        .setSpaceId("100")
                        .setBodyFormat(BodyFormat.BODY_FORMAT_STORAGE_XHTML)
                        .build())
                .forEachRemaining(posts::add);

        assertThat(posts).hasSize(1);
        assertThat(posts.get(0).getBlogPost().getBody().getStorage().getValue())
                .isEqualTo("<p>Post Release notes</p>");
        assertThat(fake.requestsTo("/wiki/api/v2/blogposts").getFirst().query())
                .contains("body-format=storage");
    }

    // ======================================================================
    // ATTACHMENTS
    // ======================================================================

    @Test
    void getAttachmentMetadataOnlyByDefault() {
        fake.stub("/wiki/api/v2/attachments/a1", ConfluenceFixtures.attachmentJson("a1", "200"));

        GetAttachmentResponse response = stub.getAttachment(
                GetAttachmentRequest.newBuilder().setId("a1").build());

        assertThat(response.getAttachment().getTitle()).isEqualTo("diagram.png");
        assertThat(response.getAttachment().getFileSize()).isEqualTo(12345L);
        assertThat(response.getAttachment().hasContent()).isFalse();
        assertThat(VALIDATOR.validate(response.getAttachment()).violations()).isEmpty();
    }

    @Test
    void getAttachmentWithContentDownloadsThroughTheClient() {
        fake.stub("/wiki/api/v2/attachments/a1", ConfluenceFixtures.attachmentJson("a1", "200"));
        fake.stub("/wiki/download/attachments/a1/diagram.png", "png-bytes");

        GetAttachmentResponse response = stub.getAttachment(GetAttachmentRequest.newBuilder()
                .setId("a1").setIncludeContent(true).build());

        assertThat(response.getAttachment().hasContent()).isTrue();
        assertThat(response.getAttachment().getContent().toString(StandardCharsets.UTF_8))
                .isEqualTo("png-bytes");
    }

    @Test
    void getAttachmentAboveTheInlineCapIsFailedPrecondition() throws Exception {
        server.shutdown();
        channel.shutdownNow();
        startServer(new ConfluenceGrpcService(config, client, 100));
        fake.stub("/wiki/api/v2/attachments/a1", ConfluenceFixtures.attachmentJson("a1", "200"));

        assertThatThrownBy(() -> stub.getAttachment(GetAttachmentRequest.newBuilder()
                .setId("a1").setIncludeContent(true).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        e -> {
                            assertThat(e.getStatus().getCode())
                                    .isEqualTo(Status.Code.FAILED_PRECONDITION);
                            assertThat(e.getStatus().getDescription()).contains("inline cap");
                        });
    }

    // ======================================================================
    // SYNC
    // ======================================================================

    /** Stubs one ENG space with one page (plus full sub-entities) and one blog post. */
    private void stubWorkspace() {
        fake.stub("/wiki/api/v2/spaces",
                ConfluenceFixtures.spaceListJson(null,
                        ConfluenceFixtures.spaceJson("100", "ENG", "Engineering")));
        fake.stub("/wiki/api/v2/spaces/100/properties", ConfluenceFixtures.emptyListJson());

        fake.stub("/wiki/api/v2/pages",
                ConfluenceFixtures.pageListJson(null,
                        ConfluenceFixtures.pageJson("200", "100", "Design Doc", T2.toString())));
        fake.stub("/wiki/api/v2/pages/200/footer-comments",
                ConfluenceFixtures.listJson(null, "",
                        ConfluenceFixtures.footerCommentJson("c1", "200")));
        fake.stub("/wiki/api/v2/pages/200/inline-comments", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages/200/attachments",
                ConfluenceFixtures.listJson(null, "",
                        ConfluenceFixtures.attachmentJson("a1", "200")));
        fake.stub("/wiki/api/v2/pages/200/labels", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages/200/properties", ConfluenceFixtures.emptyListJson());

        fake.stub("/wiki/api/v2/blogposts",
                ConfluenceFixtures.blogPostListJson(null,
                        ConfluenceFixtures.blogPostJson("300", "100", "Release notes",
                                T1.toString())));
        for (String sub : List.of("footer-comments", "inline-comments", "attachments", "labels",
                "properties")) {
            fake.stub("/wiki/api/v2/blogposts/300/" + sub, ConfluenceFixtures.emptyListJson());
        }
    }

    private List<SyncResponse> sync(SyncRequest request) {
        List<SyncResponse> events = new ArrayList<>();
        stub.sync(request).forEachRemaining(events::add);
        return events;
    }

    @Test
    void syncFullCrawlStreamsChangesSnapshotAndResumeCursor() {
        stubWorkspace();

        List<SyncResponse> events = sync(SyncRequest.getDefaultInstance());

        // Every change is a well-formed UPSERT envelope (the platform
        // validation rules hold on the streamed wire).
        List<ConfluenceChange> changes = events.stream()
                .filter(SyncResponse::hasChange).map(SyncResponse::getChange).toList();
        assertThat(changes).isNotEmpty();
        assertThat(changes).allSatisfy(change -> {
            assertThat(change.getChangeId()).isNotBlank();
            assertThat(change.getEntity().getEntityId()).isNotBlank();
            assertThat(VALIDATOR.validate(change).violations()).isEmpty();
        });
        assertThat(changes).extracting(c -> c.getEntity().getEntityCase())
                .contains(ConfluenceEntity.EntityCase.SPACE,
                        ConfluenceEntity.EntityCase.PAGE,
                        ConfluenceEntity.EntityCase.BLOG_POST,
                        ConfluenceEntity.EntityCase.ATTACHMENT,
                        ConfluenceEntity.EntityCase.COMMENT);

        // One snapshot for the ENG sweep.
        List<ConfluenceSnapshot> snapshots = events.stream()
                .filter(SyncResponse::hasSnapshot).map(SyncResponse::getSnapshot).toList();
        assertThat(snapshots).hasSize(1);
        assertThat(snapshots.get(0).getSpaceKey()).isEqualTo("ENG");

        // The terminal message carries the resume cursor: the newest
        // modification observed (the page at T2).
        assertThat(events.getLast().getEventCase()).isEqualTo(SyncResponse.EventCase.RESUME_CURSOR);
        assertThat(Instant.parse(events.getLast().getResumeCursor())).isEqualTo(T2);
    }

    @Test
    void syncWithoutBodiesStripsContentBodies() {
        stubWorkspace();

        List<SyncResponse> events = sync(SyncRequest.newBuilder().setIncludeBodies(false).build());

        ConfluenceEntity page = events.stream()
                .filter(SyncResponse::hasChange).map(SyncResponse::getChange)
                .map(ConfluenceChange::getEntity)
                .filter(e -> e.getEntityCase() == ConfluenceEntity.EntityCase.PAGE)
                .findFirst().orElseThrow();
        assertThat(page.getPage().hasBody()).isFalse();
        assertThat(page.getPage().getTitle()).isEqualTo("Design Doc");
    }

    @Test
    void syncIncrementalResumesFromTheCursor() {
        fake.stub("/wiki/api/v2/spaces",
                ConfluenceFixtures.spaceListJson(null,
                        ConfluenceFixtures.spaceJson("100", "ENG", "Engineering")));
        fake.stub("/wiki/api/v2/spaces/100/properties", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages",
                ConfluenceFixtures.pageListJson(null,
                        ConfluenceFixtures.pageJson("203", "100", "Newest", T3.toString()),
                        ConfluenceFixtures.pageJson("202", "100", "Middle", T2.toString())));
        fake.stub("/wiki/api/v2/blogposts", ConfluenceFixtures.emptyListJson());
        for (String sub : List.of("footer-comments", "inline-comments", "attachments", "labels",
                "properties")) {
            fake.stub("/wiki/api/v2/pages/203/" + sub, ConfluenceFixtures.emptyListJson());
        }

        List<SyncResponse> events = sync(SyncRequest.newBuilder()
                .setSinceCursor(T2.toString()).setIncludeBodies(true).build());

        List<ConfluenceChange> changes = events.stream()
                .filter(SyncResponse::hasChange).map(SyncResponse::getChange).toList();
        assertThat(changes).hasSize(1);
        assertThat(changes.get(0).getEntity().getEntityId()).isEqualTo("203");
        assertThat(changes.get(0).getEntity().getPage().hasBody()).isTrue();
        assertThat(events.getLast().getEventCase()).isEqualTo(SyncResponse.EventCase.RESUME_CURSOR);
        assertThat(Instant.parse(events.getLast().getResumeCursor())).isEqualTo(T3);
    }

    // ======================================================================
    // REFLECTION
    // ======================================================================

    @Test
    void reflectionServesTheServiceDescriptor() throws Exception {
        ServerReflectionGrpc.ServerReflectionStub reflection =
                ServerReflectionGrpc.newStub(channel);
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<ServerReflectionResponse> response = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        StreamObserver<ServerReflectionRequest> requests = reflection.serverReflectionInfo(
                new StreamObserver<>() {
                    @Override
                    public void onNext(ServerReflectionResponse value) {
                        response.set(value);
                    }

                    @Override
                    public void onError(Throwable t) {
                        error.set(t);
                        done.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        done.countDown();
                    }
                });
        requests.onNext(ServerReflectionRequest.newBuilder()
                .setFileContainingSymbol("ai.pipestream.proto.acquire.confluence.v1.ConfluenceService")
                .build());
        requests.onCompleted();

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(error.get()).isNull();
        assertThat(response.get().hasFileDescriptorResponse()).isTrue();
        List<String> files = new ArrayList<>();
        for (com.google.protobuf.ByteString bytes
                : response.get().getFileDescriptorResponse().getFileDescriptorProtoList()) {
            files.add(DescriptorProtos.FileDescriptorProto.parseFrom(bytes).getName());
        }
        assertThat(files).contains(
                "ai/protomolt/proto/acquire/confluence/v1/confluence_service.proto");
    }
}
