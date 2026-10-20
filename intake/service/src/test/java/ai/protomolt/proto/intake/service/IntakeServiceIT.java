package ai.protomolt.proto.intake.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.intake.service.identity.ApiKeyServerInterceptor;
import ai.protomolt.proto.intake.service.identity.InMemoryApiKeyIdentityResolver;
import ai.protomolt.proto.intake.service.identity.IntakeScope;
import ai.protomolt.proto.intake.v1.IngestDocumentRequest;
import ai.protomolt.proto.intake.v1.IngestDocumentResponse;
import ai.protomolt.proto.intake.v1.IntakeServiceGrpc;
import ai.protomolt.proto.intake.v1.RawPayload;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.GetDocumentResponse;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end integration test of the intake service against a REAL repo-service:
 * testcontainers PostgreSQL 18 + LocalStack S3 behind {@link V2RepositoryFixture},
 * intake mounted in front over the in-process transport — the all-in-one
 * embedding path, no fakes anywhere. Proves the receipt is honest: what
 * intake acknowledges is fetchable back out of the repository at the
 * receipt's address, and identical content dedupes.
 */
@Testcontainers
class IntakeServiceIT {

    static final String ACCOUNT = "acct-intake-it";
    static final String API_KEY = "it-key";

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    static V2RepositoryFixture repo;
    static IntakeServices intake;
    static ManagedChannel repoChannel;
    static ManagedChannel intakeChannel;
    static IntakeServiceGrpc.IntakeServiceBlockingStub intakeStub;

    @BeforeAll
    static void boot() throws Exception {
        repo = new V2RepositoryFixture(POSTGRES, LOCALSTACK, "intake-it-repo");
        repoChannel = InProcessChannelBuilder.forName("intake-it-repo").build();

        intake =
                IntakeServices.build(
                        new IntakeServiceConfig(
                                0,
                                IntakeServiceConfig.INPROCESS_TARGET_PREFIX + "intake-it-repo",
                                IntakeServiceConfig.DEFAULT_MAX_PAYLOAD_BYTES),
                        new InMemoryApiKeyIdentityResolver()
                                .register(API_KEY, IntakeScope.unrestricted(ACCOUNT)));
        intake.startInProcess("intake-it-service");
        intakeChannel = InProcessChannelBuilder.forName("intake-it-service").build();
        Metadata metadata = new Metadata();
        metadata.put(ApiKeyServerInterceptor.API_KEY, API_KEY);
        intakeStub =
                IntakeServiceGrpc.newBlockingStub(intakeChannel)
                        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    }

    @AfterAll
    static void shutdown() {
        intakeChannel.shutdownNow();
        intake.close();
        repoChannel.shutdownNow();
        repo.close();
    }

    @Test
    void ingestedPayloadIsFetchableAtTheReceiptAddressAndDedupes() throws Exception {
        byte[] payload = "intake integration payload".getBytes();
        IngestDocumentRequest request =
                IngestDocumentRequest.newBuilder()
                        .setRaw(
                                RawPayload.newBuilder()
                                        .setData(ByteString.copyFrom(payload))
                                        .setFilename("it.txt")
                                        .setMimeType("text/plain"))
                        .setDatasourceId("ds-it")
                        .build();

        IngestDocumentResponse receipt = intakeStub.ingestDocument(request);
        assertThat(receipt.getDocId()).isNotBlank();
        assertThat(receipt.getRevision()).isEqualTo(1);
        assertThat(receipt.getDrive()).isEqualTo("intake");
        assertThat(receipt.getDeduplicated()).isFalse();
        assertThat(receipt.getAddress().getAccountId()).isEqualTo(ACCOUNT);
        assertThat(receipt.getAddress().getGraphId()).isEqualTo("intake:" + ACCOUNT);
        assertThat(receipt.getAddress().getGraphAddressId()).isEqualTo("ds-it");

        // The receipt is honest: the document is fetchable at its address.
        GetDocumentResponse fetched =
                ai.protomolt.proto.repo.v2.DocumentServiceGrpc.newBlockingStub(repoChannel)
                        .getDocument(
                                GetDocumentRequest.newBuilder()
                                        .setAddress(receipt.getAddress())
                                        .build());
        assertThat(repo.retainedBlob(receipt.getAddress(), fetched.getManifest().getRevision()))
                .isEqualTo(payload);
        assertThat(fetched.getDocument().getOwnership().getAccountId()).isEqualTo(ACCOUNT);
        assertThat(fetched.getDocument().getOwnership().getDatasourceId()).isEqualTo("ds-it");

        assertThat(fetched.getManifest().getRevision()).isEqualTo(1);
        assertThat(fetched.getDocument().getBlobBag().getBlob().hasStorageRef()).isTrue();

        // Identical content dedupes; the existing coordinates are echoed.
        IngestDocumentResponse again = intakeStub.ingestDocument(request);
        assertThat(again.getDeduplicated()).isTrue();
        assertThat(again.getDocId()).isEqualTo(receipt.getDocId());
        assertThat(again.getRevision()).isEqualTo(receipt.getRevision());
        assertThat(repo.catalog.revisions(receipt.getAddress(), Long.MAX_VALUE, 10)).hasSize(1);
    }

    @Test
    void httpLaneUploadIsFetchableAtTheReceiptAddress() throws Exception {
        try (IntakeHttpServer http = intake.startHttp(0)) {
            byte[] payload = "http lane integration payload".getBytes();
            java.net.http.HttpResponse<String> response =
                    java.net.http.HttpClient.newHttpClient()
                            .send(
                                    java.net.http.HttpRequest.newBuilder(
                                                    java.net.URI.create(
                                                            "http://localhost:" + http.port()
                                                                    + IntakeHttpServer.UPLOAD_PATH
                                                                    + "?datasource_id=ds-http-it&filename=it-http.txt"))
                                            .header("x-api-key", API_KEY)
                                            .header("Content-Type", "text/plain")
                                            .POST(
                                                    java.net.http.HttpRequest.BodyPublishers
                                                            .ofByteArray(payload))
                                            .build(),
                                    java.net.http.HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            IngestDocumentResponse.Builder receipt = IngestDocumentResponse.newBuilder();
            com.google.protobuf.util.JsonFormat.parser().merge(response.body(), receipt);
            assertThat(receipt.getDocId()).isNotBlank();
            assertThat(receipt.getSizeBytes()).isEqualTo(payload.length);
            assertThat(receipt.getDrive()).isEqualTo("intake");
            assertThat(receipt.getAddress().getAccountId()).isEqualTo(ACCOUNT);
            assertThat(receipt.getAddress().getGraphId()).isEqualTo("intake:" + ACCOUNT);
            assertThat(receipt.getAddress().getGraphAddressId()).isEqualTo("ds-http-it");

            // The receipt is honest: the payload is fetchable at its address.
            GetDocumentResponse fetched =
                    ai.protomolt.proto.repo.v2.DocumentServiceGrpc.newBlockingStub(repoChannel)
                            .getDocument(
                                    GetDocumentRequest.newBuilder()
                                            .setAddress(receipt.getAddress())
                                            .build());
            assertThat(repo.retainedBlob(receipt.getAddress(), fetched.getManifest().getRevision()))
                    .isEqualTo(payload);
            assertThat(fetched.getDocument().getOwnership().getDatasourceId())
                    .isEqualTo("ds-http-it");
        }
    }

    @Test
    void upsertCreatesDeduplicatesAndVersionsWithoutRewritingIdenticalBytes() throws Exception {
        var doc = ai.protomolt.proto.repo.v1.Document.newBuilder().setDocId("upsert-source")
                .setBlobBag(ai.protomolt.proto.repo.v1.BlobBag.newBuilder().setBlob(
                        ai.protomolt.proto.repo.v1.Blob.newBuilder().setBlobId("source")
                                .setData(ByteString.copyFromUtf8("first bytes")))).build();
        var request = IngestDocumentRequest.newBuilder().setDatasourceId("ds-upsert")
                .setMode(ai.protomolt.proto.intake.v1.IngestMode.INGEST_MODE_UPSERT).setDocument(doc).build();
        var created = intakeStub.ingestDocument(request);
        assertThat(created.getRevision()).isEqualTo(1);
        int before = repo.client.listObjectsV2(b -> b.bucket("intake-it-repo")).keyCount();
        var duplicate = intakeStub.ingestDocument(request);
        assertThat(duplicate.getRevision()).isEqualTo(1);
        assertThat(duplicate.getDeduplicated()).isTrue();
        assertThat(repo.client.listObjectsV2(b -> b.bucket("intake-it-repo")).keyCount()).isEqualTo(before);
        var metadataChanged = request.toBuilder().putMetadata("crawl", "second").build();
        var second = intakeStub.ingestDocument(metadataChanged);
        assertThat(second.getRevision()).isEqualTo(2);
        assertThat(second.getDeduplicated()).isFalse();
        assertThat(repo.client.listObjectsV2(b -> b.bucket("intake-it-repo")).keyCount()).isEqualTo(before);
        var changed = metadataChanged.toBuilder().setDocument(doc.toBuilder().setBlobBag(
                doc.getBlobBag().toBuilder().setBlob(doc.getBlobBag().getBlob().toBuilder()
                        .setData(ByteString.copyFromUtf8("other bytes"))))).build();
        var third = intakeStub.ingestDocument(changed);
        assertThat(third.getRevision()).isEqualTo(3);
        assertThat(repo.retainedBlob(created.getAddress(), 1)).isEqualTo("first bytes".getBytes());
        assertThat(repo.retainedBlob(created.getAddress(), 3)).isEqualTo("other bytes".getBytes());
        assertThat(intakeStub.ingestDocument(changed).getRevision()).isEqualTo(3);
    }

    @Test
    void upsertSurfacesAConcurrentUpdateWithoutRetryingTheSave() throws Exception {
        var initial = IngestDocumentRequest.newBuilder().setDatasourceId("ds-upsert-race")
                .setDocument(ai.protomolt.proto.repo.v1.Document.newBuilder().setDocId("upsert-race")
                        .setSearchMetadata(ai.protomolt.proto.repo.v1.SearchMetadata.newBuilder().setTitle("first")))
                .build();
        var created = intakeStub.ingestDocument(initial);
        var documents = ai.protomolt.proto.repo.v2.DocumentServiceGrpc.newBlockingStub(repoChannel);
        var original = documents.getDocument(GetDocumentRequest.newBuilder().setAddress(created.getAddress()).build());
        var saves = new java.util.concurrent.atomic.AtomicInteger();
        // A forwarding transport places a real competing commit precisely after the read.
        // Both writes and all reads still use the real PostgreSQL/S3 repository.
        var forwarding = new ai.protomolt.proto.repo.v2.DocumentServiceGrpc.DocumentServiceImplBase() {
            @Override public void getDocument(GetDocumentRequest request,
                    io.grpc.stub.StreamObserver<GetDocumentResponse> observer) {
                var snapshot = documents.getDocument(request);
                intakeStub.ingestDocument(initial.toBuilder().setExpected(
                        ai.protomolt.proto.repo.v2.ExpectedRevision.newBuilder().setRevision(1))
                        .setDocument(initial.getDocument().toBuilder().setSearchMetadata(
                                ai.protomolt.proto.repo.v1.SearchMetadata.newBuilder().setTitle("winner"))).build());
                observer.onNext(snapshot);
                observer.onCompleted();
            }
            @Override public void saveDocument(ai.protomolt.proto.repo.v2.SaveDocumentRequest request,
                    io.grpc.stub.StreamObserver<ai.protomolt.proto.repo.v2.SaveDocumentResponse> observer) {
                saves.incrementAndGet();
                try {
                    observer.onNext(documents.saveDocument(request));
                    observer.onCompleted();
                } catch (StatusRuntimeException failure) { observer.onError(failure); }
            }
        };
        var name = io.grpc.inprocess.InProcessServerBuilder.generateName();
        var server = io.grpc.inprocess.InProcessServerBuilder.forName(name).addService(forwarding).build().start();
        var channel = InProcessChannelBuilder.forName(name).build();
        try {
            var ingest = new RawIngests(ai.protomolt.proto.repo.v2.DocumentServiceGrpc.newBlockingStub(channel),
                    IntakeServiceConfig.DEFAULT_MAX_PAYLOAD_BYTES);
            var scope = IntakeScope.unrestricted(ACCOUNT);
            var candidate = original.getDocument().toBuilder().setSearchMetadata(
                    ai.protomolt.proto.repo.v1.SearchMetadata.newBuilder().setTitle("loser")).build();
            assertThatThrownBy(() -> ingest.save(scope, RawIngests.Targeting.of(scope, "ds-upsert-race", "intake"),
                    candidate, java.util.Map.of(), "", 0, null))
                    .isInstanceOfSatisfying(StatusRuntimeException.class,
                            failure -> assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.ABORTED));
            assertThat(saves.get()).isEqualTo(1);
            assertThat(repo.catalog.revisions(created.getAddress(), Long.MAX_VALUE, 10)).hasSize(2);
            assertThat(documents.getDocument(GetDocumentRequest.newBuilder().setAddress(created.getAddress()).build())
                    .getDocument().getSearchMetadata().getTitle()).isEqualTo("winner");
        } finally { channel.shutdownNow(); server.shutdownNow(); }
    }

    @Test
    void updateUsesObservedRevisionAndRetryKeepsItsReceipt() throws Exception {
        var firstRequest = IngestDocumentRequest.newBuilder().setDatasourceId("ds-update")
                .setDocument(ai.protomolt.proto.repo.v1.Document.newBuilder().setDocId("update-doc")
                        .setSearchMetadata(ai.protomolt.proto.repo.v1.SearchMetadata.newBuilder().setTitle("first")))
                .build();
        var first = intakeStub.ingestDocument(firstRequest);
        var update = firstRequest.toBuilder().setExpected(
                ai.protomolt.proto.repo.v2.ExpectedRevision.newBuilder().setRevision(first.getRevision()))
                .setDocument(firstRequest.getDocument().toBuilder().setSearchMetadata(
                        firstRequest.getDocument().getSearchMetadata().toBuilder().setTitle("second")))
                .build();
        var second = intakeStub.ingestDocument(update);
        assertThat(second.getRevision()).isEqualTo(2);
        var replay = intakeStub.ingestDocument(update);
        assertThat(replay.getRevision()).isEqualTo(second.getRevision());
        assertThat(replay.getDeduplicated()).isTrue();
        assertThat(repo.catalog.revisions(first.getAddress(), Long.MAX_VALUE, 10)).hasSize(2);
        var documents = ai.protomolt.proto.repo.v2.DocumentServiceGrpc.newBlockingStub(repoChannel);
        assertThat(documents.getDocument(GetDocumentRequest.newBuilder().setAddress(first.getAddress())
                .setRevision(ai.protomolt.proto.repo.v2.RevisionSelector.newBuilder().setRevision(1)).build())
                .getDocument().getSearchMetadata().getTitle()).isEqualTo("first");
        assertThat(documents.getDocument(GetDocumentRequest.newBuilder().setAddress(first.getAddress()).build())
                .getDocument().getSearchMetadata().getTitle()).isEqualTo("second");
        var stale = update.toBuilder().putMetadata("different", "intent").build();
        assertThatThrownBy(() -> intakeStub.ingestDocument(stale))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.ABORTED));
        assertThat(repo.catalog.revisions(first.getAddress(), Long.MAX_VALUE, 10)).hasSize(2);
    }

    @Test
    void identicalBinaryReusesStoredObjectsEvenWhenMetadataCreatesANewRevision() throws Exception {
        var request = IngestDocumentRequest.newBuilder().setDatasourceId("ds-sha-reuse")
                .setRaw(RawPayload.newBuilder().setData(ByteString.copyFromUtf8("same bytes"))
                        .setMimeType("text/plain").setFilename("same.txt")).build();
        var first = intakeStub.ingestDocument(request);
        var original = repo.catalog.manifest(first.getAddress(), first.getRevision()).orElseThrow();
        int objectsBefore = repo.client.listObjectsV2(b -> b.bucket("intake-it-repo")).keyCount();
        var update = request.toBuilder().putMetadata("crawler", "new observation")
                .setExpected(ai.protomolt.proto.repo.v2.ExpectedRevision.newBuilder().setRevision(first.getRevision()))
                .build();
        var second = intakeStub.ingestDocument(update);
        var current = repo.catalog.manifest(first.getAddress(), second.getRevision()).orElseThrow();
        assertThat(second.getRevision()).isEqualTo(2);
        assertThat(current.getMetadataOrThrow("crawler")).isEqualTo("new observation");
        // A fresh revision is not an operation replay, even though all binary objects were reused.
        assertThat(second.getDeduplicated()).isFalse();
        assertThat(current.getPartsList().stream().map(p -> p.getRef()).toList())
                .isEqualTo(original.getPartsList().stream().map(p -> p.getRef()).toList());
        assertThat(current.getPartsList().stream().flatMap(p -> p.getBlobRefsList().stream()).toList())
                .isEqualTo(original.getPartsList().stream().flatMap(p -> p.getBlobRefsList().stream()).toList());
        assertThat(repo.client.listObjectsV2(b -> b.bucket("intake-it-repo")).keyCount()).isEqualTo(objectsBefore);
        assertThat(repo.retainedBlob(first.getAddress(), 1)).isEqualTo("same bytes".getBytes());
        assertThat(repo.retainedBlob(second.getAddress(), 2)).isEqualTo("same bytes".getBytes());
        assertThat(intakeStub.ingestDocument(update).getRevision()).isEqualTo(2);
        assertThat(repo.catalog.revisions(first.getAddress(), Long.MAX_VALUE, 10)).hasSize(2);
        var documents = ai.protomolt.proto.repo.v2.DocumentServiceGrpc.newBlockingStub(repoChannel);
        var retained = documents.getDocument(GetDocumentRequest.newBuilder().setAddress(first.getAddress()).build())
                .getDocument();
        var changedBytes = ByteString.copyFromUtf8("diff bytes");
        var changed = retained.toBuilder().setBlobBag(retained.getBlobBag().toBuilder().setBlob(
                retained.getBlobBag().getBlob().toBuilder().clearStorageRef().setData(changedBytes)
                        .setChecksum(RawIngests.sha256Hex(changedBytes.toByteArray())))).build();
        var third = intakeStub.ingestDocument(IngestDocumentRequest.newBuilder().setDatasourceId("ds-sha-reuse")
                .setExpected(ai.protomolt.proto.repo.v2.ExpectedRevision.newBuilder().setRevision(2))
                .setDocument(changed).build());
        assertThat(third.getRevision()).isEqualTo(3);
        assertThat(repo.retainedBlob(first.getAddress(), 1)).isEqualTo("same bytes".getBytes());
        assertThat(repo.retainedBlob(third.getAddress(), 3)).isEqualTo("diff bytes".getBytes());
        assertThat(intakeStub.ingestDocument(update).getRevision()).isEqualTo(2);
        assertThat(repo.catalog.revisions(first.getAddress(), Long.MAX_VALUE, 10)).hasSize(3);
    }

    @Test
    void delayedDeleteCannotRemoveANewerUpdateAndCurrentDeleteReplays() {
        var create = IngestDocumentRequest.newBuilder().setDatasourceId("ds-delete")
                .setDocument(ai.protomolt.proto.repo.v1.Document.newBuilder().setDocId("delete-doc")
                        .setSearchMetadata(ai.protomolt.proto.repo.v1.SearchMetadata.newBuilder().setTitle("first")))
                .build();
        var first = intakeStub.ingestDocument(create);
        var delayed = ai.protomolt.proto.repo.v2.DeleteDocumentRequest.newBuilder().setOperationId("old-delete")
                .setAddress(first.getAddress()).setExpected(
                        ai.protomolt.proto.repo.v2.ExpectedRevision.newBuilder().setRevision(first.getRevision())).build();
        var update = create.toBuilder().setExpected(delayed.getExpected())
                .setDocument(create.getDocument().toBuilder().setSearchMetadata(
                        create.getDocument().getSearchMetadata().toBuilder().setTitle("newer"))).build();
        var second = intakeStub.ingestDocument(update);
        assertThatThrownBy(() -> intakeStub.deleteDocument(delayed))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.ABORTED));
        var documents = ai.protomolt.proto.repo.v2.DocumentServiceGrpc.newBlockingStub(repoChannel);
        assertThat(documents.getDocument(GetDocumentRequest.newBuilder().setAddress(first.getAddress()).build())
                .getDocument().getSearchMetadata().getTitle()).isEqualTo("newer");
        var currentDelete = delayed.toBuilder().setOperationId("current-delete").setExpected(
                ai.protomolt.proto.repo.v2.ExpectedRevision.newBuilder().setRevision(second.getRevision())).build();
        var deleted = intakeStub.deleteDocument(currentDelete);
        assertThat(deleted.getAccession().getTombstone()).isTrue();
        assertThat(deleted.getAccession().getRevision()).isEqualTo(3);
        var replay = intakeStub.deleteDocument(currentDelete);
        assertThat(replay.getReplayed()).isTrue();
        assertThat(replay.getAccession()).isEqualTo(deleted.getAccession());
        assertThat(documents.getDocument(GetDocumentRequest.newBuilder().setAddress(first.getAddress())
                .setRevision(ai.protomolt.proto.repo.v2.RevisionSelector.newBuilder().setRevision(2)).build())
                .getDocument().getSearchMetadata().getTitle()).isEqualTo("newer");
    }

    @Test
    void unknownDriveSurfacesRepoErrorUnchanged() {
        assertThatThrownBy(
                        () ->
                                intakeStub.ingestDocument(
                                        IngestDocumentRequest.newBuilder()
                                                .setRaw(
                                                        RawPayload.newBuilder()
                                                                .setData(ByteString.copyFromUtf8("x")))
                                                .setDatasourceId("ds-it")
                                                .setDrive("no-such-drive")
                                                .build()))
                .isInstanceOfSatisfying(
                        StatusRuntimeException.class,
                        e ->
                                assertThat(e.getStatus().getCode())
                                        .isEqualTo(Status.Code.INVALID_ARGUMENT));
    }
}
