package ai.pipestream.proto.intake.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.intake.service.identity.ApiKeyServerInterceptor;
import ai.pipestream.proto.intake.service.identity.InMemoryApiKeyIdentityResolver;
import ai.pipestream.proto.intake.service.identity.IntakeScope;
import ai.pipestream.proto.intake.v1.IngestDocumentRequest;
import ai.pipestream.proto.intake.v1.IngestDocumentResponse;
import ai.pipestream.proto.intake.v1.IngestMetadata;
import ai.pipestream.proto.intake.v1.IngestStreamRequest;
import ai.pipestream.proto.intake.v1.IngestStreamResponse;
import ai.pipestream.proto.intake.v1.IntakeServiceGrpc;
import ai.pipestream.proto.intake.v1.RawPayload;
import ai.pipestream.proto.repo.v1.Blob;
import ai.pipestream.proto.repo.v1.BlobBag;
import ai.pipestream.proto.repo.v1.DocIdDerivationMethod;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.OwnershipContext;
import ai.pipestream.proto.repo.v1.SaveDocumentRequest;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pins the intake wire contract against a recording fake repo: which
 * {@code SaveDocument} request each lane issues, what the receipt echoes, and
 * every UNAUTHENTICATED / PERMISSION_DENIED / INVALID_ARGUMENT split the
 * proto header promises.
 */
class IntakeSaveContractTest {

    static final String ACCOUNT = "acct-contract";
    static final String KEY_UNRESTRICTED = "key-unrestricted";
    static final String KEY_NARROW = "key-narrow";

    static FakeDocumentService repo;
    static Server repoServer;
    static IntakeServices intake;
    static Server intakeServer;
    static ManagedChannel channel;

    @BeforeAll
    static void boot() throws Exception {
        repo = new FakeDocumentService();
        String repoName = InProcessServerBuilder.generateName();
        repoServer =
                InProcessServerBuilder.forName(repoName)
                        .directExecutor()
                        .addService(repo)
                        .build()
                        .start();
        InMemoryApiKeyIdentityResolver resolver =
                new InMemoryApiKeyIdentityResolver()
                        .register(KEY_UNRESTRICTED, IntakeScope.unrestricted(ACCOUNT))
                        .register(
                                KEY_NARROW,
                                new IntakeScope(
                                        ACCOUNT,
                                        Set.of("ds-allowed"),
                                        Set.of("intake"),
                                        Set.of("text/plain"),
                                        16L));
        intake =
                IntakeServices.build(
                        new IntakeServiceConfig(
                                0,
                                IntakeServiceConfig.INPROCESS_TARGET_PREFIX + repoName,
                                1024L),
                        resolver);
        String intakeName = InProcessServerBuilder.generateName();
        intakeServer = intake.startInProcess(intakeName);
        channel = InProcessChannelBuilder.forName(intakeName).build();
    }

    @AfterAll
    static void shutdown() {
        channel.shutdownNow();
        intake.close();
        repoServer.shutdownNow();
    }

    @BeforeEach
    void reset() {
        repo.saves.clear();
        repo.deduplicated = false;
    }

    static IntakeServiceGrpc.IntakeServiceBlockingStub stub(String apiKey) {
        Metadata metadata = new Metadata();
        metadata.put(ApiKeyServerInterceptor.API_KEY, apiKey);
        return IntakeServiceGrpc.newBlockingStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    }

    static IntakeServiceGrpc.IntakeServiceStub asyncStub(String apiKey) {
        Metadata metadata = new Metadata();
        metadata.put(ApiKeyServerInterceptor.API_KEY, apiKey);
        return IntakeServiceGrpc.newStub(channel)
                .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    }

    @Test
    void rawIngestIssuesTheIntakeSaveArmAndDerivesTheDocIdFromContent() throws Exception {
        byte[] payload = "the quick brown fox".getBytes();
        IngestDocumentResponse receipt =
                stub(KEY_UNRESTRICTED)
                        .ingestDocument(
                                IngestDocumentRequest.newBuilder()
                                        .setRaw(
                                                RawPayload.newBuilder()
                                                        .setData(ByteString.copyFrom(payload))
                                                        .setFilename("fox.txt")
                                                        .setMimeType("text/plain"))
                                        .setDatasourceId("ds-1")
                                        .putMetadata("origin", "contract-test")
                                        .build());

        assertThat(repo.saves).hasSize(1);
        SaveDocumentRequest save = repo.saves.getFirst();
        assertThat(save.getUseDatasourceId()).isTrue();
        assertThat(save.getGraphId()).isEqualTo("intake:" + ACCOUNT);
        assertThat(save.getDrive()).isEqualTo("intake");
        assertThat(save.getConnectorId()).isEqualTo("intake");
        assertThat(save.getWrittenBy().getModuleId()).isEqualTo("intake");
        assertThat(save.getMetadataMap()).containsEntry("origin", "contract-test");

        Document saved = save.getDocument();
        assertThat(saved.getOwnership().getAccountId()).isEqualTo(ACCOUNT);
        assertThat(saved.getOwnership().getDatasourceId()).isEqualTo("ds-1");
        assertThat(saved.getBlobBag().getBlob().getData().toByteArray()).isEqualTo(payload);
        assertThat(saved.getBlobBag().getBlob().getFilename()).isEqualTo("fox.txt");
        assertThat(saved.getSearchMetadata().getSourceMimeType()).isEqualTo("text/plain");
        assertThat(saved.getDocIdDerivation().getMethod())
                .isEqualTo(DocIdDerivationMethod.DOC_ID_DERIVATION_METHOD_CONTENT_HASH);

        byte[] digest = MessageDigest.getInstance("SHA-256").digest(payload);
        String expectedSha = HexFormat.of().formatHex(digest);
        String expectedDocId = UUID.nameUUIDFromBytes(digest).toString();
        assertThat(saved.getDocId()).isEqualTo(expectedDocId);
        assertThat(receipt.getDocId()).isEqualTo(expectedDocId);
        assertThat(receipt.getSha256()).isEqualTo(expectedSha);
        assertThat(receipt.getSizeBytes()).isEqualTo(payload.length);
        assertThat(receipt.getAddress().getGraphId()).isEqualTo("intake:" + ACCOUNT);
        assertThat(receipt.getDeduplicated()).isFalse();
    }

    @Test
    void typedDocumentIngestOverridesOwnershipWithTheKeyScope() {
        Document caller =
                Document.newBuilder()
                        .setDocId("caller-doc")
                        .setOwnership(
                                OwnershipContext.newBuilder()
                                        .setAccountId("someone-else")
                                        .setDatasourceId("their-ds")
                                        .setConnectorId("their-connector"))
                        .setBlobBag(
                                BlobBag.newBuilder()
                                        .setBlob(
                                                Blob.newBuilder()
                                                        .setBlobId("b1")
                                                        .setData(ByteString.copyFromUtf8("typed"))
                                                        .setMimeType("text/plain")))
                        .build();

        IngestDocumentResponse receipt =
                stub(KEY_UNRESTRICTED)
                        .ingestDocument(
                                IngestDocumentRequest.newBuilder()
                                        .setDocument(caller)
                                        .setDatasourceId("ds-2")
                                        .build());

        Document saved = repo.saves.getFirst().getDocument();
        assertThat(saved.getOwnership().getAccountId()).isEqualTo(ACCOUNT);
        assertThat(saved.getOwnership().getDatasourceId()).isEqualTo("ds-2");
        // Non-ownership caller fields survive.
        assertThat(saved.getOwnership().getConnectorId()).isEqualTo("their-connector");
        assertThat(saved.getDocId()).isEqualTo("caller-doc");
        assertThat(saved.getDocIdDerivation().getMethod())
                .isEqualTo(DocIdDerivationMethod.DOC_ID_DERIVATION_METHOD_CALLER_PROVIDED);
        assertThat(receipt.getDocId()).isEqualTo("caller-doc");
        assertThat(receipt.getSha256()).isNotBlank();
    }

    @Test
    void missingContentAndMissingDatasourceAreInvalidArgument() {
        assertThatThrownBy(
                        () ->
                                stub(KEY_UNRESTRICTED)
                                        .ingestDocument(
                                                IngestDocumentRequest.newBuilder()
                                                        .setDatasourceId("ds-1")
                                                        .build()))
                .isInstanceOfSatisfying(
                        StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));

        assertThatThrownBy(
                        () ->
                                stub(KEY_UNRESTRICTED)
                                        .ingestDocument(
                                                IngestDocumentRequest.newBuilder()
                                                        .setRaw(RawPayload.newBuilder().setData(ByteString.copyFromUtf8("x")))
                                                        .build()))
                .isInstanceOfSatisfying(
                        StatusRuntimeException.class,
                        e -> {
                            assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                            assertThat(e.getStatus().getDescription()).contains("datasource_id");
                        });
    }

    @Test
    void scopeViolationsArePermissionDeniedNotUnauthenticated() {
        // Datasource outside the narrow key's allowlist.
        assertThatThrownBy(() -> ingestNarrow("ds-forbidden", "intake", "text/plain", "x"))
                .isInstanceOfSatisfying(
                        StatusRuntimeException.class,
                        e -> {
                            assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
                            assertThat(e.getStatus().getDescription()).contains("ds-forbidden");
                        });
        // Drive outside the allowlist.
        assertThatThrownBy(() -> ingestNarrow("ds-allowed", "pipeline", "text/plain", "x"))
                .isInstanceOfSatisfying(
                        StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED));
        // Declared MIME type outside the restriction.
        assertThatThrownBy(() -> ingestNarrow("ds-allowed", "intake", "application/pdf", "x"))
                .isInstanceOfSatisfying(
                        StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED));
        // A content-type-restricted key demands a declared type.
        assertThatThrownBy(() -> ingestNarrow("ds-allowed", "intake", "", "x"))
                .isInstanceOfSatisfying(
                        StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED));
        assertThat(repo.saves).isEmpty();
    }

    @Test
    void payloadCapsAreResourceExhausted() {
        // Per-key cap (16 bytes on the narrow key).
        assertThatThrownBy(
                        () -> ingestNarrow("ds-allowed", "intake", "text/plain", "x".repeat(17)))
                .isInstanceOfSatisfying(
                        StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
        // Service cap (1024 bytes in this rig) on an otherwise unrestricted key.
        assertThatThrownBy(
                        () ->
                                stub(KEY_UNRESTRICTED)
                                        .ingestDocument(
                                                IngestDocumentRequest.newBuilder()
                                                        .setRaw(
                                                                RawPayload.newBuilder()
                                                                        .setData(
                                                                                ByteString.copyFromUtf8(
                                                                                        "y".repeat(2048))))
                                                        .setDatasourceId("ds-1")
                                                        .build()))
                .isInstanceOfSatisfying(
                        StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
        assertThat(repo.saves).isEmpty();
    }

    @Test
    void missingAndUnknownKeysAreUnauthenticated() {
        assertThatThrownBy(
                        () ->
                                IntakeServiceGrpc.newBlockingStub(channel)
                                        .ingestDocument(rawRequest("ds-1", "hello")))
                .isInstanceOfSatisfying(
                        StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED));
        assertThatThrownBy(() -> stub("no-such-key").ingestDocument(rawRequest("ds-1", "hello")))
                .isInstanceOfSatisfying(
                        StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED));
    }

    @Test
    void bearerAuthorizationHeaderAuthenticates() {
        Metadata metadata = new Metadata();
        metadata.put(ApiKeyServerInterceptor.AUTHORIZATION, "Bearer " + KEY_UNRESTRICTED);
        IngestDocumentResponse receipt =
                IntakeServiceGrpc.newBlockingStub(channel)
                        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
                        .ingestDocument(rawRequest("ds-1", "bearer works"));
        assertThat(receipt.getDocId()).isNotBlank();
    }

    @Test
    void streamLaneAssemblesFramesInOrderAndAnswersTheSameReceipt() throws Exception {
        IngestStreamResponse receipt =
                streamIngest(
                        KEY_UNRESTRICTED,
                        IngestMetadata.newBuilder()
                                .setDatasourceId("ds-stream")
                                .setFilename("stream.txt")
                                .setMimeType("text/plain")
                                .build(),
                        "part one|", "part two");

        byte[] expected = "part one|part two".getBytes();
        assertThat(repo.saves).hasSize(1);
        assertThat(repo.saves.getFirst().getDocument().getBlobBag().getBlob().getData().toByteArray())
                .isEqualTo(expected);
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(expected);
        assertThat(receipt.getSha256()).isEqualTo(HexFormat.of().formatHex(digest));
        assertThat(receipt.getSizeBytes()).isEqualTo(expected.length);
        assertThat(receipt.getDocId()).isEqualTo(UUID.nameUUIDFromBytes(digest).toString());
    }

    @Test
    void streamFrameDisciplineViolationsAreInvalidArgument() throws Exception {
        // Data before metadata.
        StatusRuntimeException first =
                streamFailure(
                        asyncObserverCall(
                                KEY_UNRESTRICTED,
                                IngestStreamRequest.newBuilder()
                                        .setData(ByteString.copyFromUtf8("data-first"))
                                        .build()));
        assertThat(first.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);

        // Second metadata frame.
        StatusRuntimeException second =
                streamFailure(
                        asyncObserverCall(
                                KEY_UNRESTRICTED,
                                IngestStreamRequest.newBuilder()
                                        .setMetadata(IngestMetadata.newBuilder().setDatasourceId("ds-1"))
                                        .build(),
                                IngestStreamRequest.newBuilder()
                                        .setMetadata(IngestMetadata.newBuilder().setDatasourceId("ds-1"))
                                        .build()));
        assertThat(second.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(repo.saves).isEmpty();
    }

    @Test
    void dedupeEchoesThroughTheReceipt() {
        repo.deduplicated = true;
        IngestDocumentResponse receipt =
                stub(KEY_UNRESTRICTED).ingestDocument(rawRequest("ds-1", "already stored"));
        assertThat(receipt.getDeduplicated()).isTrue();
    }

    // ------------------------------------------------------------------
    // helpers

    static IngestDocumentRequest rawRequest(String datasourceId, String payload) {
        return IngestDocumentRequest.newBuilder()
                .setRaw(
                        RawPayload.newBuilder()
                                .setData(ByteString.copyFromUtf8(payload))
                                .setMimeType("text/plain"))
                .setDatasourceId(datasourceId)
                .build();
    }

    void ingestNarrow(String datasourceId, String drive, String mimeType, String payload) {
        stub(KEY_NARROW)
                .ingestDocument(
                        IngestDocumentRequest.newBuilder()
                                .setRaw(
                                        RawPayload.newBuilder()
                                                .setData(ByteString.copyFromUtf8(payload))
                                                .setMimeType(mimeType))
                                .setDatasourceId(datasourceId)
                                .setDrive(drive)
                                .build());
    }

    IngestStreamResponse streamIngest(String key, IngestMetadata metadata, String... parts)
            throws Exception {
        AtomicReference<IngestStreamResponse> response = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<IngestStreamRequest> requests =
                asyncStub(key)
                        .ingestStream(
                                new StreamObserver<>() {
                                    @Override
                                    public void onNext(IngestStreamResponse value) {
                                        response.set(value);
                                    }

                                    @Override
                                    public void onError(Throwable t) {
                                        failure.set(t);
                                        done.countDown();
                                    }

                                    @Override
                                    public void onCompleted() {
                                        done.countDown();
                                    }
                                });
        requests.onNext(IngestStreamRequest.newBuilder().setMetadata(metadata).build());
        for (String part : parts) {
            requests.onNext(
                    IngestStreamRequest.newBuilder().setData(ByteString.copyFromUtf8(part)).build());
        }
        requests.onCompleted();
        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        if (failure.get() != null) {
            throw new AssertionError("stream failed", failure.get());
        }
        return response.get();
    }

    /** Sends the given frames, half-closes, and returns the terminal failure. */
    Throwable asyncObserverCall(String key, IngestStreamRequest... frames) throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<IngestStreamRequest> requests =
                asyncStub(key)
                        .ingestStream(
                                new StreamObserver<>() {
                                    @Override
                                    public void onNext(IngestStreamResponse value) {}

                                    @Override
                                    public void onError(Throwable t) {
                                        failure.set(t);
                                        done.countDown();
                                    }

                                    @Override
                                    public void onCompleted() {
                                        done.countDown();
                                    }
                                });
        try {
            for (IngestStreamRequest frame : frames) {
                requests.onNext(frame);
            }
            requests.onCompleted();
        } catch (IllegalStateException alreadyClosed) {
            // The server rejected an earlier frame and cancelled the call
            // before the client finished sending; the terminal error still
            // arrives on the response observer.
        }
        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        return failure.get();
    }

    static StatusRuntimeException streamFailure(Throwable t) {
        assertThat(t).isInstanceOf(StatusRuntimeException.class);
        return (StatusRuntimeException) t;
    }
}
