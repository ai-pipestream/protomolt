package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.authz.CredentialBinding;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.Blobs;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.FileStorageReference;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.ParseStatus;
import ai.protomolt.proto.repo.v1.ParserResult;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.*;
import com.google.protobuf.ByteString;
import com.sun.net.httpserver.HttpServer;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Integration tests for {@code UploadDocument}, {@code ReadDocument}, {@code ReadBlob},
 * and {@link UploadHttpHandler} (requirements §6, §7, §8, §11, §15.10, §15.11, §15.12, §15.17).
 */
class StreamingUploadAndReadIT {
    private AccessionFixture f;
    private String account;
    private Server server;
    private ManagedChannel channel;
    private DocumentServiceGrpc.DocumentServiceBlockingStub blockingStub;
    private DocumentServiceGrpc.DocumentServiceStub asyncStub;
    private final AtomicReference<RepositoryCaller> activeCaller = new AtomicReference<>(processAuthority());

    @BeforeEach
    void setUp() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
        var serverName = "streaming-" + UUID.randomUUID();
        var service = new AccessionGrpcService(f.saves, f.uploads, f.reads, ignored -> activeCaller.get());
        var callerInterceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call,
                    Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                var current = activeCaller.get();
                var caller = new Caller(current.principalName(), Set.of(), current.processAuthority());
                var binding = current.credentialBinding().map(b ->
                        new CredentialBinding(b.issuer(), b.credentialId(), b.generation()));
                var auth = new AuthenticatedCaller(caller, binding);
                var grpcCtx = Context.current()
                        .withValue(CallerContexts.CALLER, caller)
                        .withValue(CallerContexts.AUTHENTICATED_CALLER, auth);
                return Contexts.interceptCall(grpcCtx, call, headers, next);
            }
        };
        server = InProcessServerBuilder.forName(serverName)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(io.grpc.ServerInterceptors.intercept(service, callerInterceptor))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(serverName)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
        blockingStub = DocumentServiceGrpc.newBlockingStub(channel);
        asyncStub = DocumentServiceGrpc.newStub(channel);
    }

    @AfterEach
    void tearDown() {
        if (channel != null) channel.shutdownNow();
        if (server != null) server.shutdownNow();
        if (f != null) f.close();
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void listenerSetupFailureClosesPreparedBlob(boolean failCancelHandler, boolean failClose) throws Exception {
        var address = address(account, "listener-failure");
        f.saves.save(processAuthority(), create("save", address,
                document(account, "listener-failure", "title")).build());
        var closeFailure = new IOException("provider close failed");
        var opened = new java.util.concurrent.atomic.AtomicInteger();
        var closed = new java.util.concurrent.atomic.AtomicInteger();
        var tracking = new ForwardingStack(f.stack) {
            @Override public java.io.InputStream open(StackRef ref) throws IOException {
                var stream = super.open(ref);
                opened.incrementAndGet();
                return new java.io.FilterInputStream(stream) {
                    @Override public void close() throws IOException {
                        closed.incrementAndGet();
                        super.close();
                        if (failClose && ref.getSha256().equals(SaveIntents.sha256Hex("raw bytes of title".getBytes(StandardCharsets.UTF_8)))) throw closeFailure;
                    }
                };
            }
        };
        var reads = new DocumentReads(f.catalog,
                new DocumentSaves.Drives("default", java.util.Map.of("default", tracking)),
                limits(), bytes -> () -> {}, java.time.Clock.systemUTC());
        var failure = new IllegalStateException("listener setup failed");
        var observer = new io.grpc.stub.ServerCallStreamObserver<ReadBlobResponse>() {
            @Override public boolean isCancelled() { return false; }
            @Override public boolean isReady() { return false; }
            @Override public void setOnCancelHandler(Runnable handler) {
                if (failCancelHandler) throw failure;
            }
            @Override public void setOnReadyHandler(Runnable handler) { throw failure; }
            @Override public void setCompression(String compression) {}
            @Override public void request(int count) {}
            @Override public void setMessageCompression(boolean enabled) {}
            @Override public void disableAutoInboundFlowControl() {}
            @Override public void onNext(ReadBlobResponse value) { throw new AssertionError("unexpected frame"); }
            @Override public void onError(Throwable error) { throw new AssertionError("unexpected callback", error); }
            @Override public void onCompleted() { throw new AssertionError("unexpected completion"); }
        };
        assertThatThrownBy(() -> reads.readBlob(processAuthority(),
                ReadBlobRequest.newBuilder().setAddress(address).setBlobId("b1").build(), observer))
                .isSameAs(failure);
        if (failClose) assertThat(failure.getSuppressed()).containsExactly(closeFailure);
        else assertThat(failure.getSuppressed()).isEmpty();
        assertThat(opened.get()).isGreaterThan(0);
        assertThat(closed.get()).as("each prepared stream closes exactly once").isEqualTo(opened.get());
    }

    @Test
    void readyListenerFailureClosesPartOpenedBySynchronousCallbackAndReleasesLease() throws Exception {
        var address = address(account, "listener-part");
        var document = document(account, "listener-part", "title").toBuilder()
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("title").setBody("x".repeat(90_000)))
                .build();
        f.saves.save(processAuthority(), create("save-part", address, document).build());
        var opens = new java.util.concurrent.atomic.AtomicInteger();
        var closes = new java.util.concurrent.atomic.AtomicInteger();
        var reserved = new java.util.concurrent.atomic.AtomicLong();
        var released = new java.util.concurrent.atomic.AtomicLong();
        var tracking = new ForwardingStack(f.stack) {
            @Override public java.io.InputStream open(StackRef ref) throws IOException {
                var stream = super.open(ref);
                opens.incrementAndGet();
                return new java.io.FilterInputStream(stream) {
                    @Override public void close() throws IOException {
                        closes.incrementAndGet();
                        super.close();
                    }
                };
            }
        };
        var reads = new DocumentReads(f.catalog,
                new DocumentSaves.Drives("default", java.util.Map.of("default", tracking)),
                limits(), bytes -> { reserved.addAndGet(bytes); return () -> released.addAndGet(bytes); },
                java.time.Clock.systemUTC());
        var failure = new IllegalStateException("ready registration failed after callback");
        var observer = new io.grpc.stub.ServerCallStreamObserver<ReadDocumentResponse>() {
            private boolean ready = true;
            @Override public boolean isCancelled() { return false; }
            @Override public boolean isReady() { return ready; }
            @Override public void setOnCancelHandler(Runnable handler) {}
            @Override public void setOnReadyHandler(Runnable handler) {
                handler.run();
                assertThat(reserved.get()).isGreaterThan(0);
                assertThat(reserved.get())
                        .as("the buffered part's lease is still outstanding while frames are pending")
                        .isGreaterThan(released.get());
                assertThat(opens.get())
                        .as("the part is drained and verified before its first frame, so its stream is closed")
                        .isEqualTo(closes.get());
                throw failure;
            }
            @Override public void setCompression(String compression) {}
            @Override public void request(int count) {}
            @Override public void setMessageCompression(boolean enabled) {}
            @Override public void disableAutoInboundFlowControl() {}
            @Override public void onNext(ReadDocumentResponse value) {
                if (value.hasPartData()) ready = false;
            }
            @Override public void onError(Throwable error) { throw new AssertionError("unexpected callback", error); }
            @Override public void onCompleted() { throw new AssertionError("unexpected completion"); }
        };
        assertThatThrownBy(() -> reads.readDocument(processAuthority(),
                ReadDocumentRequest.newBuilder().setAddress(address).build(), observer)).isSameAs(failure);
        assertThat(closes.get()).isEqualTo(opens.get());
        assertThat(released.get()).isEqualTo(reserved.get());
    }

    @Test
    void uploadDocumentRawBlobAndStreamedReadsVerifyContentAndClaimChecks() throws Exception {
        var address = address(account, "doc-stream-raw");
        byte[] rawPayload = new byte[150 * 1024]; // > 2 * 64 KiB chunks
        for (int i = 0; i < rawPayload.length; i++) {
            rawPayload[i] = (byte) ((i * 31 + 7) & 0xFF);
        }
        String rawSha256 = SaveIntents.sha256Hex(rawPayload);

        var inlineSibling = "inline-sibling-bytes".getBytes(StandardCharsets.UTF_8);
        var document = Document.newBuilder()
                .setDocId("doc-stream-raw")
                .setOwnership(ownership(account))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("streamed raw").setBody("body"))
                .setBlobBag(BlobBag.newBuilder().setBlobs(Blobs.newBuilder()
                        .addBlob(Blob.newBuilder()
                                .setBlobId("inline-1")
                                .setMimeType("text/plain")
                                .setFilename("inline.txt")
                                .setSizeBytes(inlineSibling.length)
                                .setData(ByteString.copyFrom(inlineSibling)))
                        .addBlob(Blob.newBuilder()
                                .setBlobId("streamed-1")
                                .setMimeType("application/pdf")
                                .setFilename("report.pdf")
                                .setSizeBytes(rawPayload.length))))
                .putParserResults("tika", ParserResult.newBuilder()
                        .setParserName("tika")
                        .setParserVersion("1")
                        .setStatus(ParseStatus.PARSE_STATUS_OK)
                        .build())
                .build();

        var header = UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(create("op-raw-1", address, document))
                .setBlobId("streamed-1")
                .setDeclaredLength(rawPayload.length)
                .setExpectedSha256(rawSha256)
                .build();

        var uploadResponse = sendUpload(header, splitChunks(rawPayload, 23_456));
        assertThat(uploadResponse.getSaved().getOperationId()).isEqualTo("op-raw-1");
        assertThat(uploadResponse.getSaved().getAccession().getRevision()).isEqualTo(1L);
        assertThat(uploadResponse.getSaved().getReplayed()).isFalse();

        var stored = f.catalog.manifest(address, 1).orElseThrow();
        var getDoc = blockingStub.getDocument(GetDocumentRequest.newBuilder().setAddress(address).build());
        var returnedBlobs = getDoc.getDocument().getBlobBag().getBlobs().getBlobList();
        assertThat(returnedBlobs).extracting(Blob::getBlobId).containsExactly("inline-1", "streamed-1");
        assertThat(returnedBlobs).allSatisfy(b -> assertThat(b.hasStorageRef()).isTrue());
        assertThat(f.blobBytes(stored, "inline-1")).isEqualTo(inlineSibling);
        assertThat(f.blobBytes(stored, "streamed-1")).isEqualTo(rawPayload);

        // ReadDocument: manifest first, then PartData chunks with offset and last=true only on final chunk, then ReadComplete.
        var frames = new ArrayList<ReadDocumentResponse>();
        blockingStub.readDocument(ReadDocumentRequest.newBuilder().setAddress(address).build())
                .forEachRemaining(frames::add);
        assertThat(frames.getFirst().hasManifest()).isTrue();
        assertThat(frames.getLast().hasComplete()).isTrue();

        var reassembled = reassembleParts(frames);
        assertThat(reassembled).containsKeys(
                DocumentPart.DOCUMENT_PART_CORE,
                DocumentPart.DOCUMENT_PART_BLOBS,
                DocumentPart.DOCUMENT_PART_PARSED);
        for (var manifestPart : frames.getFirst().getManifest().getPartsList()) {
            var partBytes = reassembled.get(manifestPart.getSelector().getPart());
            assertThat(partBytes.size()).isEqualTo(manifestPart.getSizeBytes());
            assertThat(SaveIntents.sha256Hex(partBytes.toByteArray())).isEqualTo(manifestPart.getSha256());
        }
        var blobsEntry = stored.getPartsList().stream()
                .filter(p -> p.getEntry().getSelector().getPart() == DocumentPart.DOCUMENT_PART_BLOBS)
                .findFirst()
                .orElseThrow();
        var merged = Document.newBuilder()
                .mergeFrom(reassembled.get(DocumentPart.DOCUMENT_PART_CORE))
                .mergeFrom(RawBlobs.withClaimChecks(
                        reassembled.get(DocumentPart.DOCUMENT_PART_BLOBS),
                        blobsEntry.getBlobRefsList()))
                .mergeFrom(reassembled.get(DocumentPart.DOCUMENT_PART_PARSED));
        assertThat(merged.build()).isEqualTo(getDoc.getDocument());

        // ReadBlob full read and range reads.
        var fullBlobFrames = new ArrayList<ReadBlobResponse>();
        blockingStub.readBlob(ReadBlobRequest.newBuilder()
                .setAddress(address)
                .setBlobId("streamed-1")
                .build())
                .forEachRemaining(fullBlobFrames::add);
        assertThat(fullBlobFrames.getFirst().hasHeader()).isTrue();
        var blobHeader = fullBlobFrames.getFirst().getHeader();
        assertThat(blobHeader.getRevision()).isEqualTo(1L);
        assertThat(blobHeader.getTotalLength()).isEqualTo(rawPayload.length);
        assertThat(blobHeader.getSha256()).isEqualTo(rawSha256);
        assertThat(blobHeader.getMimeType()).isEqualTo("application/pdf");
        assertThat(blobHeader.getFilename()).isEqualTo("report.pdf");
        assertThat(fullBlobFrames.size()).isGreaterThan(3); // header + at least 3 chunks of <= 64 KiB
        assertThat(concatBlobData(fullBlobFrames)).isEqualTo(rawPayload);

        // Sub-range read: offset=50_000, length=70_000.
        var subRangeFrames = new ArrayList<ReadBlobResponse>();
        blockingStub.readBlob(ReadBlobRequest.newBuilder()
                .setAddress(address)
                .setBlobId("streamed-1")
                .setOffset(50_000)
                .setLength(70_000)
                .build())
                .forEachRemaining(subRangeFrames::add);
        assertThat(subRangeFrames.getFirst().getHeader().getTotalLength()).isEqualTo(rawPayload.length);
        assertThat(subRangeFrames.getFirst().getHeader().getSha256()).isEqualTo(rawSha256);
        assertThat(concatBlobData(subRangeFrames))
                .isEqualTo(Arrays.copyOfRange(rawPayload, 50_000, 120_000));

        // Open-ended range read: offset=100_000, length=0 (to the end).
        var tailRangeFrames = new ArrayList<ReadBlobResponse>();
        blockingStub.readBlob(ReadBlobRequest.newBuilder()
                .setAddress(address)
                .setBlobId("streamed-1")
                .setOffset(100_000)
                .setLength(0)
                .build())
                .forEachRemaining(tailRangeFrames::add);
        assertThat(concatBlobData(tailRangeFrames))
                .isEqualTo(Arrays.copyOfRange(rawPayload, 100_000, rawPayload.length));
    }

    @Test
    void uploadDocumentEncodedPartsForCoreParsedAndBlobs() throws Exception {
        var address = address(account, "doc-encoded-parts");
        String largeBody = "x".repeat(90_000); // > 64 KiB so ReadDocument splits CORE into multiple PartData frames
        var coreFragmentDoc = Document.newBuilder()
                .setDocId("doc-encoded-parts")
                .setOwnership(ownership(account))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("encoded core").setBody(largeBody))
                .build();
        byte[] coreFragmentBytes = RawBlobs.deterministic(coreFragmentDoc).toByteArray();

        // 1. Stream CORE as an encoded part on creation.
        var createSave = SaveDocumentRequest.newBuilder()
                .setOperationId("op-enc-core")
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(ExpectedRevision.newBuilder().setNone(true))
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE))
                .setOwner(intakeOwner())
                .build();
        var coreHeader = UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(createSave)
                .setEncodedPart(StreamedPart.newBuilder()
                        .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE)))
                .setDeclaredLength(coreFragmentBytes.length)
                .setExpectedSha256(SaveIntents.sha256Hex(coreFragmentBytes))
                .build();
        var r1 = sendUpload(coreHeader, splitChunks(coreFragmentBytes, 19_000));
        assertThat(r1.getSaved().getAccession().getRevision()).isEqualTo(1L);

        // Verify ReadDocument splits the > 64 KiB CORE part across 2 PartData frames with accurate offsets.
        var readR1 = new ArrayList<ReadDocumentResponse>();
        blockingStub.readDocument(ReadDocumentRequest.newBuilder().setAddress(address).build())
                .forEachRemaining(readR1::add);
        var coreFrames = readR1.stream()
                .filter(ReadDocumentResponse::hasPartData)
                .map(ReadDocumentResponse::getPartData)
                .toList();
        assertThat(coreFrames).hasSize(2);
        assertThat(coreFrames.get(0).getOffset()).isZero();
        assertThat(coreFrames.get(0).getData().size()).isEqualTo(DocumentStreams.CHUNK_SIZE);
        assertThat(coreFrames.get(0).getLast()).isFalse();
        assertThat(coreFrames.get(1).getOffset()).isEqualTo(DocumentStreams.CHUNK_SIZE);
        assertThat(coreFrames.get(1).getLast()).isTrue();

        // 2. Stream PARSED as an encoded part on revision 2 while carrying CORE forward.
        var parsedFragmentDoc = Document.newBuilder()
                .setDocId("doc-encoded-parts")
                .putParserResults("tika", ParserResult.newBuilder()
                        .setParserName("tika")
                        .setParserVersion("2.0")
                        .setStatus(ParseStatus.PARSE_STATUS_OK)
                        .build())
                .build();
        byte[] parsedFragmentBytes = RawBlobs.deterministic(parsedFragmentDoc).toByteArray();
        var parsedSave = SaveDocumentRequest.newBuilder()
                .setOperationId("op-enc-parsed")
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(at(1))
                .addWrites(put(DocumentPart.DOCUMENT_PART_PARSED))
                .setOwner(intakeOwner())
                .build();
        var parsedHeader = UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(parsedSave)
                .setEncodedPart(StreamedPart.newBuilder()
                        .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED)))
                .setDeclaredLength(parsedFragmentBytes.length)
                .setExpectedSha256(SaveIntents.sha256Hex(parsedFragmentBytes))
                .build();
        var r2 = sendUpload(parsedHeader, splitChunks(parsedFragmentBytes, 64));
        assertThat(r2.getSaved().getAccession().getRevision()).isEqualTo(2L);

        // 3. Stream BLOBS as an encoded part on revision 3; inline raw bytes in the streamed fragment are claim-checked.
        var blobsFragmentDoc = Document.newBuilder()
                .setDocId("doc-encoded-parts")
                .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder()
                        .setBlobId("b-enc")
                        .setMimeType("text/plain")
                        .setFilename("enc.txt")
                        .setSizeBytes(11)
                        .setData(ByteString.copyFromUtf8("hello world"))))
                .build();
        byte[] blobsFragmentBytes = RawBlobs.deterministic(blobsFragmentDoc).toByteArray();
        var blobsSave = SaveDocumentRequest.newBuilder()
                .setOperationId("op-enc-blobs")
                .setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(at(2))
                .addWrites(put(DocumentPart.DOCUMENT_PART_BLOBS))
                .setOwner(intakeOwner())
                .build();
        var blobsHeader = UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(blobsSave)
                .setEncodedPart(StreamedPart.newBuilder()
                        .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_BLOBS)))
                .setDeclaredLength(blobsFragmentBytes.length)
                .setExpectedSha256(SaveIntents.sha256Hex(blobsFragmentBytes))
                .build();
        var r3 = sendUpload(blobsHeader, List.of(ByteString.copyFrom(blobsFragmentBytes)));
        assertThat(r3.getSaved().getAccession().getRevision()).isEqualTo(3L);
        var storedR3 = f.catalog.manifest(address, 3).orElseThrow();
        assertThat(f.blobBytes(storedR3, "b-enc")).isEqualTo("hello world".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void uploadProtocolViolationsAreRefusedUnrecordedWithoutWritingObjects() throws Exception {
        var address = address(account, "doc-proto-violations");
        var validDoc = documentWithPendingBlob(account, "doc-proto-violations", "b1");
        var validHeader = UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(create("op-proto", address, validDoc))
                .setBlobId("b1")
                .build();

        f.stack.reset();
        // 1. Data frame before UploadHeader.
        assertMalformedUnrecorded(() -> sendRawFrames(List.of(
                UploadDocumentRequest.newBuilder().setData(ByteString.copyFromUtf8("early")).build())));

        // 2. Empty stream completed without UploadHeader.
        assertMalformedUnrecorded(() -> sendRawFrames(List.of()));

        // 3. Duplicate UploadHeader mid-stream.
        assertMalformedUnrecorded(() -> sendRawFrames(List.of(
                UploadDocumentRequest.newBuilder().setHeader(validHeader).build(),
                UploadDocumentRequest.newBuilder().setHeader(validHeader).build())));

        // 4. Unknown blob_id not in document.blob_bag.
        var unknownBlobHeader = validHeader.toBuilder().setBlobId("missing-blob").build();
        assertMalformedUnrecorded(() -> sendUpload(unknownBlobHeader, List.of(ByteString.copyFromUtf8("abc"))));

        // 5. Uploaded blob_id already carries inline data.
        var inlineBlobDoc = document(account, "doc-proto-violations", "title");
        var inlineBlobHeader = UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(create("op-proto", address, inlineBlobDoc))
                .setBlobId("b1")
                .build();
        assertMalformedUnrecorded(() -> sendUpload(inlineBlobHeader, List.of(ByteString.copyFromUtf8("abc"))));

        // 6. Client-supplied storage_ref on uploaded blob.
        var forgedDoc = validDoc.toBuilder()
                .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder()
                        .setBlobId("b1")
                        .setStorageRef(FileStorageReference.newBuilder()
                                .setDriveName(f.stack.stackId())
                                .setObjectKey(f.prefix + "/forged"))))
                .build();
        var forgedHeader = UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(create("op-proto", address, forgedDoc))
                .setBlobId("b1")
                .build();
        assertMalformedUnrecorded(() -> sendUpload(forgedHeader, List.of(ByteString.copyFromUtf8("abc"))));

        // 7. Streamed encoded_part also carries inline fields for that part in save.document.
        var duplicateEncodedCore = UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(create("op-proto", address, inlineBlobDoc))
                .setEncodedPart(StreamedPart.newBuilder()
                        .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE)))
                .build();
        assertMalformedUnrecorded(() -> sendUpload(duplicateEncodedCore, List.of(ByteString.copyFromUtf8("abc"))));

        assertThat(f.catalog.outcome(account, "op-proto")).isEmpty();
        assertThat(f.catalog.current(address)).isEmpty();
        assertThat(listS3Objects()).isEmpty();
    }

    @Test
    void unauthorizedCallerIsRejectedOnUploadHeaderBeforeDataFramesTouchTheStack() {
        var address = address(account, "doc-unauth-upload");
        var doc = documentWithPendingBlob(account, "doc-unauth-upload", "b1");
        var header = UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(create("op-unauth", address, doc))
                .setBlobId("b1")
                .setDeclaredLength(5)
                .build();

        // Credential-bound caller registered in the catalog, while no grant in the account covers
        // the intake owner "ds-1" -> OWNER_UNKNOWN (FAILED_PRECONDITION).
        var credential = UUID.randomUUID();
        f.catalog.registerCredential(ISSUER, credential, "ungranted-writer");
        activeCaller.set(writer(account, "ungranted-writer", credential, 1L));
        try {
            f.stack.reset();
            var rejection = detail(
                    () -> sendUpload(header, List.of(ByteString.copyFromUtf8("hello"))),
                    Status.Code.FAILED_PRECONDITION,
                    SaveRejection.class);
            assertThat(rejection.getReason()).isEqualTo(RejectionReason.REJECTION_REASON_OWNER_UNKNOWN);
            assertThat(f.stack.puts.get()).isZero();
            assertThat(f.catalog.outcome(account, "op-unauth")).isEmpty();

            // Install grant for a different credential -> caller is rejected with WRITE_DENIED (PERMISSION_DENIED).
            var otherCredential = UUID.randomUUID();
            f.catalog.registerCredential(ISSUER, otherCredential, "other-writer");
            f.intakeGrant(account, otherCredential, "ds-1", true);
            var denied = detail(
                    () -> sendUpload(header, List.of(ByteString.copyFromUtf8("hello"))),
                    Status.Code.PERMISSION_DENIED,
                    SaveRejection.class);
            assertThat(denied.getReason()).isEqualTo(RejectionReason.REJECTION_REASON_WRITE_DENIED);
            assertThat(f.stack.puts.get()).isZero();
            assertThat(f.catalog.outcome(account, "op-unauth")).isEmpty();
        } finally {
            activeCaller.set(processAuthority());
        }
    }

    @Test
    void uploadLengthAndDigestMismatchesRecordUploadMismatchAndLeaveNoOrphanObjects() throws Exception {
        var address = address(account, "doc-mismatch");
        var doc = documentWithPendingBlob(account, "doc-mismatch", "b1");
        byte[] actual = "actual-payload-bytes".getBytes(StandardCharsets.UTF_8);

        // 1. Declared length longer than actual stream (truncated stream).
        var shortHeader = UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(create("op-short", address, doc))
                .setBlobId("b1")
                .setDeclaredLength(actual.length + 10)
                .build();
        var shortRejection = detail(
                () -> sendUpload(shortHeader, List.of(ByteString.copyFrom(actual))),
                Status.Code.INVALID_ARGUMENT,
                SaveRejection.class);
        assertThat(shortRejection.getReason()).isEqualTo(RejectionReason.REJECTION_REASON_UPLOAD_MISMATCH);
        assertThat(f.catalog.outcome(account, "op-short").orElseThrow().outcome().getRejection().getReason())
                .isEqualTo(RejectionReason.REJECTION_REASON_UPLOAD_MISMATCH);
        assertThat(listS3Objects()).isEmpty();

        // 2. Declared length shorter than actual stream (overflow stream).
        var longHeader = UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(create("op-long", address, doc))
                .setBlobId("b1")
                .setDeclaredLength(5)
                .build();
        var longRejection = detail(
                () -> sendUpload(longHeader, List.of(ByteString.copyFrom(actual))),
                Status.Code.INVALID_ARGUMENT,
                SaveRejection.class);
        assertThat(longRejection.getReason()).isEqualTo(RejectionReason.REJECTION_REASON_UPLOAD_MISMATCH);
        assertThat(f.catalog.outcome(account, "op-long").orElseThrow().outcome().getRejection().getReason())
                .isEqualTo(RejectionReason.REJECTION_REASON_UPLOAD_MISMATCH);
        assertThat(listS3Objects()).isEmpty();

        // 3. Expected SHA-256 mismatch: object is deleted from S3 and UPLOAD_MISMATCH is recorded.
        String wrongSha = "0".repeat(64);
        var shaHeader = UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(create("op-bad-sha", address, doc))
                .setBlobId("b1")
                .setDeclaredLength(actual.length)
                .setExpectedSha256(wrongSha)
                .build();
        var shaRejection = detail(
                () -> sendUpload(shaHeader, List.of(ByteString.copyFrom(actual))),
                Status.Code.INVALID_ARGUMENT,
                SaveRejection.class);
        assertThat(shaRejection.getReason()).isEqualTo(RejectionReason.REJECTION_REASON_UPLOAD_MISMATCH);
        assertThat(f.catalog.outcome(account, "op-bad-sha").orElseThrow().outcome().getRejection().getReason())
                .isEqualTo(RejectionReason.REJECTION_REASON_UPLOAD_MISMATCH);
        assertThat(listS3Objects()).isEmpty();

        // Reusing op-bad-sha with different bytes fails with ALREADY_EXISTS (OperationReuse),
        // while uploading with the right SHA under a new operation_id succeeds.
        byte[] differentBytes = "different-bytes-here".getBytes(StandardCharsets.UTF_8);
        assertThat(detail(
                () -> sendUpload(shaHeader, List.of(ByteString.copyFrom(differentBytes))),
                Status.Code.ALREADY_EXISTS,
                OperationReuse.class).getOperationId()).isEqualTo("op-bad-sha");

        var fixedHeader = UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(create("op-fixed", address, doc))
                .setBlobId("b1")
                .setDeclaredLength(actual.length)
                .setExpectedSha256(SaveIntents.sha256Hex(actual))
                .build();
        var committed = sendUpload(fixedHeader, List.of(ByteString.copyFrom(actual)));
        assertThat(committed.getSaved().getAccession().getRevision()).isEqualTo(1L);
    }

    @Test
    void crashBeforeCommitAndCrashAfterCommitPreserveUploadIdempotency() throws Exception {
        var address = address(account, "doc-upload-retry");
        var doc = documentWithPendingBlob(account, "doc-upload-retry", "b1");
        byte[] payload = "idempotent-stream-payload".getBytes(StandardCharsets.UTF_8);
        var header = UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(create("op-idempotent", address, doc))
                .setBlobId("b1")
                .setDeclaredLength(payload.length)
                .setExpectedSha256(SaveIntents.sha256Hex(payload))
                .build();

        // Simulate crash after stack upload but before catalog commit.
        f.stack.afterPut = () -> {
            throw new IOException("simulated crash before catalog commit");
        };
        assertThatThrownBy(() -> sendUpload(header, List.of(ByteString.copyFrom(payload))))
                .isInstanceOfSatisfying(StatusRuntimeException.class, failure ->
                        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE));
        assertThat(blockingStub.getOperationOutcome(GetOperationOutcomeRequest.newBuilder()
                .setAccountId(account)
                .setOperationId("op-idempotent")
                .build()).hasNone()).isTrue();

        // Retry with the same operation_id after recovery -> commits revision 1 with replayed = false.
        f.stack.afterPut = () -> {};
        var committed = sendUpload(header, List.of(ByteString.copyFrom(payload)));
        assertThat(committed.getSaved().getAccession().getRevision()).isEqualTo(1L);
        assertThat(committed.getSaved().getReplayed()).isFalse();

        // Retry with the same operation_id after commit -> the header, which declares the
        // length and digest, is answered replayed with the recorded accession.
        var replayed = sendUpload(header, List.of(ByteString.copyFrom(payload)));
        assertThat(replayed.getAnswerCase()).isEqualTo(UploadDocumentResponse.AnswerCase.REPLAYED);
        assertThat(replayed.getReplayed().getAccession()).isEqualTo(committed.getSaved().getAccession());
        assertThat(replayed.getReplayed().getReplayed()).isTrue();
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1L);
    }

    @Test
    void clientCancellationMidUploadAbortsStackWriteAndRecordsNoOutcome() throws Exception {
        var address = address(account, "doc-cancel-upload");
        var doc = documentWithPendingBlob(account, "doc-cancel-upload", "b1");
        var header = UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(create("op-cancelled", address, doc))
                .setBlobId("b1")
                .setDeclaredLength(10L * 1024 * 1024)
                .build();

        var errorLatch = new CountDownLatch(1);
        var requestRef = new AtomicReference<ClientCallStreamObserver<UploadDocumentRequest>>();
        var responseObserver = new ClientResponseObserver<UploadDocumentRequest, UploadDocumentResponse>() {
            @Override
            public void beforeStart(ClientCallStreamObserver<UploadDocumentRequest> requestStream) {
                requestRef.set(requestStream);
            }
            @Override public void onNext(UploadDocumentResponse value) {}
            @Override public void onError(Throwable t) { errorLatch.countDown(); }
            @Override public void onCompleted() {}
        };

        var req = asyncStub.uploadDocument(responseObserver);
        req.onNext(UploadDocumentRequest.newBuilder().setHeader(header).build());
        req.onNext(UploadDocumentRequest.newBuilder().setData(ByteString.copyFrom(new byte[64 * 1024])).build());
        requestRef.get().cancel("client cancelled mid-upload", null);
        assertThat(errorLatch.await(5, TimeUnit.SECONDS)).isTrue();

        // Give the server virtual thread a brief moment to unwind its cancelled stream.
        Thread.sleep(100);
        assertThat(blockingStub.getOperationOutcome(GetOperationOutcomeRequest.newBuilder()
                .setAccountId(account)
                .setOperationId("op-cancelled")
                .build()).hasNone()).isTrue();
        assertThat(f.catalog.current(address)).isEmpty();
        assertThat(listS3Objects()).isEmpty();
    }

    @Test
    void readDocumentAndReadBlobReportDataLossOnCorruptedStorageAndValidateRanges() throws Exception {
        var address = address(account, "doc-corrupt-checks");
        byte[] raw = "verifiable-blob-bytes-for-corruption-check".getBytes(StandardCharsets.UTF_8);
        var doc = documentWithPendingBlob(account, "doc-corrupt-checks", "b1");
        var header = UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(create("op-corrupt-1", address, doc))
                .setBlobId("b1")
                .setDeclaredLength(raw.length)
                .setExpectedSha256(SaveIntents.sha256Hex(raw))
                .build();
        sendUpload(header, List.of(ByteString.copyFrom(raw)));
        var stored = f.catalog.manifest(address, 1).orElseThrow();

        // Out-of-bounds range on ReadBlob fails with INVALID_ARGUMENT before BlobHeader is sent.
        var outOfBoundsFrames = new ArrayList<ReadBlobResponse>();
        assertThatThrownBy(() -> blockingStub.readBlob(ReadBlobRequest.newBuilder()
                .setAddress(address)
                .setBlobId("b1")
                .setOffset(raw.length + 1)
                .setLength(1)
                .build()).forEachRemaining(outOfBoundsFrames::add))
                .isInstanceOfSatisfying(StatusRuntimeException.class, failure ->
                        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
        assertThat(outOfBoundsFrames).isEmpty();

        // Corrupt the raw blob object in S3 -> full ReadBlob fails with DATA_LOSS at EOF.
        var blobKey = stored.getPartsList().stream()
                .flatMap(p -> p.getBlobRefsList().stream())
                .filter(b -> b.getBlobId().equals("b1"))
                .findFirst()
                .orElseThrow()
                .getRef()
                .getKey();
        byte[] corruptedBlob = raw.clone();
        corruptedBlob[0] ^= 0x55;
        s3().putObject(
                PutObjectRequest.builder().bucket(BUCKET).key(blobKey).build(),
                RequestBody.fromBytes(corruptedBlob));

        assertThatThrownBy(() -> blockingStub.readBlob(ReadBlobRequest.newBuilder()
                .setAddress(address)
                .setBlobId("b1")
                .build()).forEachRemaining(frame -> {}))
                .isInstanceOfSatisfying(StatusRuntimeException.class, failure ->
                        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.DATA_LOSS));

        // Corrupt the PARSED part object in S3 -> ReadDocument fails with DATA_LOSS and never emits ReadComplete.
        var parsedKey = stored.getPartsList().stream()
                .filter(p -> p.getEntry().getSelector().getPart() == DocumentPart.DOCUMENT_PART_PARSED)
                .findFirst()
                .orElseThrow()
                .getRef()
                .getKey();
        s3().putObject(
                PutObjectRequest.builder().bucket(BUCKET).key(parsedKey).build(),
                RequestBody.fromBytes("corrupted-parsed-part".getBytes(StandardCharsets.UTF_8)));

        var docFrames = new ArrayList<ReadDocumentResponse>();
        assertThatThrownBy(() -> blockingStub.readDocument(ReadDocumentRequest.newBuilder()
                .setAddress(address)
                .build()).forEachRemaining(docFrames::add))
                .isInstanceOfSatisfying(StatusRuntimeException.class, failure ->
                        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.DATA_LOSS));
        assertThat(docFrames).noneMatch(ReadDocumentResponse::hasComplete);
        assertThat(docFrames)
                .as("no frame of the corrupt PARSED part may reach the client")
                .noneMatch(frame -> frame.hasPartData()
                        && frame.getPartData().getSelector().getPart() == DocumentPart.DOCUMENT_PART_PARSED);
    }

    @Test
    void readDocumentEmitsNoFrameOfACorruptPartLargerThanOneChunk() throws Exception {
        // A CORE part larger than one 64 KiB frame: a read that verified only at end of
        // stream would already have emitted the part's first frame when the digest fails.
        var address = address(account, "doc-corrupt-large");
        var document = Document.newBuilder()
                .setDocId("doc-corrupt-large")
                .setOwnership(ownership(account))
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("large corrupt core")
                        .setBody("z".repeat(150_000)))
                .build();
        blockingStub.saveDocument(create("op-corrupt-large", address, document)
                .clearWrites()
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE))
                .setDocument(document)
                .build());
        var stored = f.catalog.manifest(address, 1).orElseThrow();
        var coreEntry = stored.getPartsList().stream()
                .filter(p -> p.getEntry().getSelector().getPart() == DocumentPart.DOCUMENT_PART_CORE)
                .findFirst()
                .orElseThrow();
        assertThat(coreEntry.getRef().getLength()).isGreaterThan(DocumentStreams.CHUNK_SIZE);

        // Flip one byte in the middle of the stored object, keeping its length, so only
        // the digest check at end of stream can catch the corruption.
        var coreKey = coreEntry.getRef().getKey();
        byte[] storedBytes = s3().getObject(
                software.amazon.awssdk.services.s3.model.GetObjectRequest.builder()
                        .bucket(BUCKET).key(coreKey).build())
                .readAllBytes();
        storedBytes[storedBytes.length / 2] ^= 0x55;
        s3().putObject(
                PutObjectRequest.builder().bucket(BUCKET).key(coreKey).build(),
                RequestBody.fromBytes(storedBytes));

        var frames = new ArrayList<ReadDocumentResponse>();
        assertThatThrownBy(() -> blockingStub.readDocument(ReadDocumentRequest.newBuilder()
                .setAddress(address)
                .build()).forEachRemaining(frames::add))
                .isInstanceOfSatisfying(StatusRuntimeException.class, failure ->
                        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.DATA_LOSS));
        assertThat(frames).noneMatch(ReadDocumentResponse::hasPartData);
        assertThat(frames).noneMatch(ReadDocumentResponse::hasComplete);
    }

    @Test
    void httpUploadHandlerResolvesEachRequestsCallerAndAnswersAnOutageAsUnavailable() throws Exception {
        var outage = new AtomicBoolean();
        var seen = new ArrayList<String>();
        UploadHttpHandler.Callers callers = headers -> {
            if (outage.get()) throw new IllegalStateException("identity provider unreachable");
            String credential = headers.getFirst("x-test-credential");
            if (credential == null) return Optional.empty();
            seen.add(credential);
            return Optional.of(new RepositoryCaller("resolved-" + credential, true));
        };
        var httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        httpServer.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        httpServer.createContext(UploadHttpHandler.UPLOAD_PATH, new UploadHttpHandler(f.uploads, callers));
        httpServer.start();
        var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        String uri = "http://127.0.0.1:" + httpServer.getAddress().getPort() + UploadHttpHandler.UPLOAD_PATH
                + "?account_id=" + account + "&datasource_id=ds-1&drive=default&filename=resolved.bin"
                + "&operation_id=resolved-op&doc_id=resolved-doc";
        byte[] payload = "resolved per request".getBytes(StandardCharsets.UTF_8);
        try {
            var none = client.send(HttpRequest.newBuilder(URI.create(uri)).header("Accept", "application/proto")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(payload)).build(), HttpResponse.BodyHandlers.ofByteArray());
            assertThat(none.statusCode()).isEqualTo(401);
            assertThat(ItemError.parseFrom(none.body()).getCode()).isEqualTo(16);

            outage.set(true);
            var down = client.send(HttpRequest.newBuilder(URI.create(uri)).header("Accept", "application/proto")
                    .header("x-test-credential", "alice").POST(HttpRequest.BodyPublishers.ofByteArray(payload)).build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            assertThat(down.statusCode()).isEqualTo(503);
            var unavailable = ItemError.parseFrom(down.body());
            assertThat(unavailable.getCode()).isEqualTo(14);
            assertThat(unavailable.getMessage()).contains("identity provider unreachable");
            assertThat(f.catalog.outcome(account, "resolved-op")).isEmpty();

            outage.set(false);
            var saved = client.send(HttpRequest.newBuilder(URI.create(uri)).header("Accept", "application/proto")
                    .header("x-test-credential", "alice").POST(HttpRequest.BodyPublishers.ofByteArray(payload)).build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            assertThat(saved.statusCode()).isEqualTo(200);
            assertThat(seen).containsExactly("alice");
        } finally {
            httpServer.stop(0);
        }
    }

    @Test
    void httpUploadHandlerAnswersInProtobufWhenTheClientAcceptsIt() throws Exception {
        String token = "secret-upload-token-" + UUID.randomUUID();
        var httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        httpServer.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        httpServer.createContext(UploadHttpHandler.UPLOAD_PATH, new UploadHttpHandler(f.uploads, token));
        httpServer.start();
        var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        String baseUrl = "http://127.0.0.1:" + httpServer.getAddress().getPort();
        try {
            byte[] payload = "protobuf-answered-upload-bytes".getBytes(StandardCharsets.UTF_8);
            String sha256 = SaveIntents.sha256Hex(payload);

            var unauth = client.send(
                    HttpRequest.newBuilder(URI.create(baseUrl + UploadHttpHandler.UPLOAD_PATH))
                            .header("Accept", "application/proto")
                            .header("api_token", "wrong-token")
                            .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                            .build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            assertThat(unauth.statusCode()).isEqualTo(401);
            assertThat(unauth.headers().firstValue("grpc-status")).hasValue("16");
            assertThat(unauth.headers().firstValue("grpc-message")).hasValue("Unauthorized");
            assertThat(unauth.headers().firstValue("Content-Type")).hasValue("application/proto");
            var unauthError = ItemError.parseFrom(unauth.body());
            assertThat(unauthError.getCode()).isEqualTo(16);
            assertThat(unauthError.getMessage()).isEqualTo("Unauthorized");
            assertThat(unauthError.getDetailCase()).isEqualTo(ItemError.DetailCase.DETAIL_NOT_SET);

            var badDigest = client.send(
                    HttpRequest.newBuilder(URI.create(baseUrl + UploadHttpHandler.UPLOAD_PATH
                                    + "?account_id=" + account + "&datasource_id=ds-1&drive=default"
                                    + "&filename=bad.bin&operation_id=proto-op-bad&doc_id=proto-doc-bad"))
                            .header("Accept", "text/html, application/proto;q=0.9")
                            .header("api_token", token)
                            .header("X-Content-Sha256", "f".repeat(64))
                            .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                            .build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            assertThat(badDigest.statusCode()).isEqualTo(400);
            assertThat(badDigest.headers().firstValue("grpc-status")).hasValue("3");
            assertThat(badDigest.headers().firstValue("grpc-message")).isPresent().get().asString().isNotBlank();
            var mismatch = ItemError.parseFrom(badDigest.body());
            assertThat(mismatch.getCode()).isEqualTo(3);
            assertThat(mismatch.getRejection().getReason()).isEqualTo(RejectionReason.REJECTION_REASON_UPLOAD_MISMATCH);

            // A stale expected revision answers ABORTED with the RevisionConflict detail.
            var stale = client.send(
                    HttpRequest.newBuilder(URI.create(baseUrl + UploadHttpHandler.UPLOAD_PATH
                                    + "?account_id=" + account + "&datasource_id=ds-1&drive=default"
                                    + "&filename=stale.bin&operation_id=proto-op-stale&doc_id=proto-doc-stale&expected_revision=5"))
                            .header("Accept", "application/proto")
                            .header("api_token", token)
                            .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                            .build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            assertThat(stale.statusCode()).isEqualTo(409);
            assertThat(stale.headers().firstValue("grpc-status")).hasValue("10");
            var conflict = ItemError.parseFrom(stale.body()).getConflict();
            assertThat(conflict.getExpected().getRevision()).isEqualTo(5);
            assertThat(conflict.getCurrentRevision()).isEqualTo(0);
            assertThat(conflict.getAddress().getDocId()).isEqualTo("proto-doc-stale");

            var uploadUri = URI.create(baseUrl + UploadHttpHandler.UPLOAD_PATH
                    + "?account_id=" + account + "&datasource_id=ds-1&drive=default&filename=proto.pdf"
                    + "&operation_id=proto-op-ok&doc_id=proto-doc-1&blob_id=proto-blob-1&content_type=application/pdf");
            for (boolean replay : new boolean[] {false, true}) {
                var ok = client.send(
                        HttpRequest.newBuilder(uploadUri)
                                .header("Accept", "application/proto")
                                .header("Authorization", "Bearer " + token)
                                .header("X-Content-Sha256", sha256)
                                .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                                .build(),
                        HttpResponse.BodyHandlers.ofByteArray());
                assertThat(ok.statusCode()).isEqualTo(200);
                assertThat(ok.headers().firstValue("Content-Type")).hasValue("application/proto");
                var saved = UploadDocumentResponse.parseFrom(ok.body()).getSaved();
                assertThat(saved.getOperationId()).isEqualTo("proto-op-ok");
                assertThat(saved.getReplayed()).isEqualTo(replay);
                assertThat(saved.getAccession().getRevision()).isEqualTo(1);
                assertThat(saved.getAccession().getAddress().getDocId()).isEqualTo("proto-doc-1");
            }
        } finally {
            httpServer.stop(0);
        }
    }

    @Test
    void httpUploadHandlerStreamsRawUploadsEnforcesAuthAndReplaysIdempotentRetries() throws Exception {
        String token = "secret-upload-token-" + UUID.randomUUID();
        var handler = new UploadHttpHandler(f.uploads, token);
        var httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        httpServer.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        httpServer.createContext(UploadHttpHandler.UPLOAD_PATH, handler);
        httpServer.createContext("/other", handler);
        httpServer.start();
        var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        int port = httpServer.getAddress().getPort();
        String baseUrl = "http://127.0.0.1:" + port;

        try {
            byte[] payload = "http-streamed-document-content-bytes".getBytes(StandardCharsets.UTF_8);
            String sha256 = SaveIntents.sha256Hex(payload);

            // 1. Missing/wrong token -> 401 Unauthorized.
            var unauth = client.send(
                    HttpRequest.newBuilder(URI.create(baseUrl + UploadHttpHandler.UPLOAD_PATH))
                            .header("api_token", "wrong-token")
                            .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(unauth.statusCode()).isEqualTo(401);

            // 2. Wrong path -> 404 Not Found; wrong method -> 405 Method Not Allowed.
            var notFound = client.send(
                    HttpRequest.newBuilder(URI.create(baseUrl + "/other"))
                            .header("Authorization", "Bearer " + token)
                            .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(notFound.statusCode()).isEqualTo(404);

            var wrongMethod = client.send(
                    HttpRequest.newBuilder(URI.create(baseUrl + UploadHttpHandler.UPLOAD_PATH))
                            .header("Authorization", "Bearer " + token)
                            .GET()
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(wrongMethod.statusCode()).isEqualTo(405);

            // 3. Chunked transfer (unknown Content-Length) -> 411 Length Required.
            var chunked = client.send(
                    HttpRequest.newBuilder(URI.create(baseUrl + UploadHttpHandler.UPLOAD_PATH
                                    + "?account_id=" + account + "&datasource_id=ds-1&drive=default&filename=a.bin"))
                            .header("api_token", token)
                            .POST(HttpRequest.BodyPublishers.ofInputStream(
                                    () -> new java.io.ByteArrayInputStream(payload)))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(chunked.statusCode()).isEqualTo(411);

            // 4. Digest mismatch -> 400 Bad Request and recorded UPLOAD_MISMATCH.
            var badDigest = client.send(
                    HttpRequest.newBuilder(URI.create(baseUrl + UploadHttpHandler.UPLOAD_PATH
                                    + "?account_id=" + account
                                    + "&datasource_id=ds-1"
                                    + "&drive=default"
                                    + "&filename=bad.bin"
                                    + "&operation_id=http-op-bad"
                                    + "&doc_id=http-doc-bad"))
                            .header("api_token", token)
                            .header("X-Content-Sha256", "f".repeat(64))
                            .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(badDigest.statusCode()).isEqualTo(400);
            assertThat(f.catalog.outcome(account, "http-op-bad").orElseThrow().outcome().getRejection().getReason())
                    .isEqualTo(RejectionReason.REJECTION_REASON_UPLOAD_MISMATCH);

            // 5. Valid streaming upload -> 200 OK, readable via ReadBlob, and idempotent on retry.
            var uploadUri = URI.create(baseUrl + UploadHttpHandler.UPLOAD_PATH
                    + "?account_id=" + account
                    + "&datasource_id=ds-1"
                    + "&drive=default"
                    + "&filename=contract.pdf"
                    + "&operation_id=http-op-ok"
                    + "&doc_id=http-doc-1"
                    + "&blob_id=http-blob-1"
                    + "&content_type=application/pdf");
            var ok = client.send(
                    HttpRequest.newBuilder(uploadUri)
                            .header("Authorization", "Bearer " + token)
                            .header("X-Content-Sha256", sha256)
                            .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(ok.statusCode()).isEqualTo(200);
            assertThat(ok.body())
                    .contains("\"operation_id\":\"http-op-ok\"")
                    .contains("\"doc_id\":\"http-doc-1\"")
                    .contains("\"revision\":1")
                    .contains("\"replayed\":false")
                    .contains("\"size_bytes\":" + payload.length)
                    .contains("\"sha256\":\"" + sha256 + "\"");

            var replay = client.send(
                    HttpRequest.newBuilder(uploadUri)
                            .header("Authorization", "Bearer " + token)
                            .header("X-Content-Sha256", sha256)
                            .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(replay.statusCode()).isEqualTo(200);
            assertThat(replay.body())
                    .contains("\"revision\":1")
                    .contains("\"replayed\":true");

            var readFrames = new ArrayList<ReadBlobResponse>();
            blockingStub.readBlob(ReadBlobRequest.newBuilder()
                    .setAddress(address(account, "http-doc-1"))
                    .setBlobId("http-blob-1")
                    .build()).forEachRemaining(readFrames::add);
            assertThat(concatBlobData(readFrames)).isEqualTo(payload);

            // 6. Early refusal (401/400) does not wait to drain a 600 MB request body stream.
            try (var socket = new java.net.Socket("127.0.0.1", port)) {
                socket.setSoTimeout(5_000);
                var out = socket.getOutputStream();
                String rawReq = "POST " + UploadHttpHandler.UPLOAD_PATH + " HTTP/1.1\r\n"
                        + "Host: 127.0.0.1\r\n"
                        + "api_token: wrong-token\r\n"
                        + "Content-Length: " + (600L * 1024 * 1024) + "\r\n\r\n";
                out.write(rawReq.getBytes(StandardCharsets.UTF_8));
                out.flush();
                var responseLine = new java.io.BufferedReader(
                        new java.io.InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
                        .readLine();
                assertThat(responseLine).contains("401");
            }
        } finally {
            httpServer.stop(0);
        }
    }

    @Test
    void replayMustRejectDeclaredLengthAndExpectedDigestMismatchesWithoutOverwritingRecordedOutcome() throws Exception {
        var address = address(account, "doc-replay-mismatch");
        var doc = document(account, "doc-replay-mismatch", "title");
        byte[] payload = doc.getBlobBag().getBlob().getData().toByteArray();
        var inline = doc.toBuilder()
                .setBlobBag(doc.getBlobBag().toBuilder().setBlob(doc.getBlobBag().getBlob().toBuilder().clearData()))
                .build();
        var header = UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(create("op-replay-strict", address, inline))
                .setBlobId("b1")
                .setDeclaredLength(payload.length)
                .setExpectedSha256(SaveIntents.sha256Hex(payload))
                .build();

        var committed = f.uploads.upload(processAuthority(), header, new java.io.ByteArrayInputStream(payload));
        assertThat(committed.getSaved().getAccession().getRevision()).isEqualTo(1L);
        assertThat(committed.getSaved().getReplayed()).isFalse();

        // 1. Retry with declared_length mismatch (+1 and -1) must be rejected with UPLOAD_MISMATCH.
        var badLengthPlus = header.toBuilder().setDeclaredLength(payload.length + 1).build();
        assertThatThrownBy(() -> f.uploads.upload(
                processAuthority(), badLengthPlus, new java.io.ByteArrayInputStream(payload)))
                .isInstanceOfSatisfying(AccessionException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat((SaveRejection) failure.detail().orElseThrow())
                            .extracting(SaveRejection::getReason)
                            .isEqualTo(RejectionReason.REJECTION_REASON_UPLOAD_MISMATCH);
                });

        // Over UploadDocument the header alone fixes the intent of a retry that declares the
        // length and digest, so another declared length is another intent under the same
        // operation ID, refused in the first answer before any byte is sent.
        var badLengthMinus = header.toBuilder().setDeclaredLength(Math.max(0, payload.length - 1)).build();
        assertThat(detail(
                () -> sendUpload(badLengthMinus, List.of(ByteString.copyFrom(payload))),
                Status.Code.ALREADY_EXISTS,
                OperationReuse.class).getOperationId())
                .isEqualTo("op-replay-strict");

        // 2. Retry with expected_sha256 mismatch must be rejected with UPLOAD_MISMATCH.
        var badDigest = header.toBuilder().setExpectedSha256("0".repeat(64)).build();
        assertThatThrownBy(() -> f.uploads.upload(
                processAuthority(), badDigest, new java.io.ByteArrayInputStream(payload)))
                .isInstanceOfSatisfying(AccessionException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat((SaveRejection) failure.detail().orElseThrow())
                            .extracting(SaveRejection::getReason)
                            .isEqualTo(RejectionReason.REJECTION_REASON_UPLOAD_MISMATCH);
                });
        assertThat(detail(
                () -> sendUpload(badDigest, List.of(ByteString.copyFrom(payload))),
                Status.Code.ALREADY_EXISTS,
                OperationReuse.class).getOperationId())
                .isEqualTo("op-replay-strict");

        // 3. The original committed Accession outcome in the catalog is untouched and still replays on a valid retry.
        assertThat(f.catalog.outcome(account, "op-replay-strict").orElseThrow().outcome().hasAccession()).isTrue();
        var validReplay = f.uploads.upload(processAuthority(), header, new java.io.ByteArrayInputStream(payload));
        assertThat(validReplay.getSaved().getReplayed()).isTrue();
        assertThat(validReplay.getSaved().getAccession()).isEqualTo(committed.getSaved().getAccession());
    }

    @Test
    void readDocumentCancellationClosesActiveStackStreamAndEncodedPartReservesAdmissionBytes() throws Exception {
        // 1. Encoded-part upload reserves admission bytes before/while buffering the fragment.
        var reservedBytes = new java.util.concurrent.atomic.AtomicLong();
        var releasedBytes = new java.util.concurrent.atomic.AtomicLong();
        var trackingSaves = new DocumentSaves(
                f.catalog,
                f.drives,
                resolver(),
                limits(),
                bytes -> {
                    reservedBytes.addAndGet(bytes);
                    return () -> releasedBytes.addAndGet(bytes);
                },
                java.time.Clock.systemUTC());
        var trackingUploads = new DocumentUploads(trackingSaves);

        var address = address(account, "doc-reserved-enc");
        var coreFragmentDoc = Document.newBuilder()
                .setDocId("doc-reserved-enc")
                .setOwnership(ownership(account))
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("reserved")
                        .setBody("y".repeat(90_000)))
                .build();
        byte[] coreFragmentBytes = RawBlobs.deterministic(coreFragmentDoc).toByteArray();
        var encHeader = UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(SaveDocumentRequest.newBuilder()
                        .setOperationId("op-reserved-enc")
                        .setTarget(DocumentTarget.newBuilder().setAddress(address))
                        .setExpected(ExpectedRevision.newBuilder().setNone(true))
                        .addWrites(put(DocumentPart.DOCUMENT_PART_CORE))
                        .setOwner(intakeOwner()))
                .setEncodedPart(StreamedPart.newBuilder()
                        .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE)))
                .setDeclaredLength(coreFragmentBytes.length)
                .setExpectedSha256(SaveIntents.sha256Hex(coreFragmentBytes))
                .build();

        trackingUploads.upload(processAuthority(), encHeader, new java.io.ByteArrayInputStream(coreFragmentBytes));
        assertThat(reservedBytes.get()).isGreaterThanOrEqualTo(coreFragmentBytes.length);
        assertThat(releasedBytes.get()).isEqualTo(reservedBytes.get());

        // Exhausted host reservation during encoded-part buffering fails with RESOURCE_EXHAUSTED unrecorded.
        var exhaustedSaves = new DocumentSaves(
                f.catalog,
                f.drives,
                resolver(),
                limits(),
                bytes -> {
                    throw new ai.protomolt.proto.repo.spi.RepositoryException(
                            ai.protomolt.proto.repo.spi.RepositoryException.Code.RESOURCE_EXHAUSTED,
                            "host reservation exhausted");
                },
                java.time.Clock.systemUTC());
        var exhaustedUploads = new DocumentUploads(exhaustedSaves);
        var exhaustedHeader = encHeader.toBuilder()
                .setSave(encHeader.getSave().toBuilder()
                        .setOperationId("op-exhausted-enc")
                        .setTarget(DocumentTarget.newBuilder().setAddress(address(account, "doc-exhausted-enc"))))
                .build();
        assertThatThrownBy(() -> exhaustedUploads.upload(
                processAuthority(), exhaustedHeader, new java.io.ByteArrayInputStream(coreFragmentBytes)))
                .isInstanceOfSatisfying(AccessionException.class, failure ->
                        assertThat(failure.code()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
        assertThat(f.catalog.outcome(account, "op-exhausted-enc")).isEmpty();

        // 2. ReadDocument retains the open Stack InputStream and closes it immediately on cancellation.
        var streamClosed = new CountDownLatch(1);
        ai.protomolt.proto.repo.stack.Stack trackingCloseStack = new ForwardingStack(f.stack) {
            @Override
            public java.io.InputStream open(StackRef ref) throws IOException {
                var delegate = super.open(ref);
                return new java.io.FilterInputStream(delegate) {
                    @Override
                    public void close() throws IOException {
                        streamClosed.countDown();
                        super.close();
                    }
                };
            }
        };
        var readDrives = new DocumentSaves.Drives("default", java.util.Map.of("default", trackingCloseStack));
        var readReserved = new java.util.concurrent.atomic.AtomicLong();
        var readReleased = new java.util.concurrent.atomic.AtomicLong();
        var customReads = new DocumentReads(
                f.catalog,
                readDrives,
                limits(),
                bytes -> {
                    readReserved.addAndGet(bytes);
                    return () -> readReleased.addAndGet(bytes);
                },
                java.time.Clock.systemUTC());

        var cancelHandler = new AtomicReference<Runnable>();
        var ready = new java.util.concurrent.atomic.AtomicBoolean(true);
        var frames = new ArrayList<ReadDocumentResponse>();
        var serverObserver = new io.grpc.stub.ServerCallStreamObserver<ReadDocumentResponse>() {
            @Override
            public boolean isCancelled() {
                return !ready.get();
            }

            @Override
            public void setOnCancelHandler(Runnable onCancelHandler) {
                cancelHandler.set(onCancelHandler);
            }

            @Override
            public void setCompression(String compression) {}

            @Override
            public boolean isReady() {
                return ready.get();
            }

            @Override
            public void setOnReadyHandler(Runnable onReadyHandler) {}

            @Override
            public void request(int count) {}

            @Override
            public void setMessageCompression(boolean enable) {}

            @Override
            public void disableAutoInboundFlowControl() {}

            @Override
            public void onNext(ReadDocumentResponse value) {
                frames.add(value);
                if (value.hasPartData() && !value.getPartData().getLast()) {
                    ready.set(false);
                }
            }

            @Override
            public void onError(Throwable t) {
                throw new AssertionError("unexpected onError during paused read", t);
            }

            @Override
            public void onCompleted() {
                throw new AssertionError("unexpected onCompleted before cancellation");
            }
        };

        customReads.readDocument(
                processAuthority(),
                ReadDocumentRequest.newBuilder().setAddress(address).build(),
                serverObserver);

        assertThat(frames).hasSize(2);
        assertThat(frames.get(0).hasManifest()).isTrue();
        assertThat(frames.get(1).hasPartData()).isTrue();
        assertThat(frames.get(1).getPartData().getLast()).isFalse();
        assertThat(streamClosed.await(1, TimeUnit.SECONDS))
                .as("the part is fully drained and verified, so its Stack InputStream is closed before its first frame")
                .isTrue();
        assertThat(readReserved.get()).isGreaterThanOrEqualTo(coreFragmentBytes.length);
        assertThat(readReserved.get() - readReleased.get())
                .as("the part's reservation bounds the buffered bytes until its last frame goes out")
                .isEqualTo(coreFragmentBytes.length);

        cancelHandler.get().run();

        assertThat(readReleased.get())
                .as("cancel() must release the buffered part's reservation")
                .isEqualTo(readReserved.get());

        // 2b. Full multi-chunk ReadDocument holds the part reservation inside every PartData callback
        // (including the final frame where last == true) and releases it once the final callback returns.
        readReserved.set(0L);
        readReleased.set(0L);
        var partFrames = new java.util.concurrent.atomic.AtomicInteger();
        var sawLastPartFrame = new java.util.concurrent.atomic.AtomicBoolean();
        var sawCompleteFrame = new java.util.concurrent.atomic.AtomicBoolean();
        customReads.readDocument(
                processAuthority(),
                ReadDocumentRequest.newBuilder().setAddress(address).build(),
                new StreamObserver<>() {
                    @Override
                    public void onNext(ReadDocumentResponse value) {
                        if (value.hasPartData()) {
                            partFrames.incrementAndGet();
                            assertThat(readReserved.get() - readReleased.get())
                                    .as("part reservation must remain live inside PartData callback; last=%s",
                                            value.getPartData().getLast())
                                    .isEqualTo(coreFragmentBytes.length);
                            if (value.getPartData().getLast()) {
                                sawLastPartFrame.set(true);
                            }
                        } else if (value.hasComplete()) {
                            sawCompleteFrame.set(true);
                            assertThat(readReserved.get() - readReleased.get())
                                    .as("part reservation must be released after final PartData callback returns")
                                    .isZero();
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        throw new AssertionError("unexpected onError during full read", t);
                    }

                    @Override
                    public void onCompleted() {}
                });
        assertThat(partFrames.get()).isGreaterThanOrEqualTo(2);
        assertThat(sawLastPartFrame.get()).isTrue();
        assertThat(sawCompleteFrame.get()).isTrue();
        assertThat(readReleased.get()).isEqualTo(readReserved.get());

        // 3. Concurrent cancellation while pump() is actively executing does not release the reservation
        // until the in-flight reader unwinds.
        var inReader = new CountDownLatch(1);
        var allowReaderUnwind = new CountDownLatch(1);
        var activeBytes = new java.util.concurrent.atomic.AtomicLong();
        ai.protomolt.proto.repo.stack.Stack blockingReadStack = new ForwardingStack(f.stack) {
            @Override
            public java.io.InputStream open(StackRef ref) throws IOException {
                var delegate = super.open(ref);
                return new java.io.FilterInputStream(delegate) {
                    private boolean signalled;

                    @Override
                    public int read(byte[] b, int off, int len) throws IOException {
                        if (!signalled) {
                            signalled = true;
                            inReader.countDown();
                            try {
                                allowReaderUnwind.await(5, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new IOException("interrupted", e);
                            }
                        }
                        return super.read(b, off, len);
                    }
                };
            }
        };
        var concurrentReads = new DocumentReads(
                f.catalog,
                new DocumentSaves.Drives("default", java.util.Map.of("default", blockingReadStack)),
                limits(),
                bytes -> {
                    activeBytes.addAndGet(bytes);
                    return () -> activeBytes.addAndGet(-bytes);
                },
                java.time.Clock.systemUTC());
        var concurrentCancelRef = new AtomicReference<Runnable>();
        var concurrentCancelled = new java.util.concurrent.atomic.AtomicBoolean(false);
        var concurrentObserver = new io.grpc.stub.ServerCallStreamObserver<ReadDocumentResponse>() {
            @Override public boolean isCancelled() { return concurrentCancelled.get(); }
            @Override public void setOnCancelHandler(Runnable onCancelHandler) { concurrentCancelRef.set(onCancelHandler); }
            @Override public void setCompression(String compression) {}
            @Override public boolean isReady() { return true; }
            @Override public void setOnReadyHandler(Runnable onReadyHandler) {}
            @Override public void request(int count) {}
            @Override public void setMessageCompression(boolean enable) {}
            @Override public void disableAutoInboundFlowControl() {}
            @Override public void onNext(ReadDocumentResponse value) {}
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        };
        var pumpFuture = CompletableFuture.runAsync(() -> concurrentReads.readDocument(
                processAuthority(),
                ReadDocumentRequest.newBuilder().setAddress(address).build(),
                concurrentObserver));
        assertThat(inReader.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(activeBytes.get()).isEqualTo(coreFragmentBytes.length);

        // Cancel on this thread while pump() is still inside readNextPartChunk on the worker thread.
        concurrentCancelled.set(true);
        concurrentCancelRef.get().run();
        assertThat(activeBytes.get())
                .as("reservation must remain held while in-flight pump() has not yet unwound")
                .isEqualTo(coreFragmentBytes.length);

        allowReaderUnwind.countDown();
        pumpFuture.get(5, TimeUnit.SECONDS);
        assertThat(activeBytes.get())
                .as("reservation must be released once in-flight pump() finishes unwinding")
                .isZero();
    }

    @Test
    void closeAndDeleteFailuresPreserveDiagnostics() throws Exception {
        var address = address(account, "doc-diagnostics");
        byte[] payload = "diagnostic-payload".getBytes(StandardCharsets.UTF_8);
        var doc = documentWithPendingBlob(account, "doc-diagnostics", "b1");
        var header = UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(create("op-diag-1", address, doc))
                .setBlobId("b1")
                .setDeclaredLength(payload.length)
                .setExpectedSha256(SaveIntents.sha256Hex(payload))
                .build();
        sendUpload(header, List.of(ByteString.copyFrom(payload)));

        // 1. Normal completion close() failure on ReadDocument surfaces as UNAVAILABLE error instead of onCompleted.
        var failOnClose = new java.util.concurrent.atomic.AtomicBoolean(true);
        ai.protomolt.proto.repo.stack.Stack failingCloseStack = new ForwardingStack(f.stack) {
            @Override
            public java.io.InputStream open(StackRef ref) throws IOException {
                var delegate = super.open(ref);
                return new java.io.FilterInputStream(delegate) {
                    @Override
                    public void close() throws IOException {
                        super.close();
                        if (failOnClose.get()) {
                            throw new IOException("simulated close failure at EOF");
                        }
                    }
                };
            }
        };
        var failingReads = new DocumentReads(
                f.catalog,
                new DocumentSaves.Drives("default", java.util.Map.of("default", failingCloseStack)),
                limits(),
                bytes -> () -> {},
                java.time.Clock.systemUTC());
        var errorRef = new AtomicReference<Throwable>();
        failingReads.readDocument(
                processAuthority(),
                ReadDocumentRequest.newBuilder().setAddress(address).build(),
                new StreamObserver<>() {
                    @Override public void onNext(ReadDocumentResponse value) {}
                    @Override public void onError(Throwable t) { errorRef.set(t); }
                    @Override public void onCompleted() {}
                });
        assertThat(errorRef.get())
                .isInstanceOfSatisfying(StatusRuntimeException.class, failure ->
                        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE));

        // 2. Cancellation close() failure is logged at WARNING level without throwing on the cancel callback thread.
        var julLogger = java.util.logging.Logger.getLogger(DocumentStreams.class.getName());
        var loggedRecords = new java.util.concurrent.CopyOnWriteArrayList<java.util.logging.LogRecord>();
        var handler = new java.util.logging.Handler() {
            @Override
            public void publish(java.util.logging.LogRecord record) {
                loggedRecords.add(record);
            }
            @Override public void flush() {}
            @Override public void close() {}
        };
        julLogger.addHandler(handler);
        try {
            failOnClose.set(false);
            var cancelHandlerRef = new AtomicReference<Runnable>();
            var ready = new java.util.concurrent.atomic.AtomicBoolean(true);
            failingReads.readBlob(
                    processAuthority(),
                    ReadBlobRequest.newBuilder().setAddress(address).setBlobId("b1").build(),
                    new io.grpc.stub.ServerCallStreamObserver<ReadBlobResponse>() {
                        @Override public boolean isCancelled() { return !ready.get(); }
                        @Override public void setOnCancelHandler(Runnable onCancelHandler) { cancelHandlerRef.set(onCancelHandler); }
                        @Override public void setCompression(String compression) {}
                        @Override public boolean isReady() { return ready.get(); }
                        @Override public void setOnReadyHandler(Runnable onReadyHandler) {}
                        @Override public void request(int count) {}
                        @Override public void setMessageCompression(boolean enable) {}
                        @Override public void disableAutoInboundFlowControl() {}
                        @Override
                        public void onNext(ReadBlobResponse value) {
                            if (value.hasHeader()) {
                                failOnClose.set(true);
                                ready.set(false);
                            }
                        }
                        @Override public void onError(Throwable t) {}
                        @Override public void onCompleted() {}
                    });
            cancelHandlerRef.get().run();
            assertThat(loggedRecords)
                    .anySatisfy(record -> {
                        assertThat(record.getLevel()).isEqualTo(java.util.logging.Level.WARNING);
                        assertThat(record.getMessage()).contains("Failed to close active stack stream during client cancellation");
                        assertThat(record.getThrown())
                                .isInstanceOf(IOException.class)
                                .hasMessageContaining("simulated close failure at EOF");
                    });
        } finally {
            julLogger.removeHandler(handler);
        }

        // 3. Delete failure during upload mismatch cleanup is attached as suppressed to the rejection exception.
        ai.protomolt.proto.repo.stack.Stack failingDeleteStack = new ForwardingStack(f.stack) {
            @Override
            public void delete(StackRef ref) throws IOException {
                super.delete(ref);
                throw new IOException("simulated delete failure for " + ref.getKey());
            }
        };
        var failingSaves = new DocumentSaves(
                f.catalog,
                new DocumentSaves.Drives("default", java.util.Map.of("default", failingDeleteStack)),
                resolver(),
                limits(),
                bytes -> () -> {},
                java.time.Clock.systemUTC());
        var failingUploads = new DocumentUploads(failingSaves);
        var badDigestHeader = UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(create("op-diag-mismatch", address(account, "doc-diag-mismatch"),
                        documentWithPendingBlob(account, "doc-diag-mismatch", "b1")))
                .setBlobId("b1")
                .setDeclaredLength(payload.length)
                .setExpectedSha256("f".repeat(64))
                .build();
        assertThatThrownBy(() -> failingUploads.upload(
                processAuthority(), badDigestHeader, new java.io.ByteArrayInputStream(payload)))
                .isInstanceOfSatisfying(AccessionException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat(failure.getSuppressed())
                            .anySatisfy(s -> assertThat(s.getMessage()).contains("simulated delete failure"));
                });
    }

    @Test
    void aStreamedHeaderIsAcceptedBeforeAnyByteIsStagedAndHalfCloseAnswersSaved() throws Exception {
        var address = address(account, "doc-accepted");
        byte[] payload = "accepted-before-bytes".getBytes(StandardCharsets.UTF_8);
        f.stack.reset();
        var upload = new Handshake();
        upload.send(UploadDocumentRequest.newBuilder().setHeader(UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(create("op-accepted", address, documentWithPendingBlob(account, "doc-accepted", "b1")))
                .setBlobId("b1")
                .setDeclaredLength(payload.length)).build());
        assertThat(upload.answer().getAnswerCase()).isEqualTo(UploadDocumentResponse.AnswerCase.ACCEPTED);
        assertThat(f.catalog.outcome(account, "op-accepted")).isEmpty();
        assertThat(f.stack.puts.get()).isZero();

        upload.send(UploadDocumentRequest.newBuilder().setData(ByteString.copyFrom(payload)).build());
        upload.complete();
        var saved = upload.answer();
        assertThat(saved.getAnswerCase()).isEqualTo(UploadDocumentResponse.AnswerCase.SAVED);
        assertThat(saved.getSaved().getAccession().getRevision()).isEqualTo(1L);
        assertThat(upload.completed()).isTrue();
        assertThat(f.stack.puts.get()).isPositive();
    }

    @Test
    void anInlineBodyCommitsInOneRoundTripWithoutHalfClose() throws Exception {
        var address = address(account, "doc-inline");
        byte[] payload = "inline-body".getBytes(StandardCharsets.UTF_8);
        var upload = new Handshake();
        upload.send(UploadDocumentRequest.newBuilder().setHeader(UploadHeader.newBuilder()
                .setInlineData(ByteString.copyFrom(payload))
                .setSave(create("op-inline", address, documentWithPendingBlob(account, "doc-inline", "b1")))
                .setBlobId("b1")
                .setDeclaredLength(payload.length)
                .setExpectedSha256(SaveIntents.sha256Hex(payload))).build());
        var saved = upload.answer();
        assertThat(saved.getAnswerCase()).isEqualTo(UploadDocumentResponse.AnswerCase.SAVED);
        assertThat(saved.getSaved().getAccession().getRevision()).isEqualTo(1L);
        assertThat(upload.completed()).isTrue();
        var blob = ByteString.newOutput();
        var frames = blockingStub.readBlob(ReadBlobRequest.newBuilder().setAddress(address).setBlobId("b1").build());
        while (frames.hasNext()) frames.next().getData().writeTo(blob);
        assertThat(blob.toByteString().toByteArray()).isEqualTo(payload);
    }

    @Test
    void aRefusedHeaderIsTheFirstAnswerAndNoByteIsStaged() throws Exception {
        var address = address(account, "doc-refused-header");
        var credential = UUID.randomUUID();
        f.catalog.registerCredential(ISSUER, credential, "ungranted-writer");
        activeCaller.set(writer(account, "ungranted-writer", credential, 1L));
        try {
            f.stack.reset();
            var upload = new Handshake();
            upload.send(UploadDocumentRequest.newBuilder().setHeader(UploadHeader.newBuilder()
                    .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                    .setSave(create("op-refused-header", address,
                            documentWithPendingBlob(account, "doc-refused-header", "b1")))
                    .setBlobId("b1")
                    .setDeclaredLength(10L * 1024 * 1024)).build());
            var failure = upload.failure();
            assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
            assertThat(upload.answers).isEmpty();
            assertThat(f.stack.puts.get()).isZero();
            assertThat(f.catalog.outcome(account, "op-refused-header")).isEmpty();
        } finally {
            activeCaller.set(processAuthority());
        }
    }

    @Test
    void aTrailerReplacesTheProvisionalCoreAndTheCommitKeepsTheFinishedOne() throws Exception {
        var address = address(account, "doc-trailer");
        byte[] payload = "classified-by-trailer".getBytes(StandardCharsets.UTF_8);
        var provisional = documentWithPendingBlob(account, "doc-trailer", "b1");
        var finished = provisional.toBuilder()
                .setSearchMetadata(provisional.getSearchMetadata().toBuilder().setTitle("finished title"))
                .build();
        var upload = trailerUpload("op-trailer", address, provisional, payload);
        upload.send(UploadDocumentRequest.newBuilder()
                .setTrailer(ai.protomolt.proto.repo.v2.UploadTrailer.newBuilder().setDocument(finished)).build());
        upload.complete();
        var saved = upload.answer();
        assertThat(saved.getSaved().getAccession().getRevision()).isEqualTo(1L);
        var stored = blockingStub.getDocument(GetDocumentRequest.newBuilder().setAddress(address).build());
        assertThat(stored.getDocument().getSearchMetadata().getTitle()).isEqualTo("finished title");
        assertThat(stored.getManifest().getRevision()).isEqualTo(1L);
    }

    @Test
    void aTrailerThatChangesAnotherPartOrTheOwnershipIsRefusedAndRecordsNothing() throws Exception {
        byte[] payload = "trailer-refusals".getBytes(StandardCharsets.UTF_8);
        var blobAddress = address(account, "doc-trailer-blobs");
        var provisional = documentWithPendingBlob(account, "doc-trailer-blobs", "b1");
        var otherBlobs = provisional.toBuilder().setBlobBag(BlobBag.newBuilder().setBlob(
                provisional.getBlobBag().getBlob().toBuilder().setFilename("renamed.bin"))).build();
        f.stack.reset();
        var deletes = new java.util.concurrent.atomic.AtomicInteger();
        f.stack.afterDelete = () -> deletes.incrementAndGet();
        var upload = trailerUpload("op-trailer-blobs", blobAddress, provisional, payload);
        upload.send(UploadDocumentRequest.newBuilder()
                .setTrailer(ai.protomolt.proto.repo.v2.UploadTrailer.newBuilder().setDocument(otherBlobs)).build());
        upload.complete();
        var blobFailure = upload.failure();
        assertThat(blobFailure.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(blobFailure.getStatus().getDescription()).contains("only CORE may differ");
        assertThat(f.catalog.outcome(account, "op-trailer-blobs")).isEmpty();
        // The streamed object was put, and deleted again when the trailer was refused.
        assertThat(f.stack.puts.get()).isEqualTo(1);
        assertThat(deletes.get()).isEqualTo(1);

        var ownerAddress = address(account, "doc-trailer-owner");
        var owned = documentWithPendingBlob(account, "doc-trailer-owner", "b1");
        var otherOwner = owned.toBuilder().setOwnership(owned.getOwnership().toBuilder().setDatasourceId("ds-2")).build();
        var ownerUpload = trailerUpload("op-trailer-owner", ownerAddress, owned, payload);
        ownerUpload.send(UploadDocumentRequest.newBuilder()
                .setTrailer(ai.protomolt.proto.repo.v2.UploadTrailer.newBuilder().setDocument(otherOwner)).build());
        ownerUpload.complete();
        var ownerFailure = ownerUpload.failure();
        assertThat(ownerFailure.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(ownerFailure.getStatus().getDescription()).contains("ownership");
        assertThat(f.catalog.outcome(account, "op-trailer-owner")).isEmpty();
    }

    @Test
    void trailerFramingIsEnforced() throws Exception {
        byte[] payload = "trailer-framing".getBytes(StandardCharsets.UTF_8);
        var doc = documentWithPendingBlob(account, "doc-framing", "b1");

        // A trailer the header did not announce.
        var unannounced = new Handshake();
        unannounced.send(UploadDocumentRequest.newBuilder().setHeader(UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(create("op-unannounced", address(account, "doc-framing"), doc))
                .setBlobId("b1")
                .setDeclaredLength(payload.length)).build());
        assertThat(unannounced.answer().getAnswerCase()).isEqualTo(UploadDocumentResponse.AnswerCase.ACCEPTED);
        unannounced.send(UploadDocumentRequest.newBuilder().setData(ByteString.copyFrom(payload)).build());
        unannounced.send(UploadDocumentRequest.newBuilder()
                .setTrailer(ai.protomolt.proto.repo.v2.UploadTrailer.newBuilder().setDocument(doc)).build());
        unannounced.complete();
        assertThat(unannounced.failure().getStatus().getDescription()).contains("announced no UploadTrailer");

        // An announced trailer that never arrives.
        var missing = trailerUpload("op-missing-trailer", address(account, "doc-framing"), doc, payload);
        missing.complete();
        assertThat(missing.failure().getStatus().getDescription()).contains("without the UploadTrailer");

        // Data after the trailer.
        var late = trailerUpload("op-late-data", address(account, "doc-framing"), doc, payload);
        late.send(UploadDocumentRequest.newBuilder()
                .setTrailer(ai.protomolt.proto.repo.v2.UploadTrailer.newBuilder().setDocument(doc)).build());
        late.send(UploadDocumentRequest.newBuilder().setData(ByteString.copyFrom(payload)).build());
        late.complete();
        assertThat(late.failure().getStatus().getDescription()).contains("may follow the trailer");

        // A trailer needs a CORE PUT that is not the streamed part.
        var noCore = new Handshake();
        noCore.send(UploadDocumentRequest.newBuilder().setHeader(UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.newBuilder().setCoreInTrailer(true))
                .setSave(create("op-no-core", address(account, "doc-framing"), doc).clearWrites()
                        .addWrites(put(DocumentPart.DOCUMENT_PART_BLOBS)))
                .setBlobId("b1")
                .setDeclaredLength(payload.length)).build());
        assertThat(noCore.failure().getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(noCore.answers).isEmpty();
        for (var operation : List.of("op-unannounced", "op-missing-trailer", "op-late-data", "op-no-core"))
            assertThat(f.catalog.outcome(account, operation)).as(operation).isEmpty();
    }

    /** A streamed blob upload that announced a trailer, accepted, with its bytes sent. */
    private Handshake trailerUpload(String operation, NodeAddress address, Document provisional, byte[] payload)
            throws Exception {
        var upload = new Handshake();
        upload.send(UploadDocumentRequest.newBuilder().setHeader(UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.newBuilder().setCoreInTrailer(true))
                .setSave(create(operation, address, provisional))
                .setBlobId("b1")
                .setDeclaredLength(payload.length)).build());
        assertThat(upload.answer().getAnswerCase()).isEqualTo(UploadDocumentResponse.AnswerCase.ACCEPTED);
        upload.send(UploadDocumentRequest.newBuilder().setData(ByteString.copyFrom(payload)).build());
        return upload;
    }

    /** One UploadDocument call driven a frame at a time, its answers read in order. */
    private final class Handshake {
        final java.util.concurrent.LinkedBlockingQueue<UploadDocumentResponse> answers =
                new java.util.concurrent.LinkedBlockingQueue<>();
        private final CompletableFuture<Void> end = new CompletableFuture<>();
        private final StreamObserver<UploadDocumentRequest> requests = asyncStub.uploadDocument(new StreamObserver<>() {
            @Override public void onNext(UploadDocumentResponse value) { answers.add(value); }
            @Override public void onError(Throwable t) { end.completeExceptionally(t); }
            @Override public void onCompleted() { end.complete(null); }
        });

        void send(UploadDocumentRequest frame) { requests.onNext(frame); }

        void complete() { requests.onCompleted(); }

        UploadDocumentResponse answer() throws Exception {
            var answer = answers.poll(10, TimeUnit.SECONDS);
            if (answer == null) {
                end.getNow(null);
                fail("No UploadDocument answer arrived");
            }
            return answer;
        }

        boolean completed() throws Exception {
            end.get(10, TimeUnit.SECONDS);
            return true;
        }

        StatusRuntimeException failure() {
            var thrown = catchThrowable(() -> end.get(10, TimeUnit.SECONDS));
            assertThat(thrown).isInstanceOf(java.util.concurrent.ExecutionException.class);
            assertThat(thrown.getCause()).isInstanceOf(StatusRuntimeException.class);
            return (StatusRuntimeException) thrown.getCause();
        }
    }

    // ------------------------------------------------------------------ helpers

    private static Document documentWithPendingBlob(String account, String docId, String blobId) {
        return Document.newBuilder()
                .setDocId(docId)
                .setOwnership(ownership(account))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("title").setBody("body"))
                .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder()
                        .setBlobId(blobId)
                        .setMimeType("application/octet-stream")
                        .setFilename(blobId + ".bin")))
                .putParserResults("tika", ParserResult.newBuilder()
                        .setParserName("tika")
                        .setParserVersion("1")
                        .setStatus(ParseStatus.PARSE_STATUS_OK)
                        .build())
                .build();
    }

    private static List<ByteString> splitChunks(byte[] bytes, int chunkSize) {
        var chunks = new ArrayList<ByteString>();
        for (int i = 0; i < bytes.length; i += chunkSize) {
            int end = Math.min(bytes.length, i + chunkSize);
            chunks.add(ByteString.copyFrom(bytes, i, end - i));
        }
        return chunks;
    }

    private UploadDocumentResponse sendUpload(UploadHeader header, List<ByteString> chunks) {
        var frames = new ArrayList<UploadDocumentRequest>(chunks.size() + 1);
        frames.add(UploadDocumentRequest.newBuilder().setHeader(header).build());
        for (var chunk : chunks) {
            frames.add(UploadDocumentRequest.newBuilder().setData(chunk).build());
        }
        return sendRawFrames(frames);
    }

    private UploadDocumentResponse sendRawFrames(List<UploadDocumentRequest> frames) {
        var future = new CompletableFuture<UploadDocumentResponse>();
        var req = asyncStub.uploadDocument(new StreamObserver<>() {
            private UploadDocumentResponse value;
            @Override public void onNext(UploadDocumentResponse response) { this.value = response; }
            @Override public void onError(Throwable t) { future.completeExceptionally(t); }
            @Override public void onCompleted() { future.complete(value); }
        });
        for (var frame : frames) {
            if (future.isDone()) break;
            req.onNext(frame);
        }
        if (!future.isDone()) {
            req.onCompleted();
        }
        try {
            return future.get(15, TimeUnit.SECONDS);
        } catch (java.util.concurrent.ExecutionException failed) {
            if (failed.getCause() instanceof RuntimeException runtime) throw runtime;
            throw new RuntimeException(failed.getCause());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void assertMalformedUnrecorded(Runnable action) {
        var rejection = detail(action, Status.Code.INVALID_ARGUMENT, SaveRejection.class);
        assertThat(rejection.getReason()).isEqualTo(RejectionReason.REJECTION_REASON_MALFORMED_REQUEST);
    }

    private static EnumMap<DocumentPart, ByteString> reassembleParts(List<ReadDocumentResponse> frames)
            throws IOException {
        var buffers = new EnumMap<DocumentPart, ByteArrayOutputStream>(DocumentPart.class);
        var expectedOffset = new EnumMap<DocumentPart, Long>(DocumentPart.class);
        var seenLast = new EnumMap<DocumentPart, Boolean>(DocumentPart.class);
        for (int i = 1; i < frames.size() - 1; i++) {
            var frame = frames.get(i);
            assertThat(frame.hasPartData()).isTrue();
            var pd = frame.getPartData();
            var part = pd.getSelector().getPart();
            assertThat(seenLast.getOrDefault(part, false))
                    .as("no PartData frame may follow last=true for %s", part)
                    .isFalse();
            long offset = expectedOffset.getOrDefault(part, 0L);
            assertThat(pd.getOffset()).isEqualTo(offset);
            var out = buffers.computeIfAbsent(part, ignored -> new ByteArrayOutputStream());
            pd.getData().writeTo(out);
            expectedOffset.put(part, offset + pd.getData().size());
            if (pd.getLast()) {
                seenLast.put(part, true);
            }
        }
        var result = new EnumMap<DocumentPart, ByteString>(DocumentPart.class);
        for (var entry : buffers.entrySet()) {
            assertThat(seenLast.get(entry.getKey())).isTrue();
            result.put(entry.getKey(), ByteString.copyFrom(entry.getValue().toByteArray()));
        }
        return result;
    }

    private static byte[] concatBlobData(List<ReadBlobResponse> frames) throws IOException {
        var out = new ByteArrayOutputStream();
        for (int i = 1; i < frames.size(); i++) {
            assertThat(frames.get(i).hasData()).isTrue();
            assertThat(frames.get(i).getData().size()).isLessThanOrEqualTo(DocumentStreams.CHUNK_SIZE);
            frames.get(i).getData().writeTo(out);
        }
        return out.toByteArray();
    }

    private List<String> listS3Objects() {
        return s3().listObjectsV2(ListObjectsV2Request.builder().bucket(BUCKET).prefix(f.prefix + "/").build())
                .contents()
                .stream()
                .map(S3Object::key)
                .toList();
    }

    private static <T extends com.google.protobuf.Message> T detail(Runnable call, Status.Code code, Class<T> type) {
        try {
            call.run();
        } catch (StatusRuntimeException failure) {
            assertThat(failure.getStatus().getCode()).isEqualTo(code);
            var status = StatusProto.fromThrowable(failure);
            assertThat(status).isNotNull();
            try {
                return status.getDetails(0).unpack(type);
            } catch (com.google.protobuf.InvalidProtocolBufferException wrong) {
                throw new AssertionError(wrong);
            }
        }
        throw new AssertionError("call succeeded");
    }

    private static class ForwardingStack implements ai.protomolt.proto.repo.stack.Stack {
        private final ai.protomolt.proto.repo.stack.Stack delegate;

        ForwardingStack(ai.protomolt.proto.repo.stack.Stack delegate) {
            this.delegate = delegate;
        }

        @Override public String stackId() { return delegate.stackId(); }
        @Override public StackRef put(java.io.InputStream body, long expectedLength) throws IOException {
            return delegate.put(body, expectedLength);
        }
        @Override public StackRef put(byte[] body) throws IOException { return delegate.put(body); }
        @Override public java.io.InputStream open(StackRef ref) throws IOException { return delegate.open(ref); }
        @Override public java.io.InputStream openRange(StackRef ref, long offset, long length) throws IOException {
            return delegate.openRange(ref, offset, length);
        }
        @Override public long head(StackRef ref) throws IOException { return delegate.head(ref); }
        @Override public void delete(StackRef ref) throws IOException { delegate.delete(ref); }
        @Override public void close() throws IOException { delegate.close(); }
    }
}
