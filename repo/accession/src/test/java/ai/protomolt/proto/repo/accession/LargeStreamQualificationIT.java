package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.ParseStatus;
import ai.protomolt.proto.repo.v1.ParserResult;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.*;
import com.google.protobuf.ByteString;
import com.sun.net.httpserver.HttpServer;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Qualification test for 600 MB streaming uploads and downloads over gRPC and HTTP
 * with bounded heap usage, slow-consumer backpressure, and mid-stream cancellation
 * (requirements §15.10, §15.17).
 */
class LargeStreamQualificationIT {
    private static final long SIX_HUNDRED_MB = 600L * 1024 * 1024;
    private static final int PATTERN_BYTES = 1024 * 1024; // 1 MiB repeating deterministic block
    private static final int CHUNK_BYTES = 64 * 1024; // 64 KiB frame size
    private static final long MAX_ALLOWED_HEAP_DELTA_BYTES = 128L * 1024 * 1024; // Strictly < 128 MiB for 600 MB stream

    private static final byte[] PATTERN = buildPattern();
    private static final ByteString[] PATTERN_CHUNKS = buildPatternChunks();
    private static final String SIX_HUNDRED_MB_SHA256 = computeSixHundredMbSha256();

    private AccessionFixture f;
    private String account;
    private Server server;
    private ManagedChannel channel;
    private DocumentServiceGrpc.DocumentServiceBlockingStub blockingStub;
    private DocumentServiceGrpc.DocumentServiceStub asyncStub;

    @BeforeEach
    void setUp() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
        var serverName = "large-stream-" + UUID.randomUUID();
        server = InProcessServerBuilder.forName(serverName)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(new AccessionGrpcService(f.saves, f.uploads, f.reads, authentication ->
                        new ai.protomolt.proto.repo.spi.RepositoryCaller(
                                authentication.caller().name(),
                                authentication.caller().unrestricted())))
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

    @Test
    void grpcSixHundredMegabyteUploadAndReadBlobAndReadDocumentStayBoundedInHeap() throws Exception {
        var address = address(account, "doc-600mb-grpc");
        var document = Document.newBuilder()
                .setDocId("doc-600mb-grpc")
                .setOwnership(ownership(account))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("600mb-grpc.bin").setBody("large stream"))
                .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder()
                        .setBlobId("blob-600mb")
                        .setMimeType("application/octet-stream")
                        .setFilename("600mb-grpc.bin")
                        .setSizeBytes(SIX_HUNDRED_MB)))
                .putParserResults("tika", ParserResult.newBuilder()
                        .setParserName("tika")
                        .setParserVersion("1")
                        .setStatus(ParseStatus.PARSE_STATUS_OK)
                        .build())
                .build();

        var header = UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(create("op-600mb-grpc", address, document))
                .setBlobId("blob-600mb")
                .setDeclaredLength(SIX_HUNDRED_MB)
                .setExpectedSha256(SIX_HUNDRED_MB_SHA256)
                .build();

        long baselineHeap = settledUsedHeap();
        var peakDelta = new AtomicLong(0L);

        var uploadResponse = streamGrpcUpload(header, SIX_HUNDRED_MB, baselineHeap, peakDelta);
        assertThat(uploadResponse.getSaved().getAccession().getRevision()).isEqualTo(1L);
        assertThat(uploadResponse.getSaved().getReplayed()).isFalse();
        assertThat(peakDelta.get())
                .as("Peak live heap delta during 600 MB gRPC UploadDocument must remain strictly bounded")
                .isLessThan(MAX_ALLOWED_HEAP_DELTA_BYTES);

        // ReadDocument returns the manifest and claim-checked BLOBS metadata without pulling the 600 MB raw blob.
        var docFrames = new java.util.ArrayList<ReadDocumentResponse>();
        blockingStub.readDocument(ReadDocumentRequest.newBuilder().setAddress(address).build())
                .forEachRemaining(docFrames::add);
        assertThat(docFrames.getFirst().hasManifest()).isTrue();
        assertThat(docFrames.getLast().hasComplete()).isTrue();

        // Full 600 MB ReadBlob stream verified on the fly with bounded heap.
        long readBaseline = settledUsedHeap();
        var readPeakDelta = new AtomicLong(0L);
        var digest = SaveIntents.newSha256();
        long totalRead = 0L;
        BlobHeader blobHeader = null;
        int chunkCount = 0;

        var iter = blockingStub.readBlob(ReadBlobRequest.newBuilder()
                .setAddress(address)
                .setBlobId("blob-600mb")
                .build());
        while (iter.hasNext()) {
            var frame = iter.next();
            if (frame.hasHeader()) {
                blobHeader = frame.getHeader();
            } else {
                var data = frame.getData();
                verifySliceAgainstPattern(data, totalRead);
                digest.update(data.asReadOnlyByteBuffer());
                totalRead += data.size();
                chunkCount++;
                if ((chunkCount & 2047) == 0) {
                    recordHeapDelta(readBaseline, readPeakDelta);
                }
            }
        }
        assertThat(blobHeader).isNotNull();
        assertThat(blobHeader.getTotalLength()).isEqualTo(SIX_HUNDRED_MB);
        assertThat(blobHeader.getSha256()).isEqualTo(SIX_HUNDRED_MB_SHA256);
        assertThat(totalRead).isEqualTo(SIX_HUNDRED_MB);
        assertThat(HexFormat.of().formatHex(digest.digest())).isEqualTo(SIX_HUNDRED_MB_SHA256);
        assertThat(readPeakDelta.get())
                .as("Peak live heap delta during 600 MB ReadBlob must remain strictly bounded")
                .isLessThan(MAX_ALLOWED_HEAP_DELTA_BYTES);

        // Range read near the end of the 600 MB object (offset = 599 MiB + 12345, length = 256 KiB).
        long rangeOffset = 599L * 1024 * 1024 + 12_345;
        long rangeLength = 256L * 1024;
        long rangeRead = 0L;
        var rangeIter = blockingStub.readBlob(ReadBlobRequest.newBuilder()
                .setAddress(address)
                .setBlobId("blob-600mb")
                .setOffset(rangeOffset)
                .setLength(rangeLength)
                .build());
        while (rangeIter.hasNext()) {
            var frame = rangeIter.next();
            if (frame.hasData()) {
                var data = frame.getData();
                verifySliceAgainstPattern(data, rangeOffset + rangeRead);
                rangeRead += data.size();
            }
        }
        assertThat(rangeRead).isEqualTo(rangeLength);
    }

    @Test
    void httpSixHundredMegabyteStreamingUploadAndReadBlobStayBoundedInHeap() throws Exception {
        String token = "http-600mb-token-" + UUID.randomUUID();
        var handler = new UploadHttpHandler(f.uploads, token);
        var httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        httpServer.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        httpServer.createContext(UploadHttpHandler.UPLOAD_PATH, handler);
        httpServer.start();
        var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        int port = httpServer.getAddress().getPort();

        try {
            long baselineHeap = settledUsedHeap();
            var peakDelta = new AtomicLong(0L);

            var uri = URI.create("http://127.0.0.1:" + port + UploadHttpHandler.UPLOAD_PATH
                    + "?account_id=" + account
                    + "&datasource_id=ds-1"
                    + "&drive=default"
                    + "&filename=600mb-http.bin"
                    + "&operation_id=op-600mb-http"
                    + "&doc_id=doc-600mb-http"
                    + "&blob_id=blob-600mb-http");

            var bodyPublisher = HttpRequest.BodyPublishers.ofInputStream(
                    () -> new PatternInputStream(SIX_HUNDRED_MB, baselineHeap, peakDelta));
            var request = HttpRequest.newBuilder(uri)
                    .header("Authorization", "Bearer " + token)
                    .header("X-Content-Sha256", SIX_HUNDRED_MB_SHA256)
                    .POST(HttpRequest.BodyPublishers.fromPublisher(bodyPublisher, SIX_HUNDRED_MB))
                    .build();

            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body())
                    .contains("\"operation_id\":\"op-600mb-http\"")
                    .contains("\"doc_id\":\"doc-600mb-http\"")
                    .contains("\"revision\":1")
                    .contains("\"replayed\":false")
                    .contains("\"size_bytes\":" + SIX_HUNDRED_MB)
                    .contains("\"sha256\":\"" + SIX_HUNDRED_MB_SHA256 + "\"");
            assertThat(peakDelta.get())
                    .as("Peak live heap delta during 600 MB HTTP upload must remain strictly bounded")
                    .isLessThan(MAX_ALLOWED_HEAP_DELTA_BYTES);

            // Verify a range at the tail of the HTTP-uploaded 600 MB blob via ReadBlob.
            long tailOffset = SIX_HUNDRED_MB - 128 * 1024;
            long tailRead = 0L;
            var iter = blockingStub.readBlob(ReadBlobRequest.newBuilder()
                    .setAddress(address(account, "doc-600mb-http"))
                    .setBlobId("blob-600mb-http")
                    .setOffset(tailOffset)
                    .setLength(0)
                    .build());
            while (iter.hasNext()) {
                var frame = iter.next();
                if (frame.hasData()) {
                    verifySliceAgainstPattern(frame.getData(), tailOffset + tailRead);
                    tailRead += frame.getData().size();
                }
            }
            assertThat(tailRead).isEqualTo(128 * 1024);
        } finally {
            httpServer.stop(0);
        }
    }

    @Test
    void slowConsumerBackpressureAndMidStreamCancellationOnUploadAndDownload() throws Exception {
        // 1. Mid-stream cancellation on a 600 MB upload after streaming 16 MiB -> aborts S3 multipart upload, 0 objects, NoRecordedOutcome.
        var cancelAddress = address(account, "doc-cancel-600mb");
        var cancelDoc = Document.newBuilder()
                .setDocId("doc-cancel-600mb")
                .setOwnership(ownership(account))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("cancel").setBody("body"))
                .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder()
                        .setBlobId("blob-cancel")
                        .setMimeType("application/octet-stream")
                        .setFilename("cancel.bin")
                        .setSizeBytes(SIX_HUNDRED_MB)))
                .build();
        var cancelHeader = UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(SaveDocumentRequest.newBuilder()
                        .setOperationId("op-cancel-600mb")
                        .setTarget(DocumentTarget.newBuilder().setAddress(cancelAddress))
                        .setExpected(ExpectedRevision.newBuilder().setNone(true))
                        .setDocument(cancelDoc)
                        .addWrites(put(DocumentPart.DOCUMENT_PART_CORE))
                        .addWrites(put(DocumentPart.DOCUMENT_PART_BLOBS))
                        .setOwner(intakeOwner()))
                .setBlobId("blob-cancel")
                .setDeclaredLength(SIX_HUNDRED_MB)
                .build();

        var uploadCancelledLatch = new CountDownLatch(1);
        var uploadCallRef = new AtomicReference<ClientCallStreamObserver<UploadDocumentRequest>>();
        var uploadObserver = new ClientResponseObserver<UploadDocumentRequest, UploadDocumentResponse>() {
            @Override
            public void beforeStart(ClientCallStreamObserver<UploadDocumentRequest> requestStream) {
                uploadCallRef.set(requestStream);
            }
            @Override public void onNext(UploadDocumentResponse value) {}
            @Override public void onError(Throwable t) { uploadCancelledLatch.countDown(); }
            @Override public void onCompleted() {}
        };
        var reqStream = asyncStub.uploadDocument(uploadObserver);
        reqStream.onNext(UploadDocumentRequest.newBuilder().setHeader(cancelHeader).build());
        for (int i = 0; i < 256; i++) { // 16 MiB
            reqStream.onNext(UploadDocumentRequest.newBuilder().setData(PATTERN_CHUNKS[i % PATTERN_CHUNKS.length]).build());
        }
        uploadCallRef.get().cancel("Cancelling 600 MB upload after 16 MiB", null);
        assertThat(uploadCancelledLatch.await(10, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(100);
        assertThat(blockingStub.getOperationOutcome(GetOperationOutcomeRequest.newBuilder()
                .setAccountId(account)
                .setOperationId("op-cancel-600mb")
                .build()).hasNone()).isTrue();
        assertThat(listS3Objects()).isEmpty();

        // 1b. Mid-stream abort on a 600 MB HTTP upload after streaming 16 MiB -> 0 objects, NoRecordedOutcome.
        String token = "http-cancel-token-" + UUID.randomUUID();
        var httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        httpServer.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        httpServer.createContext(UploadHttpHandler.UPLOAD_PATH, new UploadHttpHandler(f.uploads, token));
        httpServer.start();
        try (var socket = new java.net.Socket("127.0.0.1", httpServer.getAddress().getPort())) {
            var out = socket.getOutputStream();
            String rawRequest = "POST " + UploadHttpHandler.UPLOAD_PATH
                    + "?account_id=" + account
                    + "&datasource_id=ds-1"
                    + "&drive=default"
                    + "&filename=cancel-http.bin"
                    + "&operation_id=op-cancel-http-600mb"
                    + "&doc_id=doc-cancel-http-600mb"
                    + " HTTP/1.1\r\n"
                    + "Host: 127.0.0.1\r\n"
                    + "Authorization: Bearer " + token + "\r\n"
                    + "Content-Length: " + SIX_HUNDRED_MB + "\r\n\r\n";
            out.write(rawRequest.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            for (int i = 0; i < 16; i++) { // 16 MiB
                out.write(PATTERN);
            }
            out.flush();
        }
        Thread.sleep(150);
        httpServer.stop(0);
        assertThat(blockingStub.getOperationOutcome(GetOperationOutcomeRequest.newBuilder()
                .setAccountId(account)
                .setOperationId("op-cancel-http-600mb")
                .build()).hasNone()).isTrue();
        assertThat(f.catalog.current(address(account, "doc-cancel-http-600mb"))).isEmpty();
        assertThat(listS3Objects()).isEmpty();

        // 2. Slow consumer backpressure and mid-stream cancellation on ReadBlob.
        var readAddress = address(account, "doc-slow-read");
        long blobSize = 8L * 1024 * 1024; // 8 MiB (128 chunks of 64 KiB)
        var readDoc = cancelDoc.toBuilder()
                .setDocId("doc-slow-read")
                .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder()
                        .setBlobId("blob-8mb")
                        .setMimeType("application/octet-stream")
                        .setFilename("8mb.bin")
                        .setSizeBytes(blobSize)))
                .build();
        var readHeader = UploadHeader.newBuilder()
                .setStreamed(ai.protomolt.proto.repo.v2.StreamedBody.getDefaultInstance())
                .setSave(SaveDocumentRequest.newBuilder()
                        .setOperationId("op-8mb")
                        .setTarget(DocumentTarget.newBuilder().setAddress(readAddress))
                        .setExpected(ExpectedRevision.newBuilder().setNone(true))
                        .setDocument(readDoc)
                        .addWrites(put(DocumentPart.DOCUMENT_PART_CORE))
                        .addWrites(put(DocumentPart.DOCUMENT_PART_BLOBS))
                        .setOwner(intakeOwner()))
                .setBlobId("blob-8mb")
                .setDeclaredLength(blobSize)
                .build();
        streamGrpcUpload(readHeader, blobSize, settledUsedHeap(), new AtomicLong());

        var receivedFrames = new AtomicInteger();
        var firstFourLatch = new CountDownLatch(4);
        var readCancelLatch = new CountDownLatch(1);
        var readCallRef = new AtomicReference<ClientCallStreamObserver<ReadBlobRequest>>();
        asyncStub.readBlob(
                ReadBlobRequest.newBuilder().setAddress(readAddress).setBlobId("blob-8mb").build(),
                new ClientResponseObserver<ReadBlobRequest, ReadBlobResponse>() {
                    @Override
                    public void beforeStart(ClientCallStreamObserver<ReadBlobRequest> requestStream) {
                        readCallRef.set(requestStream);
                        requestStream.disableAutoRequestWithInitial(4);
                    }
                    @Override
                    public void onNext(ReadBlobResponse value) {
                        receivedFrames.incrementAndGet();
                        firstFourLatch.countDown();
                    }
                    @Override
                    public void onError(Throwable t) {
                        readCancelLatch.countDown();
                    }
                    @Override
                    public void onCompleted() {}
                });
        assertThat(firstFourLatch.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(50);
        // Backpressure: without additional request(n) calls, the server does not flood frames past the requested 4.
        assertThat(receivedFrames.get()).isEqualTo(4);

        // Cancel the slow consumer mid-download.
        readCallRef.get().cancel("Slow consumer cancelled mid-download", null);
        assertThat(readCancelLatch.await(5, TimeUnit.SECONDS)).isTrue();
    }

    // ------------------------------------------------------------------ helpers

    private UploadDocumentResponse streamGrpcUpload(
            UploadHeader header,
            long totalBytes,
            long baselineHeap,
            AtomicLong peakDelta) throws Exception {
        var future = new CompletableFuture<UploadDocumentResponse>();
        var headerSent = new AtomicBoolean(false);
        var bytesSent = new AtomicLong(0L);
        var chunksSent = new AtomicInteger(0);
        var sending = new AtomicBoolean(false);

        var observer = new ClientResponseObserver<UploadDocumentRequest, UploadDocumentResponse>() {
            private ClientCallStreamObserver<UploadDocumentRequest> requestStream;
            private UploadDocumentResponse result;

            @Override
            public void beforeStart(ClientCallStreamObserver<UploadDocumentRequest> requestStream) {
                this.requestStream = requestStream;
                requestStream.setOnReadyHandler(this::drain);
            }

            private void drain() {
                if (!sending.compareAndSet(false, true)) return;
                try {
                    while (requestStream.isReady() && !future.isDone()) {
                        if (headerSent.compareAndSet(false, true)) {
                            requestStream.onNext(UploadDocumentRequest.newBuilder().setHeader(header).build());
                            continue;
                        }
                        long sent = bytesSent.get();
                        if (sent >= totalBytes) {
                            requestStream.onCompleted();
                            return;
                        }
                        int idx = chunksSent.getAndIncrement() % PATTERN_CHUNKS.length;
                        ByteString chunk = PATTERN_CHUNKS[idx];
                        long remaining = totalBytes - sent;
                        if (remaining < chunk.size()) {
                            chunk = chunk.substring(0, (int) remaining);
                        }
                        bytesSent.addAndGet(chunk.size());
                        requestStream.onNext(UploadDocumentRequest.newBuilder().setData(chunk).build());
                        if ((chunksSent.get() & 2047) == 0) {
                            recordHeapDelta(baselineHeap, peakDelta);
                        }
                    }
                } catch (Throwable failure) {
                    future.completeExceptionally(failure);
                } finally {
                    sending.set(false);
                }
            }

            @Override
            public void onNext(UploadDocumentResponse value) {
                this.result = value;
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
                future.complete(result);
            }
        };

        asyncStub.uploadDocument(observer);
        return future.get(5, TimeUnit.MINUTES);
    }

    private static void verifySliceAgainstPattern(ByteString data, long startOffset) {
        int size = data.size();
        if (size == 0) return;
        int firstIdx = (int) (startOffset % PATTERN_BYTES);
        int lastIdx = (int) ((startOffset + size - 1) % PATTERN_BYTES);
        int midIdx = (int) ((startOffset + (size / 2)) % PATTERN_BYTES);
        if (data.byteAt(0) != PATTERN[firstIdx]
                || data.byteAt(size - 1) != PATTERN[lastIdx]
                || data.byteAt(size / 2) != PATTERN[midIdx]) {
            fail("Streamed byte mismatch at offset " + startOffset);
        }
    }

    private static long settledUsedHeap() {
        System.gc();
        var rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }

    private static void recordHeapDelta(long baseline, AtomicLong peakDelta) {
        long current = settledUsedHeap();
        long delta = Math.max(0L, current - baseline);
        peakDelta.accumulateAndGet(delta, Math::max);
    }

    private List<String> listS3Objects() {
        return s3().listObjectsV2(ListObjectsV2Request.builder().bucket(BUCKET).prefix(f.prefix + "/").build())
                .contents()
                .stream()
                .map(S3Object::key)
                .toList();
    }

    private static byte[] buildPattern() {
        byte[] pattern = new byte[PATTERN_BYTES];
        long state = 0x9E3779B97F4A7C15L;
        for (int i = 0; i < pattern.length; i++) {
            state ^= (state << 13);
            state ^= (state >>> 7);
            state ^= (state << 17);
            pattern[i] = (byte) (state & 0xFF);
        }
        return pattern;
    }

    private static ByteString[] buildPatternChunks() {
        int count = PATTERN_BYTES / CHUNK_BYTES;
        ByteString[] chunks = new ByteString[count];
        for (int i = 0; i < count; i++) {
            chunks[i] = ByteString.copyFrom(PATTERN, i * CHUNK_BYTES, CHUNK_BYTES);
        }
        return chunks;
    }

    private static String computeSixHundredMbSha256() {
        var digest = SaveIntents.newSha256();
        int repeats = (int) (SIX_HUNDRED_MB / PATTERN_BYTES);
        for (int i = 0; i < repeats; i++) {
            digest.update(PATTERN, 0, PATTERN.length);
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static final class PatternInputStream extends InputStream {
        private final long totalLength;
        private final long baselineHeap;
        private final AtomicLong peakDelta;
        private long position;
        private long nextSampleAt = 128L * 1024 * 1024;

        PatternInputStream(long totalLength, long baselineHeap, AtomicLong peakDelta) {
            this.totalLength = totalLength;
            this.baselineHeap = baselineHeap;
            this.peakDelta = peakDelta;
        }

        @Override
        public int read() {
            if (position >= totalLength) return -1;
            int b = PATTERN[(int) (position % PATTERN_BYTES)] & 0xFF;
            position++;
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (len == 0) return 0;
            if (position >= totalLength) return -1;
            int patternOffset = (int) (position % PATTERN_BYTES);
            int maxFromPattern = PATTERN_BYTES - patternOffset;
            long remaining = totalLength - position;
            int toCopy = (int) Math.min(len, Math.min(maxFromPattern, remaining));
            System.arraycopy(PATTERN, patternOffset, b, off, toCopy);
            position += toCopy;
            if (position >= nextSampleAt) {
                nextSampleAt += 128L * 1024 * 1024;
                recordHeapDelta(baselineHeap, peakDelta);
            }
            return toCopy;
        }
    }
}
