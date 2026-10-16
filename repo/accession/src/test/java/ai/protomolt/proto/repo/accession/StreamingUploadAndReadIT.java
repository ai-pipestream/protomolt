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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
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
        var merged = Document.newBuilder();
        for (var partBytes : reassembled.values()) {
            merged.mergeFrom(partBytes);
        }
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
                .setSave(create("op-proto", address, forgedDoc))
                .setBlobId("b1")
                .build();
        assertMalformedUnrecorded(() -> sendUpload(forgedHeader, List.of(ByteString.copyFromUtf8("abc"))));

        // 7. Streamed encoded_part also carries inline fields for that part in save.document.
        var duplicateEncodedCore = UploadHeader.newBuilder()
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

        // Retry with the same operation_id after commit -> returns recorded accession with replayed = true.
        var replayed = sendUpload(header, List.of(ByteString.copyFrom(payload)));
        assertThat(replayed.getSaved().getAccession()).isEqualTo(committed.getSaved().getAccession());
        assertThat(replayed.getSaved().getReplayed()).isTrue();
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1L);
    }

    @Test
    void clientCancellationMidUploadAbortsStackWriteAndRecordsNoOutcome() throws Exception {
        var address = address(account, "doc-cancel-upload");
        var doc = documentWithPendingBlob(account, "doc-cancel-upload", "b1");
        var header = UploadHeader.newBuilder()
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
        } finally {
            httpServer.stop(0);
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
}
