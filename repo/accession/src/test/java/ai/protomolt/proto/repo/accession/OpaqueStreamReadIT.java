package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.OccurrenceLocator;
import ai.protomolt.proto.repo.v2.OpaqueAttestation;
import ai.protomolt.proto.repo.v2.PartSelection;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.ReadDocumentRequest;
import ai.protomolt.proto.repo.v2.ReadDocumentResponse;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrenceStep;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.StackRef;
import ai.protomolt.proto.repo.v2.StoredManifest;
import ai.protomolt.proto.repo.v2.StoredPartEntry;
import ai.protomolt.proto.repo.v2.ValidationMode;
import ai.protomolt.proto.repo.v1.ParserResult;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.DriverManager;
import java.time.Clock;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Streamed {@code ReadDocument} over opaque parts and the read-side reservation
 * (issue #423): an opaque part streams chunk by chunk under a fixed two-chunk
 * reservation instead of being buffered in full, a refused reservation is
 * RESOURCE_EXHAUSTED before anything is opened or allocated, and a corrupt
 * catalog length is stopped by the reservation, never pre-allocated.
 */
class OpaqueStreamReadIT {
    private static final String OPAQUE_URL = "type.test/opaque.Widget";
    private static final int CHUNK = DocumentStreams.CHUNK_SIZE;

    private AccessionFixture f;
    private String account;
    private UUID credential;
    private RepositoryCaller caller;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
        credential = UUID.randomUUID();
        f.catalog.registerCredential(ISSUER, credential, "chunker");
        caller = writer(account, "chunker", credential, 1);
    }

    @AfterEach void close() { f.close(); }

    @Test void opaquePartStreamsChunkByChunkUnderATwoChunkReservation() throws Exception {
        var address = address(account, "doc-opaque-stream");
        var payload = opaquePayload(5 * CHUNK);
        f.opaqueStepGrant(account, credential, "chunker", true, "grant-producer", OPAQUE_URL);
        f.opaqueSaves.save(caller, opaqueCreate("op-1", address, "doc-opaque-stream", payload));
        var coreEntry = corePart(f.catalog.manifest(address, 1).orElseThrow());
        long partLength = coreEntry.getRef().getLength();
        assertThat(partLength).isGreaterThan(4L * CHUNK);
        assertThat(coreEntry.getEntry().getOpaque()).isTrue();

        var bytesReadFromStack = new AtomicLong();
        var opens = new AtomicInteger();
        var closes = new AtomicInteger();
        var tracking = new CountingReadStack(f.stack, bytesReadFromStack, opens, closes);
        var reserved = new AtomicLong();
        var released = new AtomicLong();
        var largestReservation = new AtomicLong();
        var reads = new DocumentReads(f.catalog,
                new DocumentSaves.Drives("default", java.util.Map.of("default", tracking)),
                limits(),
                bytes -> {
                    reserved.addAndGet(bytes);
                    largestReservation.accumulateAndGet(bytes, Math::max);
                    return () -> released.addAndGet(bytes);
                },
                Clock.systemUTC());

        var frames = new ArrayList<ReadDocumentResponse>();
        var readAtFirstFrame = new AtomicLong(-1);
        reads.readDocument(caller, coreRead(address), new StreamObserver<>() {
            @Override public void onNext(ReadDocumentResponse value) {
                if (value.hasPartData() && readAtFirstFrame.get() < 0) {
                    readAtFirstFrame.set(bytesReadFromStack.get());
                }
                frames.add(value);
            }
            @Override public void onError(Throwable t) { throw new AssertionError("unexpected onError", t); }
            @Override public void onCompleted() {}
        });

        assertThat(readAtFirstFrame.get())
                .as("the first frame goes out before the part is fully read: no full buffering")
                .isGreaterThan(0)
                .isLessThanOrEqualTo(2L * CHUNK)
                .isLessThan(partLength);
        assertThat(largestReservation.get())
                .as("an opaque part reserves at most two chunks, never its full length")
                .isLessThanOrEqualTo(2L * CHUNK);
        assertThat(released.get()).isEqualTo(reserved.get());
        assertThat(closes.get()).isEqualTo(opens.get());

        assertThat(frames.getFirst().hasManifest()).isTrue();
        assertThat(frames.getLast().hasComplete()).isTrue();
        var partFrames = frames.stream().filter(ReadDocumentResponse::hasPartData)
                .map(ReadDocumentResponse::getPartData).toList();
        assertThat(partFrames.size()).isGreaterThanOrEqualTo(5);
        assertThat(partFrames.getLast().getLast()).isTrue();
        assertThat(partFrames.subList(0, partFrames.size() - 1)).noneMatch(frame -> frame.getLast());
        var reassembled = new ByteArrayOutputStream();
        long offset = 0;
        for (var frame : partFrames) {
            assertThat(frame.getOffset()).isEqualTo(offset);
            frame.getData().writeTo(reassembled);
            offset += frame.getData().size();
        }
        try (var stream = f.stack.open(coreEntry.getRef())) {
            assertThat(reassembled.toByteArray()).isEqualTo(stream.readAllBytes());
        }
    }

    @Test void corruptOpaquePartFailsTheStreamWithoutItsLastFrameOrCompletion() throws Exception {
        var address = address(account, "doc-opaque-corrupt");
        var payload = opaquePayload(3 * CHUNK);
        f.opaqueStepGrant(account, credential, "chunker", true, "grant-producer", OPAQUE_URL);
        f.opaqueSaves.save(caller, opaqueCreate("op-1", address, "doc-opaque-corrupt", payload));
        var coreEntry = corePart(f.catalog.manifest(address, 1).orElseThrow());

        // Flip one byte in the middle of the stored object, keeping its length, so only
        // the digest check at end of stream can catch the corruption.
        byte[] stored = s3().getObject(GetObjectRequest.builder()
                .bucket(BUCKET).key(coreEntry.getRef().getKey()).build()).readAllBytes();
        stored[stored.length / 2] ^= 0x55;
        s3().putObject(PutObjectRequest.builder().bucket(BUCKET).key(coreEntry.getRef().getKey()).build(),
                RequestBody.fromBytes(stored));

        var frames = new ArrayList<ReadDocumentResponse>();
        var error = new AtomicReference<Throwable>();
        f.reads.readDocument(caller, coreRead(address), new StreamObserver<>() {
            @Override public void onNext(ReadDocumentResponse value) { frames.add(value); }
            @Override public void onError(Throwable t) { error.set(t); }
            @Override public void onCompleted() { throw new AssertionError("unexpected onCompleted"); }
        });

        assertThat(error.get()).isInstanceOfSatisfying(StatusRuntimeException.class, failure ->
                assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.DATA_LOSS));
        assertThat(frames)
                .as("the stream terminates without the part's last frame or ReadComplete")
                .noneMatch(frame -> frame.hasPartData() && frame.getPartData().getLast())
                .noneMatch(ReadDocumentResponse::hasComplete);
    }

    @Test void aRefusedReadReservationIsResourceExhaustedBeforeTheStackIsOpened() {
        var address = address(account, "doc-read-exhausted");
        f.saves.save(processAuthority(), create("op-1", address,
                document(account, "doc-read-exhausted", "title")).build());
        var opens = new AtomicInteger();
        var reads = new DocumentReads(f.catalog,
                new DocumentSaves.Drives("default", java.util.Map.of("default",
                        new CountingReadStack(f.stack, new AtomicLong(), opens, new AtomicInteger()))),
                limits(),
                bytes -> {
                    throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,
                            "read reservation exhausted");
                },
                Clock.systemUTC());

        var frames = new ArrayList<ReadDocumentResponse>();
        var error = new AtomicReference<Throwable>();
        reads.readDocument(processAuthority(),
                ReadDocumentRequest.newBuilder().setAddress(address).build(),
                new StreamObserver<>() {
                    @Override public void onNext(ReadDocumentResponse value) { frames.add(value); }
                    @Override public void onError(Throwable t) { error.set(t); }
                    @Override public void onCompleted() { throw new AssertionError("unexpected onCompleted"); }
                });

        assertThat(error.get()).isInstanceOfSatisfying(StatusRuntimeException.class, failure ->
                assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
        assertThat(opens.get()).as("a refused reservation opens nothing and buffers nothing").isZero();
        assertThat(frames).noneMatch(ReadDocumentResponse::hasPartData);
        assertThat(frames).noneMatch(ReadDocumentResponse::hasComplete);
    }

    @Test void aCorruptOversizedCatalogLengthIsStoppedByTheReservationWithoutAllocation() throws Exception {
        var address = address(account, "doc-corrupt-length");
        f.saves.save(processAuthority(), create("op-1", address,
                document(account, "doc-corrupt-length", "title")).build());
        long corruptLength = 1536L * 1024 * 1024;
        corruptCorePartLength(corruptLength);

        long budget = 64L * 1024 * 1024;
        var requested = new ArrayList<Long>();
        var opens = new AtomicInteger();
        var reads = new DocumentReads(f.catalog,
                new DocumentSaves.Drives("default", java.util.Map.of("default",
                        new CountingReadStack(f.stack, new AtomicLong(), opens, new AtomicInteger()))),
                limits(),
                bytes -> {
                    requested.add(bytes);
                    if (bytes > budget) {
                        throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,
                                "read reservation exhausted: " + bytes + " > " + budget);
                    }
                    return () -> {};
                },
                Clock.systemUTC());

        var frames = new ArrayList<ReadDocumentResponse>();
        var error = new AtomicReference<Throwable>();
        reads.readDocument(processAuthority(), coreRead(address), new StreamObserver<>() {
            @Override public void onNext(ReadDocumentResponse value) { frames.add(value); }
            @Override public void onError(Throwable t) { error.set(t); }
            @Override public void onCompleted() { throw new AssertionError("unexpected onCompleted"); }
        });

        assertThat(requested).contains(corruptLength);
        assertThat(error.get()).isInstanceOfSatisfying(StatusRuntimeException.class, failure ->
                assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED));
        assertThat(opens.get())
                .as("the corrupt length never reaches an open stream or a buffer")
                .isZero();
        assertThat(frames).noneMatch(ReadDocumentResponse::hasPartData);
    }

    @Test void smallOpaquePartReservesAllocatedChunkBuffersAndReleasesAfterFinalCallback() {
        var address = address(account, "review-small-opaque");
        f.opaqueStepGrant(account, credential, "chunker", true, "grant-producer", OPAQUE_URL);
        f.opaqueSaves.save(caller, opaqueCreate("op-review", address, "review-small-opaque", opaquePayload(1)));

        var held = new AtomicLong();
        var duringCallback = new AtomicLong(-1);
        var atComplete = new AtomicLong(-1);
        var error = new AtomicReference<Throwable>();
        var reads = new DocumentReads(f.catalog,
                new DocumentSaves.Drives("default", java.util.Map.of("default", f.stack)),
                limits(),
                bytes -> {
                    held.addAndGet(bytes);
                    return () -> held.addAndGet(-bytes);
                },
                Clock.systemUTC());
        reads.readDocument(caller, coreRead(address), new StreamObserver<>() {
            @Override public void onNext(ReadDocumentResponse value) {
                if (value.hasPartData()) duringCallback.set(held.get());
                if (value.hasComplete()) atComplete.set(held.get());
            }
            @Override public void onError(Throwable failure) { error.set(failure); }
            @Override public void onCompleted() {}
        });

        assertThat(error.get()).isNull();
        assertThat(duringCallback.get())
                .as("two allocated 64 KiB arrays must be covered during callback")
                .isEqualTo(2L * CHUNK);
        assertThat(atComplete.get()).isZero();
        assertThat(held.get()).isZero();
    }

    @Test void opaqueStreamReleasesReservationAndClosesStreamOnPauseCancelAndCallbackThrow() {
        var address = address(account, "doc-opaque-lifecycle");
        f.opaqueStepGrant(account, credential, "chunker", true, "grant-producer", OPAQUE_URL);
        var docWithParsed = Document.newBuilder()
                .setDocId("doc-opaque-lifecycle")
                .setOwnership(ownership(account, "chunker"))
                .setSearchMetadata(ai.protomolt.proto.repo.v1.SearchMetadata.newBuilder().setTitle("opaque"))
                .setStructuredData(Any.newBuilder().setTypeUrl(OPAQUE_URL).setValue(opaquePayload(3 * CHUNK)))
                .putParserResults("docling", ParserResult.newBuilder()
                        .setParserName("docling")
                        .setParserVersion("1")
                        .setStatus(ai.protomolt.proto.repo.v1.ParseStatus.PARSE_STATUS_OK)
                        .build())
                .build();
        f.opaqueSaves.save(caller, SaveDocumentRequest.newBuilder()
                .setOperationId("op-lifecycle")
                .setTarget(ai.protomolt.proto.repo.v2.DocumentTarget.newBuilder().setAddress(address))
                .setExpected(ai.protomolt.proto.repo.v2.ExpectedRevision.newBuilder().setNone(true))
                .setDocument(docWithParsed)
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE))
                .addWrites(put(DocumentPart.DOCUMENT_PART_PARSED))
                .setOwner(stepOwner("chunker"))
                .setValidationMode(ValidationMode.VALIDATION_MODE_OPAQUE_ALLOWED)
                .addOpaqueAttestations(OpaqueAttestation.newBuilder()
                        .setOccurrence(OccurrenceLocator.newBuilder()
                                .setPart(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                                .addSteps(RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(4)))
                        .setTypeUrl(OPAQUE_URL))
                .build());

        var opens = new AtomicInteger();
        var closes = new AtomicInteger();
        var held = new AtomicLong();
        var tracking = new CountingReadStack(f.stack, new AtomicLong(), opens, closes);
        var reads = new DocumentReads(f.catalog,
                new DocumentSaves.Drives("default", java.util.Map.of("default", tracking)),
                limits(),
                bytes -> {
                    held.addAndGet(bytes);
                    return () -> held.addAndGet(-bytes);
                },
                Clock.systemUTC());

        // 1. Backpressure pause immediately after the opaque CORE part's final frame, before PARSED loads:
        //    the opaque reservation is 0 while paused, and PARSED reserves only its own length when resumed.
        var ready = new java.util.concurrent.atomic.AtomicBoolean(true);
        var onReady = new AtomicReference<Runnable>();
        var heldWhilePausedBetweenParts = new AtomicLong(-1);
        var heldDuringParsed = new AtomicLong(-1);
        var allPartsRequest = ReadDocumentRequest.newBuilder().setAddress(address).setAllowOpaque(true).build();
        reads.readDocument(caller, allPartsRequest, new ServerCallStreamObserver<>() {
            @Override public boolean isCancelled() { return false; }
            @Override public void setOnCancelHandler(Runnable onCancelHandler) {}
            @Override public void setCompression(String compression) {}
            @Override public boolean isReady() { return ready.get(); }
            @Override public void setOnReadyHandler(Runnable handler) { onReady.set(handler); }
            @Override public void request(int count) {}
            @Override public void setMessageCompression(boolean enable) {}
            @Override public void disableAutoInboundFlowControl() {}
            @Override public void onNext(ReadDocumentResponse value) {
                if (value.hasPartData()) {
                    var pd = value.getPartData();
                    if (pd.getSelector().getPart() == DocumentPart.DOCUMENT_PART_CORE && pd.getLast()) {
                        ready.set(false);
                    } else if (pd.getSelector().getPart() == DocumentPart.DOCUMENT_PART_PARSED) {
                        heldDuringParsed.set(held.get());
                    }
                }
            }
            @Override public void onError(Throwable t) { throw new AssertionError("unexpected onError", t); }
            @Override public void onCompleted() {}
        });
        heldWhilePausedBetweenParts.set(held.get());
        assertThat(heldWhilePausedBetweenParts.get())
                .as("opaque reservation is released before pausing between parts")
                .isZero();
        ready.set(true);
        onReady.get().run();
        assertThat(heldDuringParsed.get())
                .as("subsequent typed part holds only its own reservation, not the earlier opaque buffers")
                .isGreaterThan(0L)
                .isLessThan(CHUNK);
        assertThat(held.get()).isZero();
        assertThat(closes.get()).isEqualTo(opens.get());

        // 2. Reentrant mid-stream cancellation during the first opaque PartData callback:
        var cancelHandler = new AtomicReference<Runnable>();
        var cancelledFlag = new java.util.concurrent.atomic.AtomicBoolean(false);
        var heldDuringCancelledCallback = new AtomicLong(-1);
        reads.readDocument(caller, coreRead(address), new ServerCallStreamObserver<>() {
            @Override public boolean isCancelled() { return cancelledFlag.get(); }
            @Override public void setOnCancelHandler(Runnable handler) { cancelHandler.set(handler); }
            @Override public void setCompression(String compression) {}
            @Override public boolean isReady() { return true; }
            @Override public void setOnReadyHandler(Runnable handler) {}
            @Override public void request(int count) {}
            @Override public void setMessageCompression(boolean enable) {}
            @Override public void disableAutoInboundFlowControl() {}
            @Override public void onNext(ReadDocumentResponse value) {
                if (value.hasPartData()) {
                    cancelledFlag.set(true);
                    cancelHandler.get().run();
                    heldDuringCancelledCallback.set(held.get());
                }
            }
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        });
        assertThat(heldDuringCancelledCallback.get()).isEqualTo(2L * CHUNK);
        assertThat(held.get()).isZero();
        assertThat(closes.get()).isEqualTo(opens.get());

        // 3. Throwing onNext callback on an opaque PartData frame:
        var throwError = new AtomicReference<Throwable>();
        reads.readDocument(caller, coreRead(address), new StreamObserver<>() {
            @Override public void onNext(ReadDocumentResponse value) {
                if (value.hasPartData()) {
                    throw new IllegalStateException("boom");
                }
            }
            @Override public void onError(Throwable t) { throwError.set(t); }
            @Override public void onCompleted() {}
        });
        assertThat(throwError.get()).isNotNull();
        assertThat(held.get()).isZero();
        assertThat(closes.get()).isEqualTo(opens.get());
    }

    // ------------------------------------------------------------------ helpers

    /** Rewrites revision 1's stored manifest with a corrupt oversized CORE part length. */
    private void corruptCorePartLength(long length) throws Exception {
        try (var connection = DriverManager.getConnection(f.url,
                AccessionFixture.POSTGRES.getUsername(), AccessionFixture.POSTGRES.getPassword())) {
            byte[] bytes;
            try (var select = connection.prepareStatement("SELECT manifest FROM catalog_revisions WHERE revision = 1");
                 var rows = select.executeQuery()) {
                assertThat(rows.next()).isTrue();
                bytes = rows.getBytes(1);
            }
            var stored = StoredManifest.parseFrom(bytes).toBuilder();
            for (var part : stored.getPartsBuilderList()) {
                if (part.getEntry().getSelector().getPart() == DocumentPart.DOCUMENT_PART_CORE) {
                    part.getRefBuilder().setLength(length);
                    part.getEntryBuilder().setSizeBytes(length);
                }
            }
            var corrupt = stored.build().toByteString();
            connection.createStatement().execute(
                    "ALTER TABLE catalog_revisions DISABLE TRIGGER catalog_revisions_immutable");
            try (var update = connection.prepareStatement(
                    "UPDATE catalog_revisions SET manifest = ?, manifest_sha256 = ? WHERE revision = 1")) {
                update.setBytes(1, corrupt.toByteArray());
                update.setString(2, DocumentSaves.sha256(corrupt));
                assertThat(update.executeUpdate()).isEqualTo(1);
            } finally {
                connection.createStatement().execute(
                        "ALTER TABLE catalog_revisions ENABLE TRIGGER catalog_revisions_immutable");
            }
        }
    }

    private static ReadDocumentRequest coreRead(ai.protomolt.proto.repo.v1.NodeAddress address) {
        return ReadDocumentRequest.newBuilder().setAddress(address)
                .setSelection(PartSelection.newBuilder().addSelectors(
                        PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE)))
                .setAllowOpaque(true)
                .build();
    }

    private static StoredPartEntry corePart(StoredManifest stored) {
        return stored.getPartsList().stream()
                .filter(part -> part.getEntry().getSelector().getPart() == DocumentPart.DOCUMENT_PART_CORE)
                .findFirst().orElseThrow();
    }

    private static ByteString opaquePayload(int size) {
        var payload = new byte[size];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) ((i * 31 + 7) & 0xFF);
        return ByteString.copyFrom(payload);
    }

    private SaveDocumentRequest opaqueCreate(String operation, ai.protomolt.proto.repo.v1.NodeAddress address,
            String doc, ByteString payload) {
        var document = Document.newBuilder().setDocId(doc).setOwnership(ownership(account, "chunker"))
                .setSearchMetadata(ai.protomolt.proto.repo.v1.SearchMetadata.newBuilder().setTitle("opaque"))
                .setStructuredData(Any.newBuilder().setTypeUrl(OPAQUE_URL).setValue(payload))
                .build();
        return SaveDocumentRequest.newBuilder().setOperationId(operation)
                .setTarget(ai.protomolt.proto.repo.v2.DocumentTarget.newBuilder().setAddress(address))
                .setExpected(ai.protomolt.proto.repo.v2.ExpectedRevision.newBuilder().setNone(true))
                .setDocument(document)
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE))
                .setOwner(stepOwner("chunker"))
                .setValidationMode(ValidationMode.VALIDATION_MODE_OPAQUE_ALLOWED)
                .addOpaqueAttestations(OpaqueAttestation.newBuilder()
                        .setOccurrence(OccurrenceLocator.newBuilder()
                                .setPart(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                                .addSteps(RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(4)))
                        .setTypeUrl(OPAQUE_URL))
                .build();
    }

    /** Forwards to the fixture stack, counting opened streams, closed streams, and bytes read. */
    private static final class CountingReadStack implements ai.protomolt.proto.repo.stack.Stack {
        private final ai.protomolt.proto.repo.stack.Stack delegate;
        private final AtomicLong bytesRead;
        private final AtomicInteger opens;
        private final AtomicInteger closes;

        CountingReadStack(ai.protomolt.proto.repo.stack.Stack delegate, AtomicLong bytesRead,
                AtomicInteger opens, AtomicInteger closes) {
            this.delegate = delegate;
            this.bytesRead = bytesRead;
            this.opens = opens;
            this.closes = closes;
        }

        @Override public String stackId() { return delegate.stackId(); }
        @Override public StackRef put(InputStream body, long expectedLength) throws IOException {
            return delegate.put(body, expectedLength);
        }
        @Override public StackRef put(byte[] body) throws IOException { return delegate.put(body); }
        @Override public InputStream open(StackRef ref) throws IOException {
            var stream = delegate.open(ref);
            opens.incrementAndGet();
            return new FilterInputStream(stream) {
                @Override public int read() throws IOException {
                    int b = super.read();
                    if (b >= 0) bytesRead.incrementAndGet();
                    return b;
                }
                @Override public int read(byte[] b, int off, int len) throws IOException {
                    int n = super.read(b, off, len);
                    if (n > 0) bytesRead.addAndGet(n);
                    return n;
                }
                @Override public void close() throws IOException {
                    closes.incrementAndGet();
                    super.close();
                }
            };
        }
        @Override public InputStream openRange(StackRef ref, long offset, long length) throws IOException {
            return delegate.openRange(ref, offset, length);
        }
        @Override public long head(StackRef ref) throws IOException { return delegate.head(ref); }
        @Override public void delete(StackRef ref) throws IOException { delegate.delete(ref); }
        @Override public void close() throws IOException { delegate.close(); }
    }
}
