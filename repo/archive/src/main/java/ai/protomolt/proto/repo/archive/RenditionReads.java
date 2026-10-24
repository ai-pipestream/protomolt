package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.repo.archive.v1.ReadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionResponse;
import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionReadHeader;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.PartSelection;
import ai.protomolt.proto.repo.v2.ReadBlobRequest;
import ai.protomolt.proto.repo.v2.ReadBlobResponse;
import ai.protomolt.proto.repo.v2.ReadDocumentRequest;
import ai.protomolt.proto.repo.v2.ReadDocumentResponse;
import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * {@code ReadRendition} over v2: the header projected from the version's
 * manifest row, then the bytes from {@code ReadBlob} (or the PARSED part frames
 * of {@code ReadDocument}) re-framed to at most 64 KiB and paced by the
 * client's flow control. A whole-object read is checked against the row's
 * SHA-256 before the stream completes, and a mismatch ends it with
 * {@code DATA_LOSS}.
 */
public final class RenditionReads {

    /** The largest data frame the archive contract allows. */
    static final int MAX_FRAME_BYTES = 64 * 1024;

    private final DocumentArchive archive;

    /** @param archive the archive whose manifests plan the reads */
    public RenditionReads(DocumentArchive archive) {
        this.archive = Objects.requireNonNull(archive, "archive");
    }

    /** The revision, manifest row and range one read serves. */
    record Plan(NodeAddress address, long version, RenditionManifestEntry row, long offset, long length) {
        boolean wholeObject() {
            return offset == 0 && length == row.getSizeBytes();
        }

        boolean parsed() {
            return row.getRendition().getName().equals(ArchiveCoordinates.PARSED);
        }
    }

    /**
     * An opened rendition read with its validated {@link RenditionReadHeader} and
     * a streaming {@link InputStream} that pulls chunks on demand and verifies the
     * byte count and whole-object SHA-256 before returning end-of-stream.
     *
     * @param header the validated rendition read header
     * @param body the streaming rendition bytes
     */
    public record OpenedRead(RenditionReadHeader header, InputStream body) {
        /** Validates non-null components. */
        public OpenedRead {
            Objects.requireNonNull(header, "header");
            Objects.requireNonNull(body, "body");
        }
    }

    /**
     * Plans a rendition read and opens a streaming {@link InputStream} over the
     * underlying {@code ReadBlob} or {@code ReadDocument} frames without buffering
     * the whole rendition in memory.
     *
     * @param request the rendition read request
     * @return the planned header and streaming byte input
     */
    public OpenedRead open(ReadRenditionRequest request) {
        Plan plan = archive.planRead(request);
        RenditionReadHeader header = RenditionReadHeader.newBuilder()
                .setVersion(plan.version())
                .setRendition(plan.row().getRendition())
                .setSizeBytes(plan.row().getSizeBytes())
                .setSha256(plan.row().getSha256())
                .setOffset(plan.offset())
                .setLength(plan.length())
                .setWholeObject(plan.wholeObject())
                .build();
        DocumentArchive.requireValidView(header);
        Iterator<ByteString> chunks = plan.parsed() ? openParsedChunks(plan) : openBlobChunks(plan);
        return new OpenedRead(header, new VerifyingChunkInputStream(plan, chunks));
    }

    /**
     * Streams one rendition read to {@code responses}. Planning failures (an
     * unknown rendition, a version the archive does not serve, a range outside
     * the object) end the call before any frame is sent.
     */
    public void read(ReadRenditionRequest request, StreamObserver<ReadRenditionResponse> responses) {
        Plan plan;
        try {
            plan = archive.planRead(request);
        } catch (StatusRuntimeException failure) {
            responses.onError(failure);
            return;
        }
        var pacing = new Pacing(responses);
        try {
            RenditionReadHeader header = RenditionReadHeader.newBuilder()
                    .setVersion(plan.version())
                    .setRendition(plan.row().getRendition())
                    .setSizeBytes(plan.row().getSizeBytes())
                    .setSha256(plan.row().getSha256())
                    .setOffset(plan.offset())
                    .setLength(plan.length())
                    .setWholeObject(plan.wholeObject())
                    .build();
            DocumentArchive.requireValidView(header);
            pacing.send(ReadRenditionResponse.newBuilder().setHeader(header).build());
            MessageDigest digest = plan.wholeObject() ? sha256() : null;
            long sent = plan.parsed() ? streamParsed(plan, pacing, digest) : streamBlob(plan, pacing, digest);
            if (sent != plan.length()) {
                throw ArchiveErrors.dataLoss("Rendition " + plan.row().getRendition().getName() + " of version "
                        + plan.version() + " delivered " + sent + " of " + plan.length() + " bytes", null);
            }
            if (digest != null) {
                String actual = HexFormat.of().formatHex(digest.digest());
                if (!actual.equals(plan.row().getSha256())) {
                    throw ArchiveErrors.dataLoss("Rendition " + plan.row().getRendition().getName() + " of version "
                            + plan.version() + " hashes to " + actual + ", not its manifest's "
                            + plan.row().getSha256(), null);
                }
            }
            responses.onCompleted();
        } catch (StatusRuntimeException failure) {
            responses.onError(failure);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            responses.onError(Status.CANCELLED.withDescription("Rendition read interrupted")
                    .withCause(interrupted).asRuntimeException());
        }
    }

    private long streamBlob(Plan plan, Pacing pacing, MessageDigest digest) throws InterruptedException {
        if (plan.length() == 0) {
            return 0;
        }
        Iterator<ReadBlobResponse> frames = archive.documents().readBlob(ReadBlobRequest.newBuilder()
                .setAddress(plan.address())
                .setRevision(DocumentArchive.revision(plan.version()))
                .setBlobId(ArchiveCoordinates.blobId(plan.row().getRendition().getName(),
                        plan.row().getRendition().getSubKey()))
                .setOffset(plan.offset())
                .setLength(plan.length())
                .build());
        long sent = 0;
        while (frames.hasNext()) {
            ReadBlobResponse frame = frames.next();
            if (frame.hasData()) {
                sent += forward(frame.getData(), pacing, digest);
            }
        }
        return sent;
    }

    /** PARSED part frames carry the whole part; the range is cut here. */
    private long streamParsed(Plan plan, Pacing pacing, MessageDigest digest) throws InterruptedException {
        Iterator<ReadDocumentResponse> frames = archive.documents().readDocument(ReadDocumentRequest.newBuilder()
                .setAddress(plan.address())
                .setRevision(DocumentArchive.revision(plan.version()))
                .setSelection(PartSelection.newBuilder().addSelectors(ArchiveCoordinates.PARSED_PART))
                .build());
        long position = 0;
        long end = plan.offset() + plan.length();
        long sent = 0;
        while (frames.hasNext()) {
            ReadDocumentResponse frame = frames.next();
            if (!frame.hasPartData()) {
                continue;
            }
            ByteString data = frame.getPartData().getData();
            long start = position;
            position += data.size();
            long from = Math.max(start, plan.offset());
            long to = Math.min(position, end);
            if (from < to) {
                sent += forward(data.substring((int) (from - start), (int) (to - start)), pacing, digest);
            }
        }
        return sent;
    }

    private static long forward(ByteString data, Pacing pacing, MessageDigest digest) throws InterruptedException {
        for (int start = 0; start < data.size(); start += MAX_FRAME_BYTES) {
            ByteString frame = data.substring(start, Math.min(data.size(), start + MAX_FRAME_BYTES));
            if (digest != null) {
                frame.asReadOnlyByteBufferList().forEach(digest::update);
            }
            pacing.send(ReadRenditionResponse.newBuilder().setData(frame).build());
        }
        return data.size();
    }

    private Iterator<ByteString> openBlobChunks(Plan plan) {
        if (plan.length() == 0) {
            return Collections.emptyIterator();
        }
        Iterator<ReadBlobResponse> frames = archive.documents().readBlob(ReadBlobRequest.newBuilder()
                .setAddress(plan.address())
                .setRevision(DocumentArchive.revision(plan.version()))
                .setBlobId(ArchiveCoordinates.blobId(
                        plan.row().getRendition().getName(), plan.row().getRendition().getSubKey()))
                .setOffset(plan.offset())
                .setLength(plan.length())
                .build());
        return new Iterator<>() {
            private ByteString next;

            @Override
            public boolean hasNext() {
                while (next == null && frames.hasNext()) {
                    ReadBlobResponse frame = frames.next();
                    if (frame.hasData() && !frame.getData().isEmpty()) {
                        next = frame.getData();
                    }
                }
                return next != null;
            }

            @Override
            public ByteString next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                ByteString current = next;
                next = null;
                return current;
            }
        };
    }

    private Iterator<ByteString> openParsedChunks(Plan plan) {
        if (plan.length() == 0) {
            return Collections.emptyIterator();
        }
        Iterator<ReadDocumentResponse> frames = archive.documents().readDocument(ReadDocumentRequest.newBuilder()
                .setAddress(plan.address())
                .setRevision(DocumentArchive.revision(plan.version()))
                .setSelection(PartSelection.newBuilder().addSelectors(ArchiveCoordinates.PARSED_PART))
                .build());
        long end = plan.offset() + plan.length();
        return new Iterator<>() {
            private long position;
            private ByteString next;

            @Override
            public boolean hasNext() {
                while (next == null && frames.hasNext()) {
                    ReadDocumentResponse frame = frames.next();
                    if (!frame.hasPartData()) {
                        continue;
                    }
                    ByteString data = frame.getPartData().getData();
                    long start = position;
                    position += data.size();
                    long from = Math.max(start, plan.offset());
                    long to = Math.min(position, end);
                    if (from < to) {
                        next = data.substring((int) (from - start), (int) (to - start));
                    }
                }
                return next != null;
            }

            @Override
            public ByteString next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                ByteString current = next;
                next = null;
                return current;
            }
        };
    }

    private static final class VerifyingChunkInputStream extends InputStream {
        private final Plan plan;
        private final Iterator<ByteString> chunks;
        private final MessageDigest digest;
        private ByteString current = ByteString.EMPTY;
        private int offset;
        private long delivered;
        private boolean verified;

        VerifyingChunkInputStream(Plan plan, Iterator<ByteString> chunks) {
            this.plan = plan;
            this.chunks = chunks;
            this.digest = plan.wholeObject() ? sha256() : null;
        }

        @Override
        public int read() throws IOException {
            byte[] single = new byte[1];
            int n = read(single, 0, 1);
            return n == -1 ? -1 : (single[0] & 0xFF);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            Objects.checkFromIndexSize(off, len, b.length);
            if (len == 0) {
                return 0;
            }
            while (offset >= current.size()) {
                if (!chunks.hasNext()) {
                    verifyComplete();
                    return -1;
                }
                current = chunks.next();
                offset = 0;
            }
            int count = Math.min(len, current.size() - offset);
            current.copyTo(b, offset, off, count);
            offset += count;
            delivered += count;
            if (delivered > plan.length()) {
                throw ArchiveErrors.dataLoss(
                        "Rendition " + plan.row().getRendition().getName() + " of version "
                                + plan.version() + " delivered " + delivered + " of " + plan.length() + " bytes",
                        null);
            }
            if (digest != null) {
                digest.update(b, off, count);
            }
            return count;
        }

        private void verifyComplete() {
            if (verified) {
                return;
            }
            verified = true;
            if (delivered != plan.length()) {
                throw ArchiveErrors.dataLoss(
                        "Rendition " + plan.row().getRendition().getName() + " of version "
                                + plan.version() + " delivered " + delivered + " of " + plan.length() + " bytes",
                        null);
            }
            if (digest != null) {
                String actual = HexFormat.of().formatHex(digest.digest());
                if (!actual.equals(plan.row().getSha256())) {
                    throw ArchiveErrors.dataLoss(
                            "Rendition " + plan.row().getRendition().getName() + " of version "
                                    + plan.version() + " hashes to " + actual + ", not its manifest's "
                                    + plan.row().getSha256(),
                            null);
                }
            }
        }
    }

    static String sha256Hex(ByteString data) {
        MessageDigest digest = sha256();
        data.asReadOnlyByteBufferList().forEach(digest::update);
        return HexFormat.of().formatHex(digest.digest());
    }

    static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    /**
     * Sends frames only while the client's transport is ready, so a slow reader
     * holds back the v2 read instead of filling memory. Cancellation ends the wait.
     */
    private static final class Pacing {
        private final StreamObserver<ReadRenditionResponse> responses;
        private final ServerCallStreamObserver<ReadRenditionResponse> server;
        private final Object lock = new Object();

        Pacing(StreamObserver<ReadRenditionResponse> responses) {
            this.responses = responses;
            this.server = responses instanceof ServerCallStreamObserver<ReadRenditionResponse> call ? call : null;
            if (server != null) {
                server.setOnReadyHandler(this::wake);
                server.setOnCancelHandler(this::wake);
            }
        }

        private void wake() {
            synchronized (lock) {
                lock.notifyAll();
            }
        }

        void send(ReadRenditionResponse frame) throws InterruptedException {
            if (server != null) {
                synchronized (lock) {
                    while (!server.isReady()) {
                        if (server.isCancelled() || Context.current().isCancelled()) {
                            throw Status.CANCELLED.withDescription("The reader cancelled the rendition read")
                                    .asRuntimeException();
                        }
                        lock.wait(1000);
                    }
                }
            }
            responses.onNext(frame);
        }
    }
}
