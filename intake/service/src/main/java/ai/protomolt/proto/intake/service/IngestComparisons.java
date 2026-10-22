package ai.protomolt.proto.intake.service;

import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.ChecksumType;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.ReadBlobRequest;
import ai.protomolt.proto.repo.v2.RevisionSelector;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.function.UnaryOperator;

/** Typed comparison of selected parts, verifying raw bytes rather than trusting client checksum metadata. */
final class IngestComparisons {
    private IngestComparisons() {}

    static boolean same(DocumentServiceGrpc.DocumentServiceBlockingStub documents, NodeAddress address,
                        long revision, Document incoming, Document existing) {
        var left = normalize(incoming, blob -> normalized(blob,
                RawIngests.sha256Hex(blob.getData().toByteArray()), blob.getData().size()));
        var right = normalize(existing, blob -> retained(documents, address, revision, blob));
        return left.equals(right);
    }

    private static Blob retained(DocumentServiceGrpc.DocumentServiceBlockingStub documents,
                                 NodeAddress address, long revision, Blob blob) {
        // An invalid header/frame must cancel the RPC rather than leave its producer running.
        try (var context = io.grpc.Context.current().withCancellation()) {
            var previous = context.attach();
            try {
                return readRetained(documents, address, revision, blob);
            } finally {
                context.detach(previous);
            }
        }
    }

    private static Blob readRetained(DocumentServiceGrpc.DocumentServiceBlockingStub documents,
                                     NodeAddress address, long revision, Blob blob) {
        var frames = documents.readBlob(ReadBlobRequest.newBuilder().setAddress(address)
                .setRevision(RevisionSelector.newBuilder().setRevision(revision)).setBlobId(blob.getBlobId()).build());
        if (!frames.hasNext()) throw invalidStream();
        var first = frames.next();
        if (!first.hasHeader() || first.getHeader().getRevision() != revision) throw invalidStream();
        final MessageDigest digest;
        try { digest = MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException failure) { throw new IllegalStateException("SHA-256 unavailable", failure); }
        long length = 0;
        while (frames.hasNext()) {
            var frame = frames.next();
            if (!frame.hasData()) throw invalidStream();
            digest.update(frame.getData().asReadOnlyByteBuffer());
            length = Math.addExact(length, frame.getData().size());
        }
        var sha = HexFormat.of().formatHex(digest.digest());
        if (length != first.getHeader().getTotalLength() || !sha.equals(first.getHeader().getSha256()))
            throw invalidStream();
        return normalized(blob, sha, length);
    }

    private static Document normalize(Document document, UnaryOperator<Blob> transform) {
        if (!document.hasBlobBag()) return document;
        var bag = document.getBlobBag().toBuilder();
        switch (bag.getBlobDataCase()) {
            case BLOB -> bag.setBlob(transform.apply(bag.getBlob()));
            case BLOBS -> {
                var blobs = bag.getBlobs().toBuilder().clearBlob();
                for (var blob : bag.getBlobs().getBlobList()) blobs.addBlob(transform.apply(blob));
                bag.setBlobs(blobs);
            }
            case BLOBDATA_NOT_SET -> { }
        }
        return document.toBuilder().setBlobBag(bag).build();
    }

    private static Blob normalized(Blob blob, String sha, long length) {
        return blob.toBuilder().clearData().clearStorageRef().clearWritten().setChecksum(sha)
                .setChecksumType(ChecksumType.CHECKSUM_TYPE_SHA256).setSizeBytes(length).build();
    }

    private static RuntimeException invalidStream() {
        return io.grpc.Status.DATA_LOSS.withDescription("Invalid retained blob stream during intake comparison")
                .asRuntimeException();
    }
}
