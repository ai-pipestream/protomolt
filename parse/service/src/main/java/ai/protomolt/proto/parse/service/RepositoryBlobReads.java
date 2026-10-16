package ai.protomolt.proto.parse.service;

import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.ReadBlobRequest;
import ai.protomolt.proto.repo.v2.RevisionSelector;
import io.grpc.Context;
import io.grpc.Status;
import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** Revision-bound repository stream reader for the parser client's existing byte-array input. */
final class RepositoryBlobReads {
    private RepositoryBlobReads() {}

    static byte[] read(DocumentServiceGrpc.DocumentServiceBlockingStub documents,
            NodeAddress address, long revision, Blob blob) {
        // Closing the child context also cancels an unfinished stream after a local refusal.
        try (Context.CancellableContext context = Context.current().withCancellation()) {
            return context.call(() -> readStream(documents, address, revision, blob));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw Status.INTERNAL.withDescription("Repository blob read failed").withCause(e)
                    .asRuntimeException();
        }
    }

    private static byte[] readStream(DocumentServiceGrpc.DocumentServiceBlockingStub documents,
            NodeAddress address, long revision, Blob blob) {
        var frames = documents.readBlob(ReadBlobRequest.newBuilder().setAddress(address)
                .setRevision(RevisionSelector.newBuilder().setRevision(revision))
                .setBlobId(blob.getBlobId()).build());
        if (!frames.hasNext()) throw damaged("Missing blob header");
        var first = frames.next();
        if (!first.hasHeader()) throw damaged("Blob data preceded its header");
        var header = first.getHeader();
        if (header.getRevision() != revision || header.getTotalLength() < 0
                || (blob.getSizeBytes() > 0 && header.getTotalLength() != blob.getSizeBytes())
                || !header.getSha256().matches("[0-9a-f]{64}"))
            throw damaged("Blob header does not match the retained revision");
        if (header.getTotalLength() > Integer.MAX_VALUE)
            throw Status.RESOURCE_EXHAUSTED.withDescription(
                    "The parser client's byte-array input cannot represent this blob; a streaming parser is required")
                    .asRuntimeException();
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
        var bytes = new ByteArrayOutputStream();
        long received = 0;
        while (frames.hasNext()) {
            var frame = frames.next();
            if (!frame.hasData()) throw damaged("Unexpected blob frame after header");
            received += frame.getData().size();
            if (received > header.getTotalLength()) throw damaged("Blob exceeds declared length");
            for (var buffer : frame.getData().asReadOnlyByteBufferList()) digest.update(buffer);
            bytes.writeBytes(frame.getData().toByteArray());
        }
        if (received != header.getTotalLength()
                || !HexFormat.of().formatHex(digest.digest()).equals(header.getSha256()))
            throw damaged("Blob length or SHA-256 mismatch");
        return bytes.toByteArray();
    }

    private static io.grpc.StatusRuntimeException damaged(String message) {
        return Status.DATA_LOSS.withDescription(message).asRuntimeException();
    }
}
