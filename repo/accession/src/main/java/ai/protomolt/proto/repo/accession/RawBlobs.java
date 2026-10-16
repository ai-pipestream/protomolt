package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.FileStorageReference;
import ai.protomolt.proto.repo.v2.BlobObjectRef;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.grpc.Status;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.function.UnaryOperator;

/**
 * Raw blob claim checks (requirements §6, decision 45). Each raw blob in a BLOBS
 * fragment is its own stack object; the stored fragment keeps each blob's
 * metadata without its data. The server alone binds a blob ID to its stack object
 * and fills {@code Blob.storage_ref} from that binding on read. Field numbers are
 * the compiled ones: the {@code Document} subject's BACKWARD_TRANSITIVE mode keeps
 * a registered field number from changing meaning (decision 34), and unknown
 * fields of newer versions pass through untouched.
 */
final class RawBlobs {
    private RawBlobs() {}

    /** A blob's raw bytes as the client sent them inline. */
    record Raw(String blobId, ByteString data) {}

    /** The fragment to store and the raw blobs it no longer carries, in fragment order. */
    record Extracted(ByteString stored, List<Raw> blobs) {}

    /** A stored blob's descriptive metadata for {@code ReadBlob} headers. */
    record Metadata(String mimeType, String filename) {}

    /**
     * Splits the client's BLOBS fragment. A blob needs a unique, non-blank ID and
     * inline data here; a client-supplied {@code storage_ref} is refused, because
     * clients cannot name physical storage. Refusals are malformed requests,
     * found before authorization and never recorded.
     */
    static Extracted extract(ByteString fragment) {
        var blobs = new ArrayList<Raw>();
        var ids = new HashSet<String>();
        var stored = rewrite(fragment, blob -> {
            if (blob.hasStorageRef())
                throw DocumentSaves.malformedException("Blob " + blob.getBlobId()
                        + " names a storage_ref; clients cannot supply storage references");
            if (blob.getContentCase() != Blob.ContentCase.DATA)
                throw DocumentSaves.malformedException("Blob " + blob.getBlobId()
                        + " has no inline data; a raw file is sent inline or uploaded");
            if (blob.getBlobId().isBlank() || !ids.add(blob.getBlobId()))
                throw DocumentSaves.malformedException("Every blob needs a unique, non-blank blob_id");
            blobs.add(new Raw(blob.getBlobId(), blob.getData()));
            return blob.toBuilder().clearData().build();
        });
        return new Extracted(stored, List.copyOf(blobs));
    }

    /**
     * Splits the client's BLOBS fragment for an {@code UploadDocument} raw-blob
     * stream. The uploaded blob must appear in {@code blob_bag} without inline
     * {@code data} or {@code storage_ref}; any sibling blobs in {@code blob_bag}
     * follow normal inline extraction rules.
     */
    static Extracted extractForUpload(ByteString fragment, String uploadedBlobId) {
        if (uploadedBlobId == null || uploadedBlobId.isBlank())
            throw DocumentSaves.malformedException("Uploaded blob_id must not be blank");
        var blobs = new ArrayList<Raw>();
        var ids = new HashSet<String>();
        boolean[] found = {false};
        var stored = rewrite(fragment, blob -> {
            if (blob.hasStorageRef())
                throw DocumentSaves.malformedException("Blob " + blob.getBlobId()
                        + " names a storage_ref; clients cannot supply storage references");
            if (blob.getBlobId().isBlank() || !ids.add(blob.getBlobId()))
                throw DocumentSaves.malformedException("Every blob needs a unique, non-blank blob_id");
            if (blob.getBlobId().equals(uploadedBlobId)) {
                if (blob.getContentCase() != Blob.ContentCase.CONTENT_NOT_SET)
                    throw DocumentSaves.malformedException("Uploaded blob " + blob.getBlobId()
                            + " must not carry inline data in the save header");
                found[0] = true;
                return blob;
            }
            if (blob.getContentCase() != Blob.ContentCase.DATA)
                throw DocumentSaves.malformedException("Blob " + blob.getBlobId()
                        + " has no inline data; a raw file is sent inline or uploaded");
            blobs.add(new Raw(blob.getBlobId(), blob.getData()));
            return blob.toBuilder().clearData().build();
        });
        if (!found[0])
            throw DocumentSaves.malformedException("Uploaded blob " + uploadedBlobId + " is not present in blob_bag");
        return new Extracted(stored, List.copyOf(blobs));
    }

    /**
     * Orders blob object references to match the blob order in the stored BLOBS fragment.
     */
    static List<BlobObjectRef> orderRefs(ByteString stored, List<BlobObjectRef> refs) {
        if (refs.size() <= 1) return List.copyOf(refs);
        var byId = new HashMap<String, BlobObjectRef>();
        for (var ref : refs) byId.put(ref.getBlobId(), ref);
        var ordered = new ArrayList<BlobObjectRef>(refs.size());
        rewrite(stored, blob -> {
            var ref = byId.get(blob.getBlobId());
            if (ref != null) ordered.add(ref);
            return blob;
        });
        return ordered.size() == refs.size() ? List.copyOf(ordered) : List.copyOf(refs);
    }

    /**
     * Constant-memory content identity for a BLOBS PUT whose raw file is streamed
     * through {@code UploadDocument}. Binds the stored BLOBS metadata fragment,
     * any inline sibling blobs, and the streamed blob's ID, SHA-256 and byte length.
     */
    static SaveIntents.Content uploadedContent(ByteString stored, List<Raw> inlineBlobs,
            String uploadedBlobId, String uploadedSha256, long uploadedLength) {
        var digest = SaveIntents.newSha256();
        digest.update("protomolt/upload-blobs/v1\0".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        digest.update(stored.asReadOnlyByteBuffer());
        long totalLength = stored.size() + Math.max(0L, uploadedLength);
        for (var raw : inlineBlobs) {
            digest.update((raw.blobId() + "\0").getBytes(java.nio.charset.StandardCharsets.UTF_8));
            digest.update(raw.data().asReadOnlyByteBuffer());
            totalLength += raw.data().size();
        }
        digest.update(("\0" + uploadedBlobId + "\0" + uploadedSha256.toLowerCase(java.util.Locale.ROOT)
                + "\0" + uploadedLength).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return new SaveIntents.Content( java.util.HexFormat.of().formatHex(digest.digest()), totalLength);
    }

    /**
     * Extracts the MIME type and filename of a stored blob from its BLOBS fragment.
     */
    static Metadata metadata(ByteString stored, String blobId) {
        Metadata[] found = {null};
        rewrite(stored, blob -> {
            if (blob.getBlobId().equals(blobId))
                found[0] = new Metadata(blob.getMimeType(), blob.getFilename());
            return blob;
        });
        if (found[0] == null)
            throw new AccessionException(Status.Code.DATA_LOSS, "Stored blob " + blobId + " is absent from BLOBS fragment", null);
        return found[0];
    }

    /**
     * The stored fragment with each blob's {@code storage_ref} filled from its
     * binding. A blob without a binding, or a binding without a blob, is data loss.
     */
    static ByteString withClaimChecks(ByteString stored, List<BlobObjectRef> refs) {
        var bound = new HashMap<String, BlobObjectRef>();
        for (var ref : refs) bound.put(ref.getBlobId(), ref);
        var used = new HashSet<String>();
        var filled = rewrite(stored, blob -> {
            var ref = bound.get(blob.getBlobId());
            if (ref == null || !used.add(blob.getBlobId()))
                throw new AccessionException(Status.Code.DATA_LOSS, "Stored blob has no claim check binding", null);
            return blob.toBuilder().setStorageRef(FileStorageReference.newBuilder()
                    .setDriveName(ref.getRef().getStackId()).setObjectKey(ref.getRef().getKey())).build();
        });
        if (used.size() != bound.size())
            throw new AccessionException(Status.Code.DATA_LOSS, "A claim check binding names no stored blob", null);
        return filled;
    }

    private static ByteString rewrite(ByteString fragment, UnaryOperator<Blob> change) {
        final Document document;
        try {
            document = Document.parseFrom(fragment);
        } catch (InvalidProtocolBufferException corrupt) {
            throw new AccessionException(Status.Code.DATA_LOSS, "BLOBS fragment does not decode", null, corrupt);
        }
        if (!document.hasBlobBag()) return fragment;
        var bag = document.getBlobBag();
        var rewritten = switch (bag.getBlobDataCase()) {
            case BLOB -> bag.toBuilder().setBlob(change.apply(bag.getBlob())).build();
            case BLOBS -> {
                var list = bag.getBlobs().toBuilder().clearBlob();
                for (var blob : bag.getBlobs().getBlobList()) list.addBlob(change.apply(blob));
                yield bag.toBuilder().setBlobs(list).build();
            }
            case BLOBDATA_NOT_SET -> bag;
        };
        return deterministic(document.toBuilder().setBlobBag(rewritten).build());
    }

    /** Map entries sorted by key; field presence and unknown fields unchanged. */
    static ByteString deterministic(Message message) {
        var out = ByteString.newOutput(message.getSerializedSize());
        var coded = CodedOutputStream.newInstance(out);
        coded.useDeterministicSerialization();
        try {
            message.writeTo(coded);
            coded.flush();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory encoding failed", impossible);
        }
        return out.toByteString();
    }
}
