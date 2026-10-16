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
