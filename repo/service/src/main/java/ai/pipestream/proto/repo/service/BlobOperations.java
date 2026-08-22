package ai.pipestream.proto.repo.service;

import ai.pipestream.proto.repo.container.blob.BlobStore;
import ai.pipestream.proto.repo.container.codec.DocumentPartCodec;
import ai.pipestream.proto.repo.container.ledger.DriveRecord;
import ai.pipestream.proto.repo.container.ledger.Tx;
import ai.pipestream.proto.repo.v1.DeleteBlobRequest;
import ai.pipestream.proto.repo.v1.DeleteBlobResponse;
import ai.pipestream.proto.repo.v1.FileStorageReference;
import ai.pipestream.proto.repo.v1.GetBlobRequest;
import ai.pipestream.proto.repo.v1.GetBlobResponse;
import ai.pipestream.proto.repo.v1.PutBlobRequest;
import ai.pipestream.proto.repo.v1.PutBlobResponse;
import com.google.protobuf.ByteString;
import java.util.Optional;

import static ai.pipestream.proto.repo.service.GrpcErrors.invalidArgument;
import static ai.pipestream.proto.repo.service.GrpcErrors.notFound;

/**
 * The loose-blob surface of {@code DocumentService}: bytes addressed by drive and key,
 * outside the claim-check part layout that the document rows use. These three RPCs share
 * no state with the document path beyond the object store itself, so they live apart from
 * it.
 */
final class BlobOperations {

    /** What a put lands as when the caller names no content type. */
    static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";

    private final BlobStore blobStore;
    private final Tx tx;

    BlobOperations(BlobStore blobStore, Tx tx) {
        this.blobStore = blobStore;
        this.tx = tx;
    }

    GetBlobResponse get(GetBlobRequest request) {
        FileStorageReference ref = storageRef(request.hasStorageRef(), request.getStorageRef());
        DriveRecord drive = driveOrThrow(ref.getDriveName());
        BlobStore.GetResult got = blobStore.get(drive.bucket, ref.getObjectKey(),
                ref.hasVersionId() && !ref.getVersionId().isBlank() ? ref.getVersionId() : null);
        GetBlobResponse.Builder response = GetBlobResponse.newBuilder()
                .setData(ByteString.copyFrom(got.data()))
                .setSizeBytes(got.data().length)
                .setRetrievedAtEpochMs(System.currentTimeMillis());
        if (got.contentType() != null) {
            response.setMimeType(got.contentType());
        }
        return response.build();
    }

    PutBlobResponse put(PutBlobRequest request) {
        if (request.getDriveName().isBlank()) {
            throw invalidArgument("drive_name is required");
        }
        DriveRecord drive = driveOrThrow(request.getDriveName());
        byte[] data = request.getData().toByteArray();
        String sha256 = DocumentPartCodec.sha256Hex(data);
        String objectKey = request.getObjectKey().isBlank()
                ? DriveKeys.blob(drive, sha256)
                : request.getObjectKey();
        String contentType = request.getMimeType().isBlank()
                ? DEFAULT_CONTENT_TYPE : request.getMimeType();
        // Verified write: the store's checksum trailer makes it reject the PUT when the
        // landed bytes mismatch the computed digest.
        blobStore.put(new BlobStore.PutSpec(drive.bucket, objectKey, contentType, null, sha256),
                data);
        return PutBlobResponse.newBuilder()
                .setStorageRef(FileStorageReference.newBuilder()
                        .setDriveName(request.getDriveName())
                        .setObjectKey(objectKey))
                .setSizeBytes(data.length)
                .setSha256(sha256)
                .build();
    }

    DeleteBlobResponse delete(DeleteBlobRequest request) {
        FileStorageReference ref = storageRef(request.hasStorageRef(), request.getStorageRef());
        DriveRecord drive = driveOrThrow(ref.getDriveName());
        // Idempotent: delete-of-absent reports deleted=false, not an error.
        return DeleteBlobResponse.newBuilder()
                .setDeleted(blobStore.delete(drive.bucket, ref.getObjectKey()))
                .build();
    }

    /** A storage reference is a drive and a key; neither has a sensible default. */
    private static FileStorageReference storageRef(boolean present, FileStorageReference ref) {
        if (!present) {
            throw invalidArgument("storage_ref is required");
        }
        if (ref.getDriveName().isBlank()) {
            throw invalidArgument("storage_ref.drive_name is required");
        }
        if (ref.getObjectKey().isBlank()) {
            throw invalidArgument("storage_ref.object_key is required");
        }
        return ref;
    }

    private DriveRecord driveOrThrow(String name) {
        return findDriveByName(name)
                .orElseThrow(() -> notFound("drive '" + name + "' not found"));
    }

    /**
     * Drive lookup by bare name, across accounts. {@link FileStorageReference} carries no
     * account, and drive names are unique only per account: v1 trusts the caller's drive
     * reference and takes the first match. Tighten this if multi-account name reuse
     * becomes real.
     */
    private Optional<DriveRecord> findDriveByName(String name) {
        return tx.readOnly(em -> em.createQuery(
                        "SELECT d FROM DriveRecord d WHERE d.name = :name", DriveRecord.class)
                .setParameter("name", name)
                .setMaxResults(1)
                .getResultStream()
                .findFirst());
    }
}
