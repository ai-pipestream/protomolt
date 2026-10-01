package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.container.blob.BlobStore;
import ai.protomolt.proto.repo.container.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import ai.protomolt.proto.repo.container.ledger.Tx;
import ai.protomolt.proto.repo.v1.DeleteBlobRequest;
import ai.protomolt.proto.repo.v1.DeleteBlobResponse;
import ai.protomolt.proto.repo.v1.CompareAndPutBlobRequest;
import ai.protomolt.proto.repo.v1.CompareAndPutBlobResponse;
import ai.protomolt.proto.repo.v1.ConditionalBlobKey;
import ai.protomolt.proto.repo.v1.ConditionalBlobVersion;
import ai.protomolt.proto.repo.v1.FileStorageReference;
import ai.protomolt.proto.repo.v1.GetBlobForUpdateRequest;
import ai.protomolt.proto.repo.v1.GetBlobForUpdateResponse;
import ai.protomolt.proto.repo.v1.GetBlobRequest;
import ai.protomolt.proto.repo.v1.GetBlobResponse;
import ai.protomolt.proto.repo.v1.PutBlobRequest;
import ai.protomolt.proto.repo.v1.PutBlobResponse;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import ai.protomolt.proto.validate.ProtoValidator;
import io.grpc.Status;
import java.util.List;
import java.util.Optional;

import static ai.protomolt.proto.repo.service.GrpcErrors.invalidArgument;
import static ai.protomolt.proto.repo.service.GrpcErrors.notFound;

/**
 * The loose-blob surface of {@code DocumentService}: bytes addressed by drive and key,
 * outside the claim-check part layout that the document rows use. These three RPCs share
 * no state with the document path beyond the object store itself, so they live apart from
 * it.
 */
final class BlobOperations {

    /** What a put lands as when the caller names no content type. */
    static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

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

    GetBlobForUpdateResponse getForUpdate(GetBlobForUpdateRequest request) {
        request(request);
        ConditionalBlobKey key = request.getKey();
        DriveRecord drive = driveOrThrow(key.getDriveName());
        BlobStore.GetResult got;
        try {
            got = blobStore.getForUpdate(drive.bucket, key.getObjectKey());
        } catch (UnsupportedOperationException unsupported) {
            throw Status.UNIMPLEMENTED.withDescription("authoritative blob read is unsupported")
                    .asRuntimeException();
        }
        if (got == null || got.data() == null || got.data().length > BlobStore.MAX_CONDITIONAL_BYTES) {
            throw Status.INTERNAL.withDescription("authoritative blob read is invalid").asRuntimeException();
        }
        String tag = backendTag(got.eTag());
        ConditionalBlobVersion version = version(key, tag, got.data());
        var response = GetBlobForUpdateResponse.newBuilder().setVersion(version)
                .setData(ByteString.copyFrom(got.data()));
        if (got.contentType() != null) response.setMimeType(got.contentType());
        GetBlobForUpdateResponse result = response.build();
        response(result);
        return result;
    }

    CompareAndPutBlobResponse compareAndPut(CompareAndPutBlobRequest request) {
        request(request);
        ConditionalBlobKey key = request.getKey();
        DriveRecord drive = driveOrThrow(key.getDriveName());
        byte[] data = request.getData().toByteArray();
        var condition = request.hasIfAbsent() ? BlobStore.WriteCondition.absent()
                : BlobStore.WriteCondition.matching(request.getExpectedEtag());
        String contentType = request.hasMimeType() ? request.getMimeType() : DEFAULT_CONTENT_TYPE;
        String digest = DocumentPartCodec.sha256Hex(data);
        BlobStore.PutResult stored;
        try {
            stored = blobStore.conditionalPut(new BlobStore.PutSpec(drive.bucket,
                    key.getObjectKey(), contentType, null, digest), data, condition);
        } catch (BlobStore.BlobConflictException conflict) {
            throw GrpcErrors.aborted("conditional blob precondition failed");
        } catch (UnsupportedOperationException unsupported) {
            throw Status.UNIMPLEMENTED.withDescription("conditional blob write is unsupported")
                    .asRuntimeException();
        }
        if (stored == null) {
            throw Status.INTERNAL.withDescription("conditional blob write is invalid").asRuntimeException();
        }
        var result = CompareAndPutBlobResponse.newBuilder()
                .setVersion(version(key, committedTag(stored.eTag()), data)).build();
        response(result);
        return result;
    }

    private static ConditionalBlobVersion version(ConditionalBlobKey key, String tag, byte[] data) {
        return ConditionalBlobVersion.newBuilder().setKey(key).setEtag(tag)
                .setSizeBytes(data.length).setSha256(DocumentPartCodec.sha256Hex(data)).build();
    }

    private static String backendTag(String tag) {
        try {
            return BlobStore.requireStrongEtag(tag);
        } catch (IllegalArgumentException incompatible) {
            throw Status.UNIMPLEMENTED.withDescription("backing ETag format is unsupported")
                    .asRuntimeException();
        }
    }

    private static String committedTag(String tag) {
        try {
            return BlobStore.requireStrongEtag(tag);
        } catch (IllegalArgumentException incompatible) {
            throw Status.INTERNAL.withDescription("conditional blob write returned an invalid ETag")
                    .asRuntimeException();
        }
    }

    private static void request(Message value) {
        if (!valid(value)) throw invalidArgument("invalid conditional blob request");
    }

    private static void response(Message value) {
        if (!valid(value)) {
            throw Status.INTERNAL.withDescription("invalid conditional blob response").asRuntimeException();
        }
    }

    private static boolean valid(Message value) {
        if (!value.getUnknownFields().asMap().isEmpty() || !VALIDATOR.validate(value).valid()) return false;
        for (var field : value.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() != FieldDescriptor.JavaType.MESSAGE) continue;
            if (field.getKey().isRepeated()) {
                for (Object nested : (List<?>) field.getValue()) {
                    if (!valid((Message) nested)) return false;
                }
            } else if (!valid((Message) field.getValue())) return false;
        }
        return true;
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
