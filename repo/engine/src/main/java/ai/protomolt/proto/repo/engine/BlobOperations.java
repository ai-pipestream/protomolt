package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
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
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import static ai.protomolt.proto.repo.spi.RepositoryException.Code.*;
import java.util.List;
import java.util.Optional;


/**
 * The loose-blob surface of {@code DocumentService}: bytes addressed by drive and key,
 * outside the claim-check part layout that the document rows use. These three RPCs share
 * no state with the document path beyond the object store itself, so they live apart from
 * it.
 */
public final class BlobOperations implements ai.protomolt.proto.repo.spi.BlobRepository {

    /** What a put lands as when the caller names no content type. */
    static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private final BlobStore blobStore;
    private final ai.protomolt.proto.repo.container.ledger.DriveLedger drives;

    public BlobOperations(BlobStore blobStore, ai.protomolt.proto.repo.container.ledger.DriveLedger drives) {
        this.blobStore = blobStore;
        this.drives = drives;
    }

    @Override public GetBlobResponse get(RepositoryCaller caller, GetBlobRequest request) {
        requireAdministrator(caller);
        return RepositoryErrors.call(() -> {
            FileStorageReference ref = storageRef(request.hasStorageRef(), request.getStorageRef());
            DriveRecord drive = driveOrThrow(ref.getDriveName());
            BlobStore.GetResult got = missingAsRepositoryError(() -> blobStore.get(drive.bucket, ref.getObjectKey(),
                    ref.hasVersionId() && !ref.getVersionId().isBlank() ? ref.getVersionId() : null));
            GetBlobResponse.Builder response = GetBlobResponse.newBuilder()
                    .setData(ByteString.copyFrom(got.data()))
                    .setSizeBytes(got.data().length)
                    .setRetrievedAtEpochMs(System.currentTimeMillis());
            if (got.contentType() != null) {
                response.setMimeType(got.contentType());
            }
            return response.build();
        });
    }

    @Override public PutBlobResponse put(RepositoryCaller caller, PutBlobRequest request) {
        requireAdministrator(caller);
        return RepositoryErrors.call(() -> {
            if (request.getDriveName().isBlank()) {
                throw invalidArgument("drive_name is required");
            }
            requireUnmanaged(request.getObjectKey());
            DriveRecord drive = driveOrThrow(request.getDriveName());
            byte[] data = request.getData().toByteArray();
            String sha256 = DocumentPartCodec.sha256Hex(data);
            String objectKey = request.getObjectKey().isBlank()
                    ? DriveKeys.blob(drive.prefix, sha256)
                    : request.getObjectKey();
            requireUnmanaged(objectKey);
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
        });
    }

    @Override public GetBlobForUpdateResponse getForUpdate(RepositoryCaller caller, GetBlobForUpdateRequest request) {
        requireAdministrator(caller);
        return RepositoryErrors.call(() -> {
            request(request);
            ConditionalBlobKey key = request.getKey();
            DriveRecord drive = driveOrThrow(key.getDriveName());
            BlobStore.GetResult got;
            try {
                got = missingAsRepositoryError(() -> blobStore.getForUpdate(drive.bucket, key.getObjectKey()));
            } catch (UnsupportedOperationException unsupported) {
                throw new RepositoryException(UNSUPPORTED, "authoritative blob read is unsupported", unsupported);
            }
            if (got == null || got.data() == null || got.data().length > BlobStore.MAX_CONDITIONAL_BYTES) {
                throw new RepositoryException(INTERNAL, "authoritative blob read is invalid");
            }
            String tag = backendTag(got.eTag());
            ConditionalBlobVersion version = version(key, tag, got.data());
            var response = GetBlobForUpdateResponse.newBuilder().setVersion(version)
                    .setData(ByteString.copyFrom(got.data()));
            if (got.contentType() != null) response.setMimeType(got.contentType());
            GetBlobForUpdateResponse result = response.build();
            response(result);
            return result;
        });
    }

    @Override public CompareAndPutBlobResponse compareAndPut(RepositoryCaller caller, CompareAndPutBlobRequest request) {
        requireAdministrator(caller);
        return RepositoryErrors.call(() -> {
            request(request);
            ConditionalBlobKey key = request.getKey();
            requireUnmanaged(key.getObjectKey());
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
                throw new RepositoryException(CONFLICT, "conditional blob precondition failed", conflict);
            } catch (UnsupportedOperationException unsupported) {
                throw new RepositoryException(UNSUPPORTED, "conditional blob write is unsupported", unsupported);
            }
            if (stored == null) {
                throw new RepositoryException(INTERNAL, "conditional blob write is invalid");
            }
            var result = CompareAndPutBlobResponse.newBuilder()
                    .setVersion(version(key, committedTag(stored.eTag()), data)).build();
            response(result);
            return result;
        });
    }

    private static <T> T missingAsRepositoryError(java.util.function.Supplier<T> operation) {
        try { return operation.get(); }
        catch (BlobStore.BlobNotFoundException missing) {
            throw new RepositoryException(NOT_FOUND, missing.getMessage(), missing);
        }
    }

    private static void requireAdministrator(RepositoryCaller caller) {
        if (caller == null || !caller.processAuthority())
            throw new RepositoryException(PERMISSION_DENIED, "Raw blob operations require process authority");
    }

    private static RepositoryException invalidArgument(String message) {
        return new RepositoryException(INVALID_ARGUMENT, message);
    }

    private static RepositoryException notFound(String message) {
        return new RepositoryException(NOT_FOUND, message);
    }

    private static ConditionalBlobVersion version(ConditionalBlobKey key, String tag, byte[] data) {
        return ConditionalBlobVersion.newBuilder().setKey(key).setEtag(tag)
                .setSizeBytes(data.length).setSha256(DocumentPartCodec.sha256Hex(data)).build();
    }

    private static String backendTag(String tag) {
        try {
            return BlobStore.requireStrongEtag(tag);
        } catch (IllegalArgumentException incompatible) {
            throw new RepositoryException(UNSUPPORTED, "backing ETag format is unsupported", incompatible);
        }
    }

    private static String committedTag(String tag) {
        try {
            return BlobStore.requireStrongEtag(tag);
        } catch (IllegalArgumentException incompatible) {
            throw new RepositoryException(INTERNAL, "conditional blob write returned an invalid ETag", incompatible);
        }
    }

    private static void request(Message value) {
        if (!valid(value)) throw invalidArgument("invalid conditional blob request");
    }

    private static void response(Message value) {
        if (!valid(value)) {
            throw new RepositoryException(INTERNAL, "invalid conditional blob response");
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

    @Override public DeleteBlobResponse delete(RepositoryCaller caller, DeleteBlobRequest request) {
        requireAdministrator(caller);
        return RepositoryErrors.call(() -> {
            FileStorageReference ref = storageRef(request.hasStorageRef(), request.getStorageRef());
            requireUnmanaged(ref.getObjectKey());
            DriveRecord drive = driveOrThrow(ref.getDriveName());
            // Idempotent: delete-of-absent reports deleted=false, not an error.
            return DeleteBlobResponse.newBuilder()
                    .setDeleted(blobStore.delete(drive.bucket, ref.getObjectKey()))
                    .build();
        });
    }

    private static void requireUnmanaged(String objectKey) {
        if (DriveKeys.isManaged(objectKey) || DriveKeys.isArchiveOwned(objectKey))
            throw new RepositoryException(PERMISSION_DENIED,
                    "Reserved object mutations require the owning repository lifecycle");
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
     * account. Reject ambiguous names and apply the shared drive read gate.
     */
    private Optional<DriveRecord> findDriveByName(String name) {
        try { return drives.findUniqueByName(name); }
        catch (IllegalArgumentException ambiguous) {
            throw new RepositoryException(INVALID_ARGUMENT, ambiguous.getMessage(), ambiguous);
        }
    }
}
