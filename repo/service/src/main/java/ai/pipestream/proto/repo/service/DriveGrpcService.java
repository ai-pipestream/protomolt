package ai.pipestream.proto.repo.service;

import ai.pipestream.proto.repo.v1.CreateDriveRequest;
import ai.pipestream.proto.repo.v1.CreateDriveResponse;
import ai.pipestream.proto.repo.v1.Drive;
import ai.pipestream.proto.repo.v1.DriveProviderConfig;
import ai.pipestream.proto.repo.v1.DriveServiceGrpc;
import ai.pipestream.proto.repo.v1.DriveStatus;
import ai.pipestream.proto.repo.v1.DriveType;
import ai.pipestream.proto.repo.v1.GetDriveRequest;
import ai.pipestream.proto.repo.v1.GetDriveResponse;
import ai.pipestream.proto.repo.v1.ListDrivesRequest;
import ai.pipestream.proto.repo.v1.ListDrivesResponse;
import ai.pipestream.proto.repo.container.ledger.DriveLedger;
import ai.pipestream.proto.repo.container.ledger.DriveRecord;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import io.grpc.stub.StreamObserver;
import jakarta.persistence.PersistenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static ai.pipestream.proto.repo.service.GrpcErrors.invalidArgument;
import static ai.pipestream.proto.repo.service.GrpcErrors.notFound;

/**
 * Drive lifecycle: per-account storage namespaces over object-storage
 * locations. A drive binds (bucket, prefix, region, credentialsRef) and is the
 * only thing that resolves to storage coordinates; document rows reference
 * their drive by bare (account-scoped) name.
 *
 * <p>{@code CreateDrive} is idempotent by construction: the drive id is a
 * deterministic UUIDv5 over {@code "drive|<accountId>|<name>"}, so a re-create
 * of the same (account, name) finds the existing row and returns it instead of
 * failing on the unique constraint.
 *
 * <p>Bucket creation is the ONE admin-plane call this service makes on the
 * raw {@link S3Client} — the {@code BlobStore} port is deliberately
 * object-operations-only, so bucket lifecycle lives here, at the provisioning
 * call site. Everything else (every document byte) goes through the port.
 */
public final class DriveGrpcService extends DriveServiceGrpc.DriveServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(DriveGrpcService.class);

    static final String DEFAULT_PROVIDER = "s3";
    static final String STATUS_ACTIVE = "ACTIVE";

    private final DriveLedger drives;
    private final S3Client s3;
    private final String defaultBucketBase;
    private final String defaultRegion;

    /**
     * @param drives the drive-row ledger
     * @param s3 the raw S3 client — used ONLY for bucket lifecycle
     *        (create/verify) during provisioning, never for object IO
     * @param defaultBucketBase bucket-name base for drives created without an
     *        explicit bucket: {@code <base>-<accountId>-<name>}
     * @param defaultRegion region stamped on drives that don't name one
     */
    public DriveGrpcService(DriveLedger drives, S3Client s3, String defaultBucketBase, String defaultRegion) {
        this.drives = drives;
        this.s3 = s3;
        this.defaultBucketBase = defaultBucketBase;
        this.defaultRegion = defaultRegion;
    }

    @Override
    public void createDrive(CreateDriveRequest request, StreamObserver<CreateDriveResponse> observer) {
        GrpcErrors.run(observer, () -> {
            if (request.getName().isBlank()) {
                throw invalidArgument("name is required");
            }
            if (request.getAccountId().isBlank()) {
                throw invalidArgument("account_id is required");
            }
            UUID driveId = UUID.nameUUIDFromBytes(
                    ("drive|" + request.getAccountId() + "|" + request.getName())
                            .getBytes(StandardCharsets.UTF_8));
            // Deterministic id ⇒ re-create is idempotent: return the row that
            // is already there rather than erroring on the unique constraint.
            Optional<DriveRecord> existing = drives.findById(driveId);
            if (existing.isPresent()) {
                return CreateDriveResponse.newBuilder().setDrive(toProto(existing.get())).build();
            }

            String bucket = !request.getBucket().isBlank() ? request.getBucket()
                    : sanitizeBucketName(defaultBucketBase + "-" + request.getAccountId()
                            + "-" + request.getName());
            String prefix = !request.getPrefix().isBlank() ? stripSlashes(request.getPrefix())
                    : request.getName();
            ensureBucket(bucket);

            DriveRecord record = new DriveRecord();
            record.driveId = driveId;
            record.accountId = request.getAccountId();
            record.name = request.getName();
            record.driveType = driveType(request.getDriveType());
            record.provider = request.getProvider().isBlank() ? DEFAULT_PROVIDER : request.getProvider();
            record.bucket = bucket;
            record.prefix = prefix;
            record.region = request.getRegion().isBlank() ? defaultRegion : request.getRegion();
            record.credentialsRef = request.getCredentialsRef().isBlank()
                    ? null : request.getCredentialsRef();
            record.status = STATUS_ACTIVE;
            record.metadata = metadataJson(request.getMetadataMap());
            if (request.hasProviderConfig()) {
                record.writeProviderConfig(request.getProviderConfig());
            }
            try {
                drives.insert(record);
            } catch (PersistenceException race) {
                // Lost a concurrent create race on (account_id, name): the
                // winner's row IS the idempotent answer.
                Optional<DriveRecord> winner = drives.findById(driveId);
                if (winner.isPresent()) {
                    return CreateDriveResponse.newBuilder().setDrive(toProto(winner.get())).build();
                }
                throw race;
            }
            LOG.info("Created drive {}/{} (id={}, bucket={}, prefix='{}')",
                    record.accountId, record.name, driveId, bucket, prefix);
            return CreateDriveResponse.newBuilder().setDrive(toProto(record)).build();
        });
    }

    @Override
    public void getDrive(GetDriveRequest request, StreamObserver<GetDriveResponse> observer) {
        GrpcErrors.run(observer, () -> {
            DriveRecord record = switch (request.getCoordinateCase()) {
                case DRIVE_ID -> {
                    UUID driveId = parseUuid(request.getDriveId());
                    yield drives.findById(driveId)
                            .orElseThrow(() -> notFound("no drive for drive_id " + driveId));
                }
                case NAME -> {
                    if (request.getAccountId().isBlank()) {
                        throw invalidArgument("account_id is required with the name coordinate");
                    }
                    yield drives.findByName(request.getAccountId(), request.getName())
                            .orElseThrow(() -> notFound("no drive '" + request.getName()
                                    + "' for account '" + request.getAccountId() + "'"));
                }
                default -> throw invalidArgument(
                        "exactly one coordinate (drive_id or name) must be set");
            };
            return GetDriveResponse.newBuilder().setDrive(toProto(record)).build();
        });
    }

    @Override
    public void listDrives(ListDrivesRequest request, StreamObserver<ListDrivesResponse> observer) {
        GrpcErrors.run(observer, () -> {
            if (request.getAccountId().isBlank()) {
                throw invalidArgument("account_id is required");
            }
            int limit = request.getLimit() <= 0 ? 100 : request.getLimit();
            // Keyset continuation: the token is the LAST name of the previous
            // page (names are unique per account), so pages are stable under
            // concurrent inserts.
            List<DriveRecord> page = drives.listByAccount(request.getAccountId(), limit,
                    request.getContinuationToken().isBlank() ? null : request.getContinuationToken());
            ListDrivesResponse.Builder response = ListDrivesResponse.newBuilder();
            for (DriveRecord record : page) {
                response.addDrives(toProto(record));
            }
            if (page.size() == limit && !page.isEmpty()) {
                response.setNextContinuationToken(page.get(page.size() - 1).name);
            }
            return response.build();
        });
    }

    /**
     * Create the drive's bucket when absent, then verify reachability. The one
     * admin-plane call site allowed on the raw client (see class Javadoc).
     */
    private void ensureBucket(String bucket) {
        try {
            s3.headBucket(b -> b.bucket(bucket));
            return;
        } catch (S3Exception e) {
            if (e.statusCode() != 404) {
                throw e;
            }
        }
        s3.createBucket(b -> b.bucket(bucket));
        s3.headBucket(b -> b.bucket(bucket));
    }

    /** Maps the wire enum to the row's check-constrained string; UNSPECIFIED → CUSTOM. */
    private static String driveType(DriveType type) {
        return switch (type) {
            case DRIVE_TYPE_INTAKE -> "INTAKE";
            case DRIVE_TYPE_PIPELINE -> "PIPELINE";
            default -> "CUSTOM";
        };
    }

    private static Drive toProto(DriveRecord record) {
        Drive.Builder drive = Drive.newBuilder()
                .setDriveId(record.driveId.toString())
                .setName(record.name)
                .setAccountId(record.accountId)
                .setDriveType(DriveType.valueOf("DRIVE_TYPE_" + record.driveType))
                .setProvider(record.provider)
                .setBucket(record.bucket)
                .setPrefix(record.prefix)
                .putAllMetadata(metadataMap(record.metadata))
                .setCreatedAt(toTimestamp(record.createdAt));
        if (record.region != null) {
            drive.setRegion(record.region);
        }
        if (record.credentialsRef != null) {
            drive.setCredentialsRef(record.credentialsRef);
        }
        try {
            drive.setStatus(DriveStatus.valueOf("DRIVE_STATUS_" + record.status));
        } catch (IllegalArgumentException unknown) {
            drive.setStatus(DriveStatus.DRIVE_STATUS_UNSPECIFIED);
        }
        DriveProviderConfig providerConfig = record.readProviderConfig();
        if (providerConfig != null) {
            drive.setProviderConfig(providerConfig);
        }
        return drive.build();
    }

    /**
     * S3 bucket-name rules: lowercase letters/digits/dots/dashes, 3–63 chars.
     * Anything else collapses to a dash; edges are trimmed.
     */
    static String sanitizeBucketName(String raw) {
        String s = raw.toLowerCase(java.util.Locale.ROOT)
                .replaceAll("[^a-z0-9.-]", "-")
                .replaceAll("[-.]{2,}", "-")
                .replaceAll("^[-.]+", "")
                .replaceAll("[-.]+$", "");
        if (s.length() > 63) {
            s = s.substring(0, 63).replaceAll("[-.]+$", "");
        }
        if (s.length() < 3) {
            s = (s + "-bucket").replaceAll("^[-.]+", "");
        }
        return s;
    }

    private static String stripSlashes(String prefix) {
        String s = prefix;
        while (s.startsWith("/")) {
            s = s.substring(1);
        }
        while (s.endsWith("/")) {
            s = s.substring(0, s.length() - 1);
        }
        return s;
    }

    /** Metadata map → JSON for the row's jsonb column (null when empty). */
    private static String metadataJson(Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return null;
        }
        Struct.Builder struct = Struct.newBuilder();
        metadata.forEach((k, v) -> struct.putFields(k, Value.newBuilder().setStringValue(v).build()));
        try {
            return JsonFormat.printer().print(struct.build());
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("unprintable drive metadata", e);
        }
    }

    /** The row's metadata jsonb → string map (empty when unset). */
    private static Map<String, String> metadataMap(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        Struct.Builder struct = Struct.newBuilder();
        try {
            JsonFormat.parser().ignoringUnknownFields().merge(json, struct);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("unparseable drive metadata: " + json, e);
        }
        Map<String, String> out = new LinkedHashMap<>();
        struct.getFieldsMap().forEach((k, v) -> out.put(k, v.getStringValue()));
        return out;
    }

    private static UUID parseUuid(String raw) {
        try {
            return UUID.fromString(raw.trim());
        } catch (RuntimeException e) {
            throw invalidArgument("drive_id must be a UUID (got \"" + raw + "\")");
        }
    }

    private static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}
