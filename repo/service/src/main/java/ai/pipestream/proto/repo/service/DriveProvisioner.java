package ai.pipestream.proto.repo.service;

import ai.pipestream.proto.repo.container.ledger.DriveLedger;
import ai.pipestream.proto.repo.container.ledger.DriveRecord;
import ai.pipestream.proto.repo.v1.DriveProviderConfig;
import ai.pipestream.proto.repo.v1.DriveType;
import jakarta.persistence.PersistenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

/**
 * The reusable core of drive provisioning, shared by the wire path
 * ({@link DriveGrpcService}, which adds request validation and gRPC error
 * mapping) and the boot path ({@link RepoServices#seedAccountDrives()}, the
 * standalone default account's intake/pipeline drives).
 *
 * <p>Provisioning is idempotent by construction: the drive id is a
 * deterministic UUIDv5 over {@code "drive|<accountId>|<name>"}, so a
 * re-provision of the same (account, name) finds the existing row and returns
 * it instead of failing on the unique constraint — and losing a concurrent
 * create race means the winner's row IS the answer.
 *
 * <p>Bucket creation is the ONE admin-plane call made on the raw
 * {@link S3Client} — the {@code BlobStore} port is deliberately
 * object-operations-only, so bucket lifecycle lives here, at the provisioning
 * call site.
 */
final class DriveProvisioner {

    private static final Logger LOG = LoggerFactory.getLogger(DriveProvisioner.class);

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
    DriveProvisioner(DriveLedger drives, S3Client s3, String defaultBucketBase, String defaultRegion) {
        this.drives = drives;
        this.s3 = s3;
        this.defaultBucketBase = defaultBucketBase;
        this.defaultRegion = defaultRegion;
    }

    /**
     * Ensures the drive exists with every default applied (the seeder path:
     * no caller-supplied bucket, prefix, provider, region, credentials, or
     * metadata).
     *
     * @param accountId the owning account
     * @param name the account-scoped drive name
     * @param driveType the drive flavor
     * @return the existing or newly created row
     */
    DriveRecord ensureDrive(String accountId, String name, DriveType driveType) {
        return ensureDrive(accountId, name, driveType, null, null, null, null, null, null, null);
    }

    /**
     * Ensures the drive exists, creating it (and its bucket) when absent.
     * Blank bucket/prefix/provider/region/credentialsRef fall back to the
     * provisioning defaults.
     *
     * @param accountId the owning account
     * @param name the account-scoped drive name
     * @param driveType the drive flavor (UNSPECIFIED maps to CUSTOM)
     * @param bucket explicit bucket, or blank for
     *        {@code <base>-<accountId>-<name>} sanitized to S3 bucket rules
     * @param prefix explicit key prefix, or blank for the drive name
     * @param provider explicit provider, or blank for {@code s3}
     * @param region explicit region, or blank for the service default
     * @param credentialsRef explicit credentials reference, or blank for none
     * @param metadataJson metadata as JSON for the row's jsonb column, or null
     * @param providerConfig the provider config to persist, or null
     * @return the existing or newly created row
     */
    DriveRecord ensureDrive(String accountId, String name, DriveType driveType,
            String bucket, String prefix, String provider, String region,
            String credentialsRef, String metadataJson, DriveProviderConfig providerConfig) {
        UUID driveId = UUID.nameUUIDFromBytes(
                ("drive|" + accountId + "|" + name).getBytes(StandardCharsets.UTF_8));
        // Deterministic id ⇒ re-provision is idempotent: return the row that
        // is already there rather than erroring on the unique constraint.
        Optional<DriveRecord> existing = drives.findById(driveId);
        if (existing.isPresent()) {
            LOG.info("Found existing drive {}/{} (id={})", accountId, name, driveId);
            return existing.get();
        }

        String resolvedBucket = isBlank(bucket) ? sanitizeBucketName(
                defaultBucketBase + "-" + accountId + "-" + name) : bucket;
        String resolvedPrefix = isBlank(prefix) ? name : stripSlashes(prefix);
        ensureBucket(resolvedBucket);

        DriveRecord record = new DriveRecord();
        record.driveId = driveId;
        record.accountId = accountId;
        record.name = name;
        record.driveType = driveType(driveType);
        record.provider = isBlank(provider) ? DEFAULT_PROVIDER : provider;
        record.bucket = resolvedBucket;
        record.prefix = resolvedPrefix;
        record.region = isBlank(region) ? defaultRegion : region;
        record.credentialsRef = isBlank(credentialsRef) ? null : credentialsRef;
        record.status = STATUS_ACTIVE;
        record.metadata = metadataJson;
        if (providerConfig != null) {
            record.writeProviderConfig(providerConfig);
        }
        try {
            drives.insert(record);
        } catch (PersistenceException race) {
            // Lost a concurrent create race on (account_id, name): the
            // winner's row IS the idempotent answer.
            Optional<DriveRecord> winner = drives.findById(driveId);
            if (winner.isPresent()) {
                return winner.get();
            }
            throw race;
        }
        LOG.info("Created drive {}/{} (id={}, bucket={}, prefix='{}')",
                accountId, name, driveId, resolvedBucket, resolvedPrefix);
        return record;
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

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
