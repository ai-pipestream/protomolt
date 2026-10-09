package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.container.ledger.DriveLedger;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import ai.protomolt.proto.repo.v1.DriveProviderConfig;
import ai.protomolt.proto.repo.v1.DriveType;
import jakarta.persistence.PersistenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ai.protomolt.proto.repo.blob.spi.NamespaceProvisioner;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

/** Drive provisioning using the selected provider's namespace port and compatibility gate. */
public final class DriveProvisioner {

    private static final Logger LOG = LoggerFactory.getLogger(DriveProvisioner.class);

    static final String DEFAULT_PROVIDER = "s3";
    static final String STATUS_ACTIVE = "ACTIVE";

    private final DriveLedger drives;
    private final NamespaceProvisioner namespaces;
    private final String defaultProvider;
    private final java.util.function.Consumer<DriveRecord> recordGate;
    private final String defaultBucketBase;
    private final String defaultRegion;

    public DriveProvisioner(DriveLedger drives, NamespaceProvisioner namespaces, String defaultBucketBase,
            String defaultRegion, String defaultProvider) {
        this(drives, namespaces, defaultBucketBase, defaultRegion, defaultProvider, record -> {});
    }

    public DriveProvisioner(DriveLedger drives, NamespaceProvisioner namespaces, String defaultBucketBase,
            String defaultRegion, String defaultProvider, java.util.function.Consumer<DriveRecord> recordGate) {
        this.drives = drives;
        this.namespaces = java.util.Objects.requireNonNull(namespaces);
        this.defaultProvider = java.util.Objects.requireNonNull(defaultProvider);
        this.recordGate = java.util.Objects.requireNonNull(recordGate);
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
    public DriveRecord ensureDrive(String accountId, String name, DriveType driveType) {
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
    public DriveRecord ensureDrive(String accountId, String name, DriveType driveType,
            String bucket, String prefix, String provider, String region,
            String credentialsRef, String metadataJson, DriveProviderConfig providerConfig) {
        requireSelectedProvider(isBlank(provider) ? defaultProvider : provider, providerConfig);
        UUID driveId = UUID.nameUUIDFromBytes(
                ("drive|" + accountId + "|" + name).getBytes(StandardCharsets.UTF_8));
        String resolvedBucket = isBlank(bucket) ? sanitizeBucketName(
                defaultBucketBase + "-" + accountId + "-" + name) : bucket;
        String resolvedPrefix = isBlank(prefix) ? name : stripSlashes(prefix);

        DriveRecord record = new DriveRecord();
        record.driveId = driveId;
        record.accountId = accountId;
        record.name = name;
        record.driveType = driveType(driveType);
        record.provider = isBlank(provider) ? defaultProvider : provider;
        record.bucket = resolvedBucket;
        record.prefix = resolvedPrefix;
        record.region = isBlank(region) ? defaultRegion : region;
        record.credentialsRef = isBlank(credentialsRef) ? null : credentialsRef;
        record.status = STATUS_ACTIVE;
        record.metadata = metadataJson;
        if (providerConfig != null) {
            record.writeProviderConfig(providerConfig);
        }
        recordGate.accept(record);
        // Deterministic id ⇒ re-provision is idempotent: return the row that
        // is already there rather than erroring on the unique constraint.
        Optional<DriveRecord> existing = drives.findById(driveId);
        if (existing.isPresent()) {
            requireSelectedProvider(existing.get().provider, existing.get().readProviderConfig());
            LOG.info("Found existing drive {}/{} (id={})", accountId, name, driveId);
            return existing.get();
        }

        ensureBucket(resolvedBucket);
        try {
            drives.insert(record);
        } catch (PersistenceException race) {
            // Lost a concurrent create race on (account_id, name): the
            // winner's row IS the idempotent answer.
            Optional<DriveRecord> winner = drives.findById(driveId);
            if (winner.isPresent()) {
                requireSelectedProvider(winner.get().provider, winner.get().readProviderConfig());
                return winner.get();
            }
            throw race;
        }
        LOG.info("Created drive {}/{} (id={}, bucket={}, prefix='{}')",
                accountId, name, driveId, resolvedBucket, resolvedPrefix);
        // Return persisted values, including database timestamp precision, so
        // the first response agrees with later lookup and idempotent retries.
        return drives.findById(driveId).orElseThrow(() ->
                new IllegalStateException("Created drive is missing from the ledger: " + driveId));
    }

    private void requireSelectedProvider(String provider, DriveProviderConfig config) {
        requireSelectedProvider(defaultProvider, provider, config);
    }

    public static void requireSelectedProvider(String defaultProvider, String provider, DriveProviderConfig config) {
        if (!defaultProvider.equals(provider)) {
            throw new ai.protomolt.proto.repo.spi.RepositoryException(ai.protomolt.proto.repo.spi.RepositoryException.Code.FAILED_PRECONDITION, "Drive provider does not match the selected storage backend");
        }
        if (config == null) return;
        boolean mismatch = switch (config.getConfigCase()) {
            case S3 -> !"s3".equals(defaultProvider);
            case REDIS -> !"redis".equals(defaultProvider);
            case REMOTE -> !"repo".equals(defaultProvider) && !"repo-inprocess".equals(defaultProvider);
            case CONFIG_NOT_SET -> false;
        };
        if (mismatch) throw new ai.protomolt.proto.repo.spi.RepositoryException(ai.protomolt.proto.repo.spi.RepositoryException.Code.FAILED_PRECONDITION, "Drive configuration does not match the selected storage backend");
    }

    /**
     * Create the drive's bucket when absent, then verify reachability. The one
     * admin-plane operation is delegated to the selected namespace provider.
     */
    private void ensureBucket(String bucket) {
        namespaces.ensureNamespace(bucket);
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
