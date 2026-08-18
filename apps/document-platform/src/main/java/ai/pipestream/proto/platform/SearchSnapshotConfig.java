package ai.pipestream.proto.platform;

import java.util.Map;

/**
 * S3 snapshot configuration for the search role's index, read from the
 * {@code DOCUMENT_PLATFORM_SEARCH_SNAPSHOT_S3_*} family. The bucket is the
 * switch: absent means snapshots are off and every other variable of the
 * family must be absent too; present demands the region by name. Static
 * credentials come as a pair or not at all (absent falls to the AWS
 * default provider chain), and an endpoint override (LocalStack, RustFS)
 * implies path-style addressing.
 *
 * @param bucket the snapshot bucket
 * @param prefix the key prefix inside the bucket
 * @param endpoint an endpoint override, or empty for AWS
 * @param region the bucket's region
 * @param accessKey a static access key, or empty for the provider chain
 * @param secretKey the static secret, or empty for the provider chain
 */
record SearchSnapshotConfig(
        String bucket, String prefix, String endpoint, String region,
        String accessKey, String secretKey) {

    /** Env var naming the snapshot bucket; absent turns snapshots off. */
    static final String ENV_BUCKET = "DOCUMENT_PLATFORM_SEARCH_SNAPSHOT_S3_BUCKET";

    /** Env var for the key prefix (default {@value DEFAULT_PREFIX}). */
    static final String ENV_PREFIX = "DOCUMENT_PLATFORM_SEARCH_SNAPSHOT_S3_PREFIX";

    /** Env var for an endpoint override (LocalStack, RustFS), optional. */
    static final String ENV_ENDPOINT = "DOCUMENT_PLATFORM_SEARCH_SNAPSHOT_S3_ENDPOINT";

    /** Env var for the bucket's region; required with the bucket. */
    static final String ENV_REGION = "DOCUMENT_PLATFORM_SEARCH_SNAPSHOT_S3_REGION";

    /** Env var for a static access key, optional as a pair with the secret. */
    static final String ENV_ACCESS_KEY = "DOCUMENT_PLATFORM_SEARCH_SNAPSHOT_S3_ACCESS_KEY";

    /** Env var for the static secret, optional as a pair with the key. */
    static final String ENV_SECRET_KEY = "DOCUMENT_PLATFORM_SEARCH_SNAPSHOT_S3_SECRET_KEY";

    /** The default key prefix. */
    static final String DEFAULT_PREFIX = "search-snapshots";

    SearchSnapshotConfig {
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalArgumentException("bucket must not be blank");
        }
        if (region == null || region.isBlank()) {
            throw new IllegalArgumentException("region must not be blank");
        }
        prefix = prefix == null ? "" : prefix;
        endpoint = endpoint == null ? "" : endpoint;
        accessKey = accessKey == null ? "" : accessKey;
        secretKey = secretKey == null ? "" : secretKey;
        if (accessKey.isEmpty() != secretKey.isEmpty()) {
            throw new IllegalArgumentException(ENV_ACCESS_KEY + " and " + ENV_SECRET_KEY
                    + " come together: set both or neither");
        }
    }

    /**
     * Parses the family from an environment map, or returns {@code null}
     * when snapshots are off. A family member set without the bucket is a
     * refusal, never a silent no-op.
     */
    static SearchSnapshotConfig fromEnvironment(Map<String, String> environment) {
        String bucket = value(environment, ENV_BUCKET);
        if (bucket.isEmpty()) {
            for (String name : new String[] {
                    ENV_PREFIX, ENV_ENDPOINT, ENV_REGION, ENV_ACCESS_KEY, ENV_SECRET_KEY}) {
                if (!value(environment, name).isEmpty()) {
                    throw new IllegalArgumentException(name + " is set but " + ENV_BUCKET
                            + " is not: name the snapshot bucket or unset the family");
                }
            }
            return null;
        }
        String region = value(environment, ENV_REGION);
        if (region.isEmpty()) {
            throw new IllegalArgumentException(
                    ENV_REGION + " is required: " + ENV_BUCKET + " names a bucket");
        }
        String prefix = value(environment, ENV_PREFIX);
        return new SearchSnapshotConfig(
                bucket,
                prefix.isEmpty() ? DEFAULT_PREFIX : prefix,
                value(environment, ENV_ENDPOINT),
                region,
                value(environment, ENV_ACCESS_KEY),
                value(environment, ENV_SECRET_KEY));
    }

    private static String value(Map<String, String> environment, String name) {
        String value = environment.get(name);
        return value == null ? "" : value.trim();
    }
}
