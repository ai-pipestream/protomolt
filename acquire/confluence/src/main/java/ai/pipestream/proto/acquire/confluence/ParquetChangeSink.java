package ai.pipestream.proto.acquire.confluence;

import ai.pipestream.proto.acquire.confluence.v1.ConfluenceChange;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceEntity;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceSnapshot;
import ai.pipestream.proto.emit.parquet.s3.S3Clients;
import ai.pipestream.proto.emit.parquet.s3.S3ParquetSink;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Buffers the crawler's {@link ConfluenceChange} records per entity kind and flushes
 * them as Parquet part files to an S3-compatible store (RustFS, SeaweedFS, Ceph, AWS
 * S3) through {@link S3ParquetSink}. Every part file holds rows of one message type,
 * {@link ConfluenceChange}, so the files stay schema-stable no matter which entity arm
 * a change carries.
 *
 * <p>Key layout: {@code <prefix>/<entityType>/<runId>-part-<NNNNN>.parquet}, where
 * {@code entityType} is the {@link ConfluenceEntity} oneof arm in lower snake case
 * ({@code page}, {@code blog_post}, ...; {@code changes} when no entity arm is set),
 * {@code runId} is a per-sink-instance id (UTC timestamp plus a random suffix, so
 * reruns never overwrite a previous run's parts), and the part number is a per
 * entity-type sequence.</p>
 *
 * <p>Delivery: a buffer flushes when it reaches the batch size, on
 * {@link #snapshot(ConfluenceSnapshot)} (a completed space sweep bounds the part files
 * to crawl runs; the snapshot marker itself is crawl bookkeeping and is not exported),
 * and on {@link #close()}. A crash before the flush loses the buffer - the gap is
 * filled by re-running the crawl, which the deterministic change cursors make safe, or
 * by at-least-once redelivery when this sink sits behind the Kafka feed. A failed
 * upload keeps the batch buffered and is retried at the next flush point; the failure
 * is logged, never thrown into the crawler loop, matching {@link KafkaChangeSink}.</p>
 *
 * <p>Configuration is env-driven like the other sinks, but self-contained (the S3
 * target has nothing to do with the Confluence connection, so it stays off
 * {@link ConfluenceConnectorConfig}):</p>
 * <ul>
 *   <li>{@code CONFLUENCE_PARQUET_S3_BUCKET}: activates the sink</li>
 *   <li>{@code CONFLUENCE_PARQUET_S3_ENDPOINT}: path-style endpoint override for a
 *   self-hosted store (e.g. {@code http://localhost:9000} for RustFS); unset means AWS
 *   S3 in {@code CONFLUENCE_PARQUET_S3_REGION}</li>
 *   <li>{@code CONFLUENCE_PARQUET_S3_REGION}: default {@code us-east-1}</li>
 *   <li>{@code CONFLUENCE_PARQUET_S3_ACCESS_KEY_ID} /
 *   {@code CONFLUENCE_PARQUET_S3_SECRET_ACCESS_KEY}: static credentials; when unset the
 *   AWS SDK default provider chain (env, profile, IMDS, IRSA) supplies them</li>
 *   <li>{@code CONFLUENCE_PARQUET_S3_PREFIX}: key prefix, default
 *   {@code confluence-changes}</li>
 *   <li>{@code CONFLUENCE_PARQUET_S3_BATCH_SIZE}: rows per part file, default 500</li>
 * </ul>
 *
 * <p>Thread-safe: the crawler emits from virtual threads concurrently; every method is
 * synchronized.</p>
 */
public final class ParquetChangeSink implements ChangeSink, AutoCloseable {

    /** Environment variable for the target bucket; the sink activates when it is set. */
    public static final String ENV_BUCKET = "CONFLUENCE_PARQUET_S3_BUCKET";
    /** Environment variable for the path-style endpoint override (self-hosted stores). */
    public static final String ENV_ENDPOINT = "CONFLUENCE_PARQUET_S3_ENDPOINT";
    /** Environment variable for the store's region. */
    public static final String ENV_REGION = "CONFLUENCE_PARQUET_S3_REGION";
    /** Environment variable for the static access key (optional). */
    public static final String ENV_ACCESS_KEY_ID = "CONFLUENCE_PARQUET_S3_ACCESS_KEY_ID";
    /** Environment variable for the static secret key (optional). */
    public static final String ENV_SECRET_ACCESS_KEY = "CONFLUENCE_PARQUET_S3_SECRET_ACCESS_KEY";
    /** Environment variable for the key prefix. */
    public static final String ENV_PREFIX = "CONFLUENCE_PARQUET_S3_PREFIX";
    /** Environment variable for the rows-per-part-file batch size. */
    public static final String ENV_BATCH_SIZE = "CONFLUENCE_PARQUET_S3_BATCH_SIZE";

    /** Default key prefix. */
    public static final String DEFAULT_PREFIX = "confluence-changes";
    /** Default rows per part file. */
    public static final int DEFAULT_BATCH_SIZE = 500;
    /** Default region (what self-hosted stores conventionally report). */
    public static final String DEFAULT_REGION = "us-east-1";

    private static final System.Logger LOG = System.getLogger(ParquetChangeSink.class.getName());

    private final S3ParquetSink sink;
    private final String prefix;
    private final int batchSize;
    private final String runId;
    private final Map<String, List<ConfluenceChange>> buffers = new LinkedHashMap<>();
    private final Map<String, Integer> sequences = new LinkedHashMap<>();

    /**
     * @param s3 the client to upload through (see {@link S3Clients}); this sink closes
     *        it on {@link #close()}
     * @param bucket the bucket part files land in
     * @param prefix key prefix under the bucket (blank becomes {@link #DEFAULT_PREFIX})
     * @param batchSize rows per part file (values below 1 become
     *        {@link #DEFAULT_BATCH_SIZE})
     */
    public ParquetChangeSink(S3Client s3, String bucket, String prefix, int batchSize) {
        this.sink = new S3ParquetSink(Objects.requireNonNull(s3, "s3"), bucket);
        this.prefix = prefix == null || prefix.isBlank()
                ? DEFAULT_PREFIX : trimSlashes(prefix.trim());
        this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
        this.runId = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
                .withZone(ZoneOffset.UTC).format(Instant.now())
                + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Builds the sink from the process environment, active only when
     * {@code CONFLUENCE_PARQUET_S3_BUCKET} is set.
     *
     * @return the sink, or empty when the parquet lane is not configured
     */
    public static Optional<ParquetChangeSink> fromEnvironment() {
        return fromEnvironment(System.getenv());
    }

    /**
     * Builds the sink from an explicit environment map; production calls
     * {@link #fromEnvironment()}, tests call this.
     *
     * @param env the environment to read
     * @return the sink, or empty when the parquet lane is not configured
     */
    static Optional<ParquetChangeSink> fromEnvironment(Map<String, String> env) {
        String bucket = env.get(ENV_BUCKET);
        if (bucket == null || bucket.isBlank()) {
            return Optional.empty();
        }
        String region = env.get(ENV_REGION);
        if (region == null || region.isBlank()) {
            region = DEFAULT_REGION;
        }
        String endpoint = env.get(ENV_ENDPOINT);
        String accessKeyId = env.get(ENV_ACCESS_KEY_ID);
        String secretAccessKey = env.get(ENV_SECRET_ACCESS_KEY);
        boolean staticKeys = accessKeyId != null && !accessKeyId.isBlank()
                && secretAccessKey != null && !secretAccessKey.isBlank();
        S3Client s3;
        if (endpoint != null && !endpoint.isBlank()) {
            s3 = staticKeys ? S3Clients.pathStyle(endpoint, region, accessKeyId, secretAccessKey)
                    : S3Clients.pathStyle(endpoint, region);
        } else if (staticKeys) {
            s3 = S3Clients.awsRegion(region, accessKeyId, secretAccessKey);
        } else {
            s3 = S3Clients.awsRegion(region);
        }
        return Optional.of(new ParquetChangeSink(s3, bucket.trim(), env.get(ENV_PREFIX),
                parseInt(env.get(ENV_BATCH_SIZE), DEFAULT_BATCH_SIZE)));
    }

    /** Buffers one change; flushes its entity kind when the batch size is reached. */
    @Override
    public synchronized void emit(ConfluenceChange change) {
        String entityType = entityType(change);
        List<ConfluenceChange> buffer = buffers.computeIfAbsent(entityType,
                k -> new ArrayList<>());
        buffer.add(change);
        if (buffer.size() >= batchSize) {
            flush(entityType);
        }
    }

    /**
     * A completed sweep flushes every pending buffer, so part files align with crawl
     * runs. The snapshot marker itself is not exported.
     */
    @Override
    public synchronized void snapshot(ConfluenceSnapshot snapshot) {
        flushAll();
    }

    /** Flushes every pending buffer. */
    public synchronized void flush() {
        flushAll();
    }

    /** Flushes every pending buffer and closes the underlying client. */
    @Override
    public synchronized void close() {
        flushAll();
        sink.close();
    }

    private void flushAll() {
        for (String entityType : new ArrayList<>(buffers.keySet())) {
            flush(entityType);
        }
    }

    private void flush(String entityType) {
        List<ConfluenceChange> buffer = buffers.get(entityType);
        if (buffer == null || buffer.isEmpty()) {
            return;
        }
        List<ConfluenceChange> batch = List.copyOf(buffer);
        buffer.clear();
        int part = sequences.getOrDefault(entityType, 0);
        String key = prefix + "/" + entityType + "/" + runId
                + "-part-" + String.format(Locale.ROOT, "%05d", part) + ".parquet";
        try {
            sink.put(key, ConfluenceChange.getDescriptor(), batch);
            sequences.put(entityType, part + 1);
        } catch (Exception e) {
            // Keep the batch for the next flush point; never throw into the crawl loop.
            buffer.addAll(0, batch);
            LOG.log(System.Logger.Level.WARNING,
                    "confluence parquet sink: flush of {0} change(s) to {1} failed: {2}",
                    batch.size(), key, e.toString());
        }
    }

    private static String entityType(ConfluenceChange change) {
        if (!change.hasEntity()) {
            return "changes";
        }
        ConfluenceEntity.EntityCase entityCase = change.getEntity().getEntityCase();
        if (entityCase == ConfluenceEntity.EntityCase.ENTITY_NOT_SET) {
            return "changes";
        }
        return entityCase.name().toLowerCase(Locale.ROOT);
    }

    private static String trimSlashes(String value) {
        String trimmed = value;
        while (trimmed.startsWith("/")) {
            trimmed = trimmed.substring(1);
        }
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed.isEmpty() ? DEFAULT_PREFIX : trimmed;
    }

    private static int parseInt(String value, int fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
