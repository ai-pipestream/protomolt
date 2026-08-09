package ai.pipestream.proto.acquire.confluence;

import ai.pipestream.proto.acquire.confluence.v1.ConfluenceChange;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceContentRow;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceSnapshot;
import ai.pipestream.proto.descriptors.DescriptorRegistry;
import ai.pipestream.proto.emit.parquet.s3.S3Clients;
import ai.pipestream.proto.emit.parquet.s3.S3ParquetSink;
import ai.pipestream.proto.projection.MessageProjection;
import ai.pipestream.proto.projection.ProjectionException;
import com.google.protobuf.Message;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * The curated counterpart of {@link ParquetChangeSink}: projects every
 * {@link ConfluenceChange} into one flat {@link ConfluenceContentRow} analytics row
 * through the {@link MessageProjection} engine and flushes the rows as Parquet part
 * files to an S3-compatible store through {@link S3ParquetSink}. Where the raw archive
 * keeps the full change envelopes, this lane is a derived dataset: one row per change,
 * page and blog post arms unified into the same columns by the projection's candidate
 * paths, ready for analytics queries without unwrapping the envelope.
 *
 * <p>The projection is compiled once at construction from the descriptor options on
 * {@code ConfluenceContentRow}; a missing or invalid projection (e.g. a CEL rule that
 * compiles against no declared source) fails the constructor, because that is a
 * configuration error, not data. A change that fails to project at run time (a CEL
 * evaluation error, an uncoercible value) is logged and skipped - one bad change never
 * wedges the crawl.</p>
 *
 * <p>DELETE changes project like any other: the identity and operation columns carry
 * the tombstone and the content columns come out empty - the path-driven columns are
 * declared {@code optional} in the row schema, so their absence survives into the
 * Parquet file as nulls, while the CEL-derived columns (content_type, status,
 * label_names, body_length) carry their zero value, since a CEL conditional cannot
 * express absence.</p>
 *
 * <p>Key layout: {@code <prefix>/content/<runId>-part-<NNNNN>.parquet} - one row type,
 * so one directory. {@code runId} is a per-sink-instance id (UTC timestamp plus a
 * random suffix, so reruns never overwrite a previous run's parts) and the part number
 * is a per-sink sequence.</p>
 *
 * <p>Delivery mirrors {@link ParquetChangeSink}: the buffer flushes at the batch size,
 * on {@link #snapshot(ConfluenceSnapshot)}, and on {@link #close()}. A failed upload
 * keeps the batch buffered and is retried at the next flush point; the failure is
 * logged, never thrown into the crawler loop.</p>
 *
 * <p>Configuration is env-driven like the other sinks:</p>
 * <ul>
 *   <li>{@code CONFLUENCE_PROJECTED_PARQUET_S3_BUCKET}: activates the sink</li>
 *   <li>{@code CONFLUENCE_PROJECTED_PARQUET_S3_ENDPOINT}: path-style endpoint override
 *   for a self-hosted store; unset means AWS S3 in
 *   {@code CONFLUENCE_PROJECTED_PARQUET_S3_REGION}</li>
 *   <li>{@code CONFLUENCE_PROJECTED_PARQUET_S3_REGION}: default {@code us-east-1}</li>
 *   <li>{@code CONFLUENCE_PROJECTED_PARQUET_S3_ACCESS_KEY_ID} /
 *   {@code CONFLUENCE_PROJECTED_PARQUET_S3_SECRET_ACCESS_KEY}: static credentials; when
 *   unset the AWS SDK default provider chain supplies them</li>
 *   <li>{@code CONFLUENCE_PROJECTED_PARQUET_S3_PREFIX}: key prefix, default
 *   {@code confluence-content-rows}</li>
 *   <li>{@code CONFLUENCE_PROJECTED_PARQUET_S3_BATCH_SIZE}: rows per part file, default
 *   500</li>
 * </ul>
 *
 * <p>Thread-safe: the crawler emits from virtual threads concurrently; every method is
 * synchronized.</p>
 */
public final class ProjectedParquetChangeSink implements ChangeSink, AutoCloseable {

    /** Environment variable for the target bucket; the sink activates when it is set. */
    public static final String ENV_BUCKET = "CONFLUENCE_PROJECTED_PARQUET_S3_BUCKET";
    /** Environment variable for the path-style endpoint override (self-hosted stores). */
    public static final String ENV_ENDPOINT = "CONFLUENCE_PROJECTED_PARQUET_S3_ENDPOINT";
    /** Environment variable for the store's region. */
    public static final String ENV_REGION = "CONFLUENCE_PROJECTED_PARQUET_S3_REGION";
    /** Environment variable for the static access key (optional). */
    public static final String ENV_ACCESS_KEY_ID =
            "CONFLUENCE_PROJECTED_PARQUET_S3_ACCESS_KEY_ID";
    /** Environment variable for the static secret key (optional). */
    public static final String ENV_SECRET_ACCESS_KEY =
            "CONFLUENCE_PROJECTED_PARQUET_S3_SECRET_ACCESS_KEY";
    /** Environment variable for the key prefix. */
    public static final String ENV_PREFIX = "CONFLUENCE_PROJECTED_PARQUET_S3_PREFIX";
    /** Environment variable for the rows-per-part-file batch size. */
    public static final String ENV_BATCH_SIZE = "CONFLUENCE_PROJECTED_PARQUET_S3_BATCH_SIZE";

    /** Default key prefix. */
    public static final String DEFAULT_PREFIX = "confluence-content-rows";
    /** Default rows per part file. */
    public static final int DEFAULT_BATCH_SIZE = 500;
    /** Default region (what self-hosted stores conventionally report). */
    public static final String DEFAULT_REGION = "us-east-1";

    private static final System.Logger LOG =
            System.getLogger(ProjectedParquetChangeSink.class.getName());

    private final S3ParquetSink sink;
    private final MessageProjection projection;
    private final String prefix;
    private final int batchSize;
    private final String runId;
    private final List<Message> buffer = new ArrayList<>();
    private int sequence;

    /**
     * @param s3 the client to upload through (see {@link S3Clients}); this sink closes
     *        it on {@link #close()}
     * @param bucket the bucket part files land in
     * @param prefix key prefix under the bucket (blank becomes {@link #DEFAULT_PREFIX})
     * @param batchSize rows per part file (values below 1 become
     *        {@link #DEFAULT_BATCH_SIZE})
     * @throws IllegalStateException when {@code ConfluenceContentRow} declares no
     *         projection sources
     * @throws ProjectionException when the declared projection is invalid (a CEL rule
     *         that compiles against no declared source)
     */
    public ProjectedParquetChangeSink(S3Client s3, String bucket, String prefix, int batchSize) {
        this.sink = new S3ParquetSink(Objects.requireNonNull(s3, "s3"), bucket);
        this.prefix = prefix == null || prefix.isBlank()
                ? DEFAULT_PREFIX : trimSlashes(prefix.trim());
        this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
        this.runId = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
                .withZone(ZoneOffset.UTC).format(Instant.now())
                + "-" + UUID.randomUUID().toString().substring(0, 8);
        DescriptorRegistry registry = DescriptorRegistry.create(false);
        registry.register(ConfluenceChange.getDescriptor());
        this.projection = MessageProjection
                .forTarget(ConfluenceContentRow.getDescriptor(), registry)
                .orElseThrow(() -> new IllegalStateException(
                        "ConfluenceContentRow declares no projection sources"));
    }

    /**
     * Builds the sink from the process environment, active only when
     * {@code CONFLUENCE_PROJECTED_PARQUET_S3_BUCKET} is set.
     *
     * @return the sink, or empty when the projected-parquet lane is not configured
     */
    public static Optional<ProjectedParquetChangeSink> fromEnvironment() {
        return fromEnvironment(System.getenv());
    }

    /**
     * Builds the sink from an explicit environment map; production calls
     * {@link #fromEnvironment()}, tests call this.
     *
     * @param env the environment to read
     * @return the sink, or empty when the projected-parquet lane is not configured
     */
    static Optional<ProjectedParquetChangeSink> fromEnvironment(Map<String, String> env) {
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
        return Optional.of(new ProjectedParquetChangeSink(s3, bucket.trim(),
                env.get(ENV_PREFIX), parseInt(env.get(ENV_BATCH_SIZE), DEFAULT_BATCH_SIZE)));
    }

    /**
     * Projects one change into a content row and buffers it; flushes when the batch
     * size is reached. A change that fails to project is logged and skipped.
     */
    @Override
    public synchronized void emit(ConfluenceChange change) {
        Message row;
        try {
            row = projection.project(change);
        } catch (ProjectionException e) {
            // A change that cannot project is data, not config: skip it, never wedge
            // the crawl.
            LOG.log(System.Logger.Level.WARNING,
                    "confluence projected parquet sink: change {0} failed to project: {1}",
                    change.getChangeId(), e.toString());
            return;
        }
        buffer.add(row);
        if (buffer.size() >= batchSize) {
            flush();
        }
    }

    /**
     * A completed sweep flushes the pending buffer, so part files align with crawl
     * runs. The snapshot marker itself is not exported.
     */
    @Override
    public synchronized void snapshot(ConfluenceSnapshot snapshot) {
        flush();
    }

    /** Flushes the pending buffer. */
    public synchronized void flush() {
        if (buffer.isEmpty()) {
            return;
        }
        List<Message> batch = List.copyOf(buffer);
        buffer.clear();
        String key = prefix + "/content/" + runId
                + "-part-" + String.format(Locale.ROOT, "%05d", sequence) + ".parquet";
        try {
            sink.put(key, ConfluenceContentRow.getDescriptor(), batch);
            sequence++;
        } catch (Exception e) {
            // Keep the batch for the next flush point; never throw into the crawl loop.
            buffer.addAll(0, batch);
            LOG.log(System.Logger.Level.WARNING,
                    "confluence projected parquet sink: flush of {0} row(s) to {1} failed: {2}",
                    batch.size(), key, e.toString());
        }
    }

    /** Flushes the pending buffer and closes the underlying client. */
    @Override
    public synchronized void close() {
        flush();
        sink.close();
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
