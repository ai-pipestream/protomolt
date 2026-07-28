package ai.pipestream.proto.emit.parquet.s3;

import ai.pipestream.proto.emit.parquet.ParquetEmitter;
import ai.pipestream.proto.emit.parquet.ParquetExportOptions;
import ai.pipestream.proto.emit.parquet.ProtoParquetSchemas;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.util.Objects;

/**
 * Uploads descriptor-driven Parquet files to an S3-compatible object store. The bytes
 * come from {@link ParquetEmitter} (in-memory, no Hadoop); the destination is one
 * {@code putObject} per file against the bucket the operator named at construction -
 * the caller always chooses where message data lands.
 *
 * <p>Object keys must be relative: no leading {@code /}, no {@code ..} segments. The
 * objects are written with the {@code application/vnd.apache.parquet} content type.</p>
 *
 * <p>Thread-safe: {@link S3Client} is, and the sink holds no mutable state.</p>
 */
public final class S3ParquetSink implements AutoCloseable {

    /** The content type every uploaded object is stamped with. */
    public static final String PARQUET_CONTENT_TYPE = "application/vnd.apache.parquet";

    private final S3Client s3;
    private final String bucket;

    /**
     * @param s3 the client to upload through (see {@link S3Clients}); this sink closes
     *        it on {@link #close()}
     * @param bucket the bucket every file lands in
     */
    public S3ParquetSink(S3Client s3, String bucket) {
        this.s3 = Objects.requireNonNull(s3, "s3");
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalArgumentException("bucket cannot be null or blank");
        }
        this.bucket = bucket;
    }

    /**
     * Renders {@code messages} (all instances of {@code descriptor}) as one Parquet file
     * and uploads it to {@code key}.
     *
     * @param key the object key (relative: no leading slash, no {@code ..})
     * @param descriptor the message type
     * @param messages the rows
     * @return the key the file was uploaded to
     * @throws IOException when the Parquet write fails
     */
    public String put(String key, Descriptor descriptor, Iterable<? extends Message> messages)
            throws IOException {
        return putBytes(key, ParquetEmitter.toBytes(descriptor, messages));
    }

    /**
     * The export form: column ids stamped into the file schema and only the projected,
     * masked columns written. See {@link ParquetExportOptions}.
     *
     * @param key the object key (relative: no leading slash, no {@code ..})
     * @param descriptor the message type
     * @param messages the rows
     * @param ids the column-id resolver table formats use
     * @param options projection and masking controls
     * @return the key the file was uploaded to
     * @throws IOException when the Parquet write fails
     */
    public String put(String key, Descriptor descriptor, Iterable<? extends Message> messages,
                      ProtoParquetSchemas.FieldIdResolver ids, ParquetExportOptions options)
            throws IOException {
        return putBytes(key, ParquetEmitter.toBytes(descriptor, messages, ids, options));
    }

    /**
     * Uploads already-rendered Parquet bytes to {@code key}.
     *
     * @param key the object key (relative: no leading slash, no {@code ..})
     * @param parquet the file bytes
     * @return the key the file was uploaded to
     */
    public String putBytes(String key, byte[] parquet) {
        validateKey(key);
        Objects.requireNonNull(parquet, "parquet");
        s3.putObject(b -> b.bucket(bucket).key(key).contentType(PARQUET_CONTENT_TYPE),
                RequestBody.fromBytes(parquet));
        return key;
    }

    /** The bucket this sink uploads to. */
    public String bucket() {
        return bucket;
    }

    /** Closes the underlying client. */
    @Override
    public void close() {
        s3.close();
    }

    private static void validateKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key cannot be null or blank");
        }
        if (key.startsWith("/")) {
            throw new IllegalArgumentException("key must be relative (got \"" + key + "\")");
        }
        for (String segment : key.split("/")) {
            if (segment.equals("..")) {
                throw new IllegalArgumentException("key cannot contain \"..\" (got \""
                        + key + "\")");
            }
        }
    }
}
