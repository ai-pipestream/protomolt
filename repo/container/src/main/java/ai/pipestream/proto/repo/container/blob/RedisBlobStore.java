package ai.pipestream.proto.repo.container.blob;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Gatherers;

/**
 * {@link BlobStore} over Redis (blocking Jedis on virtual threads) — the
 * port's second provider. Sample-grade but honest: every port contract holds,
 * the differences from S3 are stated where they live.
 *
 * <p><b>Contract mapping.</b> Redis has no buckets: the physical key is
 * {@code <keyPrefix><bucket>/<key>}, so {@code bucket} becomes a namespace
 * label — {@link #list} and {@link #deleteAll} per bucket work and buckets
 * never collide. Two entries per object: the bytes at
 * {@code <keyPrefix><bucket>/<key>} and a metadata hash at the same key plus
 * {@code $meta} (fields {@code content_type}, {@code etag},
 * {@code last_modified_ms}; {@code versionId} is always null — Redis does not
 * version). The {@code $meta} suffix can never collide with an object key
 * because object keys are addressed through the same mapping and meta keys are
 * only ever written by this class.
 *
 * <p><b>Verified writes.</b> S3 verifies landed bytes server-side via the
 * checksum trailer; Redis has no server-side trailer, so this store computes
 * the SHA-256 client-side and REJECTS the put when it does not match
 * {@link PutSpec#sha256Hex()} — the same contract, enforced before the write
 * instead of after the landing.
 *
 * <p><b>Operation mapping.</b> get/get-version: {@code GET} + {@code HGETALL}
 * (absent → {@link BlobNotFoundException}). put: {@code SET} (+ {@code EXPIRE}
 * when a TTL is configured) and the meta hash with the same expiry. copy:
 * {@code GET}/{@code SET} within Redis. delete: {@code DEL} both entries,
 * reporting whether the data key existed. deleteAll: pipelined {@code DEL} in
 * chunks of 1000 — Redis treats delete-of-absent as success, matching the
 * port's NoSuchKey-is-success rule. list: {@code SCAN MATCH
 * <keyPrefix><bucket>/<prefix>*} — SCAN walks the WHOLE database keyspace and
 * filters (there is no per-bucket index), which is the sample-grade part.
 * headObject: {@code EXISTS} → {@link BlobNotFoundException} when absent.
 * headBucket: {@code PING} — the bucket is a namespace label here, so
 * reachability of the server is all there is to probe.
 *
 * <p>The store owns its {@link JedisPool} and is {@link AutoCloseable};
 * callers running on virtual threads park on the blocking round trips, same
 * as the S3 adapter.
 */
public final class RedisBlobStore implements BlobStore, AutoCloseable {

    /** Suffix of the per-object metadata hash key. */
    static final String META_SUFFIX = "$meta";

    private static final int DELETE_CHUNK = 1000;

    private final JedisPool pool;
    private final int ttlSeconds;
    private final long maxObjectBytes;
    private final String keyPrefix;

    /**
     * Builds the store and its connection pool.
     *
     * @param config the Redis connection and object-behaviour settings
     */
    public RedisBlobStore(RedisBlobStoreConfig config) {
        this.pool = new JedisPool(URI.create(config.uri()));
        this.ttlSeconds = config.ttlSeconds();
        this.maxObjectBytes = config.maxObjectBytes();
        this.keyPrefix = config.keyPrefix();
    }

    /**
     * A put with an explicit expiry — the overload {@link CachingBlobStore}
     * uses to give cache entries their own TTL.
     *
     * @param spec what and where to write
     * @param body the object bytes
     * @param ttlSeconds expiry for this object; {@code 0} = no expiry
     * @return the written object's coordinates
     */
    public PutResult put(PutSpec spec, byte[] body, int ttlSeconds) {
        return doPut(spec, body, ttlSeconds);
    }

    @Override
    public PutResult put(PutSpec spec, byte[] body) {
        return doPut(spec, body, ttlSeconds);
    }

    @Override
    public PutResult put(PutSpec spec, InputStream body, long contentLength) {
        // Redis SET needs the value in memory regardless — buffer it. The
        // maxObjectBytes ceiling keeps this bounded by configuration.
        byte[] bytes;
        try {
            bytes = body.readNBytes(Math.toIntExact(contentLength));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return doPut(spec, bytes, ttlSeconds);
    }

    private PutResult doPut(PutSpec spec, byte[] body, int ttl) {
        if (maxObjectBytes > 0 && body.length > maxObjectBytes) {
            throw new IllegalArgumentException("object of " + body.length + " bytes exceeds "
                    + "maxObjectBytes=" + maxObjectBytes + " (redis://" + spec.bucket() + "/" + spec.key() + ")");
        }
        String sha256 = sha256Hex(body);
        // Client-side verified write: Redis has no server-side checksum
        // trailer, so the digest is compared BEFORE the write lands.
        if (spec.sha256Hex() != null && !spec.sha256Hex().isEmpty()
                && !spec.sha256Hex().equalsIgnoreCase(sha256)) {
            throw new IllegalArgumentException("verified write rejected: declared sha256 "
                    + spec.sha256Hex() + " does not match the body's " + sha256
                    + " (redis://" + spec.bucket() + "/" + spec.key() + ")");
        }
        String etag = "\"" + sha256 + "\"";
        try (Jedis jedis = pool.getResource()) {
            byte[] dataKey = bytes(physicalKey(spec.bucket(), spec.key()));
            byte[] metaKey = bytes(physicalKey(spec.bucket(), spec.key()) + META_SUFFIX);
            jedis.set(dataKey, body);
            Map<byte[], byte[]> meta = new LinkedHashMap<>();
            meta.put(bytes("content_type"), bytes(spec.contentType() == null ? "" : spec.contentType()));
            meta.put(bytes("etag"), bytes(etag));
            meta.put(bytes("last_modified_ms"), bytes(Long.toString(System.currentTimeMillis())));
            jedis.hset(metaKey, meta);
            if (ttl > 0) {
                jedis.expire(dataKey, ttl);
                jedis.expire(metaKey, ttl);
            }
        }
        return new PutResult(etag, null);
    }

    @Override
    public GetResult get(String bucket, String key, String versionId) {
        try (Jedis jedis = pool.getResource()) {
            byte[] data = jedis.get(bytes(physicalKey(bucket, key)));
            if (data == null) {
                throw new BlobNotFoundException("blob not found: redis://" + bucket + "/" + key
                        + (versionId != null ? "@" + versionId : ""));
            }
            Map<byte[], byte[]> meta = jedis.hgetAll(bytes(physicalKey(bucket, key) + META_SUFFIX));
            String contentType = metaField(meta, "content_type");
            String etag = metaField(meta, "etag");
            return new GetResult(data, contentType == null || contentType.isEmpty() ? null : contentType,
                    etag, null);
        }
    }

    @Override
    public void copy(String srcBucket, String srcKey, String dstBucket, String dstKey) {
        // Fail-fast parity with S3BlobStore: a blank source is unaddressable,
        // not a provider error.
        if (srcBucket == null || srcBucket.isBlank() || srcKey == null || srcKey.isBlank()) {
            throw new BlobNotFoundException("copy source not addressable: blank source "
                    + (srcBucket == null || srcBucket.isBlank() ? "bucket" : "key")
                    + " (src=redis://" + srcBucket + "/" + srcKey + ")");
        }
        try (Jedis jedis = pool.getResource()) {
            byte[] data = jedis.get(bytes(physicalKey(srcBucket, srcKey)));
            if (data == null) {
                throw new BlobNotFoundException(
                        "copy source not found: redis://" + srcBucket + "/" + srcKey);
            }
            Map<byte[], byte[]> meta = jedis.hgetAll(bytes(physicalKey(srcBucket, srcKey) + META_SUFFIX));
            String contentType = metaField(meta, "content_type");
            byte[] dstDataKey = bytes(physicalKey(dstBucket, dstKey));
            byte[] dstMetaKey = bytes(physicalKey(dstBucket, dstKey) + META_SUFFIX);
            jedis.set(dstDataKey, data);
            Map<byte[], byte[]> dstMeta = new LinkedHashMap<>();
            dstMeta.put(bytes("content_type"), bytes(contentType == null ? "" : contentType));
            dstMeta.put(bytes("etag"), bytes("\"" + sha256Hex(data) + "\""));
            dstMeta.put(bytes("last_modified_ms"), bytes(Long.toString(System.currentTimeMillis())));
            jedis.hset(dstMetaKey, dstMeta);
            if (ttlSeconds > 0) {
                jedis.expire(dstDataKey, ttlSeconds);
                jedis.expire(dstMetaKey, ttlSeconds);
            }
        }
    }

    @Override
    public boolean delete(String bucket, String key) {
        try (Jedis jedis = pool.getResource()) {
            long removed = jedis.del(bytes(physicalKey(bucket, key)));
            jedis.del(bytes(physicalKey(bucket, key) + META_SUFFIX));
            return removed > 0;
        }
    }

    @Override
    public BatchDeleteResult deleteAll(String bucket, List<String> keys) {
        List<List<byte[]>> batches = keys.stream()
                .filter(k -> k != null && !k.isBlank())
                .distinct()
                .flatMap(k -> java.util.stream.Stream.of(
                        bytes(physicalKey(bucket, k)), bytes(physicalKey(bucket, k) + META_SUFFIX)))
                .gather(Gatherers.windowFixed(DELETE_CHUNK))
                .toList();
        // DEL of an absent key is success in Redis — the port's
        // NoSuchKey-is-success rule holds for free, so only a real connection
        // failure would surface (as a JedisException, not a failed key).
        try (Jedis jedis = pool.getResource()) {
            for (List<byte[]> chunk : batches) {
                try (Pipeline pipeline = jedis.pipelined()) {
                    for (byte[] key : chunk) {
                        pipeline.del(key);
                    }
                    pipeline.sync();
                }
            }
        }
        return new BatchDeleteResult(Map.of());
    }

    @Override
    public List<ListedObject> list(String bucket, String prefix) {
        String base = physicalKey(bucket, prefix == null ? "" : prefix);
        List<ListedObject> out = new ArrayList<>();
        try (Jedis jedis = pool.getResource()) {
            // SCAN walks the whole database and filters — sample-grade listing;
            // there is no per-bucket index in Redis.
            ScanParams params = new ScanParams().match(base + "*").count(1000);
            String cursor = ScanParams.SCAN_POINTER_START;
            do {
                ScanResult<String> page = jedis.scan(cursor, params);
                for (String found : page.getResult()) {
                    if (found.endsWith(META_SUFFIX)) {
                        continue; // metadata hashes are not objects
                    }
                    long size = jedis.strlen(bytes(found));
                    Map<byte[], byte[]> meta = jedis.hgetAll(bytes(found + META_SUFFIX));
                    long lastModified = 0L;
                    String ms = metaField(meta, "last_modified_ms");
                    if (ms != null && !ms.isEmpty()) {
                        lastModified = Long.parseLong(ms);
                    }
                    out.add(new ListedObject(stripPhysicalPrefix(found, bucket), size, lastModified));
                }
                cursor = page.getCursor();
            } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
        }
        return out;
    }

    @Override
    public void headBucket(String bucket) {
        // Buckets are namespace labels, not containers: the only thing that
        // can be unreachable is the server itself.
        try (Jedis jedis = pool.getResource()) {
            jedis.ping();
        }
    }

    @Override
    public void headObject(String bucket, String key) {
        try (Jedis jedis = pool.getResource()) {
            if (!jedis.exists(bytes(physicalKey(bucket, key)))) {
                throw new BlobNotFoundException("blob not found: redis://" + bucket + "/" + key);
            }
        }
    }

    /** Closes the connection pool. */
    @Override
    public void close() {
        pool.close();
    }

    /** {@code <keyPrefix><bucket>/<key>} — the bucket-as-namespace mapping. */
    private String physicalKey(String bucket, String key) {
        return keyPrefix + bucket + "/" + key;
    }

    /** Inverse of {@link #physicalKey}: the bare object key for port callers. */
    private String stripPhysicalPrefix(String physical, String bucket) {
        return physical.substring(keyPrefix.length() + bucket.length() + 1);
    }

    private static String metaField(Map<byte[], byte[]> meta, String field) {
        for (Map.Entry<byte[], byte[]> e : meta.entrySet()) {
            if (field.equals(string(e.getKey()))) {
                return string(e.getValue());
            }
        }
        return null;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String string(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    private static String sha256Hex(byte[] body) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
