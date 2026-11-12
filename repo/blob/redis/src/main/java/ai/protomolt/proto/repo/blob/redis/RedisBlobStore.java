package ai.protomolt.proto.repo.blob.redis;

import ai.protomolt.proto.repo.blob.spi.BlobStoreException;
import ai.protomolt.proto.repo.blob.spi.ExpiringBlobStore;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.ScanParams;
import java.io.InputStream;
import java.io.IOException;
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
import java.util.Objects;

/**
 * Standalone Valkey / Redis-compatible object storage with one atomic hash per encoded object.
 * Layout v2 does not read legacy split byte/metadata keys. Version selection
 * is unsupported. Conditional writes compare content ETags,
 * not mutation epochs. TTL and server eviction/persistence policy
 * are separate: disabling expiry alone does not qualify archival durability.
 */
public final class RedisBlobStore implements ExpiringBlobStore, AutoCloseable {
    private static final int MAX_VALUE_BYTES = 512 * 1024 * 1024;
    private static final int DELETE_CHUNK = 1000;
    private final JedisPool pool;
    private final URI uri;
    private final java.util.Set<RedisTransfer> transfers = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private boolean closed;
    private final int ttlSeconds;
    private final long maxObjectBytes;
    private final RedisObjectKeys keys;
    private final boolean createOnly;

    public RedisBlobStore(RedisBlobStoreConfig config) {
        createOnly = config.writePolicy() == RedisWritePolicy.CREATE_ONLY;
        keys = new RedisObjectKeys(config.keyPrefix(), config.writePolicy());
        ttlSeconds = config.ttlSeconds();
        maxObjectBytes = config.maxObjectBytes();
        uri = URI.create(config.uri());
        pool = new JedisPool(uri);
    }

    public PutResult put(PutSpec spec, byte[] body, int ttlSeconds) {
        Objects.requireNonNull(body);
        requireSize(body.length);
        var target = prepare(spec, ttlSeconds);
        return write(target, body.clone());
    }
    @Override public PutResult put(PutSpec spec, byte[] body) { return put(spec, body, ttlSeconds); }

    /** Caller owns the stream. An incomplete or invalid body never completes the atomic command. */
    @Override public PutResult put(PutSpec spec, InputStream body, long contentLength) {
        Objects.requireNonNull(body);
        requireSize(contentLength);
        var target = prepare(spec, ttlSeconds);
        var digest = digest();
        try (var transfer = openTransfer()) {
            // EVAL, script, key count, key, body, type, etag, timestamp, TTL,
            // attribute pairs, and (for create-only) the final condition.
            transfer.begin(9 + target.metadata().attributes.size() + (createOnly ? 1 : 0));
            transfer.bulk(bytes("EVAL"));
            transfer.bulk(createOnly ? RedisObjectScripts.CONDITIONAL_PUT : RedisObjectScripts.PUT);
            transfer.bulk(bytes("1"));
            transfer.bulk(bytes(target.key()));
            transfer.beginBulk((int) contentLength);
            byte[] buffer = new byte[64 * 1024];
            long remaining = contentLength;
            while (remaining > 0) {
                int count = body.read(buffer, 0, (int) Math.min(remaining, buffer.length));
                if (count < 0) throw new IllegalArgumentException("Stream differs from declared contentLength");
                if (count == 0) {
                    int value = body.read();
                    if (value < 0) throw new IllegalArgumentException("Stream differs from declared contentLength");
                    buffer[0] = (byte) value;
                    count = 1;
                }
                digest.update(buffer, 0, count);
                transfer.write(buffer, count);
                remaining -= count;
            }
            if (body.read() != -1) throw new IllegalArgumentException("Stream differs from declared contentLength");
            String sha = HexFormat.of().formatHex(digest.digest());
            if (target.declaredSha() != null && !target.declaredSha().isEmpty()
                    && !sha.equalsIgnoreCase(target.declaredSha()))
                throw new IllegalArgumentException("verified write rejected: checksum differs from declared body");
            // Do not send the trailing arguments until all local validation succeeds.
            transfer.endBulk();
            transfer.bulk(target.metadata().contentType);
            String etag = "\"" + sha + "\"";
            transfer.bulk(bytes(etag));
            transfer.bulk(bytes(Long.toString(System.currentTimeMillis())));
            transfer.bulk(bytes(Integer.toString(target.ttl())));
            for (byte[] attribute : target.metadata().attributes) transfer.bulk(attribute);
            if (createOnly) transfer.bulk(bytes(""));
            long status = transfer.integerReply();
            if (status == 0) throw new BlobConflictException("Conditional Redis write conflicted");
            if (status != 1) throw new BlobStoreException(BlobStoreException.Code.DATA_LOSS, "Invalid Redis object metadata", null);
            return new PutResult(etag, null);
        } catch (IOException failure) { throw new UncheckedIOException(failure); }
    }

    @Override public long objectLength(String namespace, String key) {
        try (var jedis = pool.getResource()) {
            var result = (List<?>) jedis.eval(RedisObjectScripts.STAT, List.of(bytes(keys.object(namespace, key))), List.of());
            requireResult(result, MAX_VALUE_BYTES, key);
            return ((Number) result.get(1)).longValue();
        }
    }

    @Override public InputStream openStream(String namespace, String key, long offset, long length) {
        if (offset < 0) throw new IllegalArgumentException("Offset must not be negative");
        String physical = keys.object(namespace, key);
        RedisTransfer transfer = null;
        try {
            transfer = openTransfer();
            transfer.command(List.of(bytes("EVAL"), RedisObjectScripts.STREAM, bytes("1"), bytes(physical),
                    bytes(Long.toString(Math.min(offset, MAX_VALUE_BYTES))),
                    bytes(Long.toString(length < 0 ? MAX_VALUE_BYTES : Math.min(length, MAX_VALUE_BYTES)))));
            long status = transfer.readStatus();
            requireResult(List.of(status), MAX_VALUE_BYTES, key);
            return transfer.payload(length < 0 ? MAX_VALUE_BYTES : Math.min(length, MAX_VALUE_BYTES));
        } catch (IOException | RuntimeException | Error failure) {
            if (transfer != null) {
                try { transfer.close(); } catch (IOException close) { failure.addSuppressed(close); }
            }
            if (failure instanceof IOException io) throw new UncheckedIOException(io);
            if (failure instanceof RuntimeException runtime) throw runtime;
            throw (Error) failure;
        }
    }

    private record WriteTarget(String key, RedisWriteMetadata metadata, String declaredSha, int ttl) {}

    private WriteTarget prepare(PutSpec spec, int ttl) {
        Objects.requireNonNull(spec);
        if (ttl < 0) throw new IllegalArgumentException("ttlSeconds must be nonnegative");
        if (createOnly && ttl != 0) throw new IllegalArgumentException("Create-only storage requires zero TTL");
        String key = keys.object(spec.bucket(), spec.key());
        var metadata = new RedisWriteMetadata(spec.contentType(), spec.metadata());
        return new WriteTarget(key, metadata, spec.sha256Hex(), ttl);
    }

    private PutResult write(WriteTarget target, byte[] body) {
        return write(target, body, createOnly ? WriteCondition.absent() : null);
    }

    /** Atomic compare/write. A conflict after an uncertain acknowledgment requires reconciliation. */
    @Override public PutResult conditionalPut(PutSpec spec, byte[] body, WriteCondition condition) {
        Objects.requireNonNull(body); Objects.requireNonNull(condition);
        if (createOnly && !condition.ifAbsent())
            throw new UnsupportedOperationException("Create-only storage does not support matching replacements");
        if (body.length > MAX_CONDITIONAL_BYTES)
            throw new IllegalArgumentException("Conditional object exceeds 9 MiB");
        requireSize(body.length);
        return write(prepare(spec, ttlSeconds), body.clone(), condition);
    }

    @Override public GetResult getForUpdate(String namespace, String key) {
        var result = getBounded(namespace, key, null, MAX_CONDITIONAL_BYTES);
        try { ai.protomolt.proto.repo.blob.spi.BlobStore.requireStrongEtag(result.eTag()); }
        catch (IllegalArgumentException malformed) {
            throw new BlobStoreException(BlobStoreException.Code.DATA_LOSS, "Invalid Redis object ETag", malformed);
        }
        return result;
    }

    private PutResult write(WriteTarget target, byte[] body, WriteCondition condition) {
        String sha = sha256(body);
        if (target.declaredSha() != null && !target.declaredSha().isEmpty() && !sha.equalsIgnoreCase(target.declaredSha()))
            throw new IllegalArgumentException("verified write rejected: checksum differs from declared body");
        String etag = "\"" + sha + "\"";
        var args = new ArrayList<byte[]>();
        args.add(body); args.add(target.metadata().contentType);
        args.add(bytes(etag)); args.add(bytes(Long.toString(System.currentTimeMillis()))); args.add(bytes(Integer.toString(target.ttl())));
        args.addAll(target.metadata().attributes);
        if (condition != null) args.add(bytes(condition.ifAbsent() ? "" : condition.expectedEtag()));
        try (var jedis = pool.getResource()) {
            long status = ((Number) jedis.eval(condition == null ? RedisObjectScripts.PUT : RedisObjectScripts.CONDITIONAL_PUT,
                    List.of(bytes(target.key())), args)).longValue();
            if (status == 0) throw new BlobConflictException("Conditional Redis write conflicted");
            if (status != 1) throw new BlobStoreException(BlobStoreException.Code.DATA_LOSS, "Invalid Redis object metadata", null);
        }
        return new PutResult(etag, null);
    }

    @Override public GetResult get(String namespace, String key, String versionId) {
        return getBounded(namespace, key, versionId, MAX_VALUE_BYTES);
    }
    @Override public GetResult getBounded(String namespace, String key, String versionId, int maxBytes) {
        if (versionId != null) throw new UnsupportedOperationException("Redis does not provide object versions");
        if (maxBytes < 0) throw new IllegalArgumentException("Read limit must not be negative");
        List<?> result;
        try (var jedis = pool.getResource()) {
            result = (List<?>) jedis.eval(RedisObjectScripts.GET, List.of(bytes(keys.object(namespace, key))),
                    List.of(bytes(Integer.toString(maxBytes))));
        }
        requireResult(result, maxBytes, key);
        String type = text(result.get(2));
        return new GetResult((byte[]) result.get(1), type.isEmpty() ? null : type, text(result.get(3)), null);
    }

    @Override public void copy(String sourceNamespace, String sourceKey, String targetNamespace, String targetKey) {
        if (sourceNamespace == null || sourceNamespace.isBlank() || sourceKey == null || sourceKey.isBlank())
            throw new BlobNotFoundException("Redis copy source is not addressable");
        int ceiling = createOnly ? MAX_CONDITIONAL_BYTES : MAX_VALUE_BYTES;
        int limit = (int) (maxObjectBytes == 0 ? ceiling : Math.min(maxObjectBytes, ceiling));
        try (var jedis = pool.getResource()) {
            var result = (List<?>) jedis.eval(createOnly ? RedisObjectScripts.COPY_CREATE_ONLY : RedisObjectScripts.COPY,
                    List.of(bytes(keys.object(sourceNamespace, sourceKey)), bytes(keys.object(targetNamespace, targetKey))),
                    List.of(bytes(Integer.toString(limit)), bytes(Long.toString(System.currentTimeMillis())), bytes(Integer.toString(ttlSeconds))));
            if (((Number) result.getFirst()).intValue() == 4) throw new BlobConflictException("Create-only Redis copy conflicted");
            requireResult(result, limit, sourceKey);
        }
    }

    @Override public boolean delete(String namespace, String key) {
        try (var jedis = pool.getResource()) { return jedis.del(keys.object(namespace, key)) > 0; }
    }
    /** Exact-key absence observation; a later PUT still requires a later cleanup pass. */
    public boolean reclaim(String namespace, String key) {
        try (var jedis = pool.getResource()) {
            return ((Number) jedis.eval(RedisObjectScripts.RECLAIM, List.of(bytes(keys.object(namespace, key))), List.of())).longValue() == 0;
        }
    }
    @Override public BatchDeleteResult deleteAll(String namespace, List<String> supplied) {
        var physical = supplied.stream().filter(k -> k != null && !k.isBlank()).distinct().map(k -> keys.object(namespace, k)).toList();
        try (var jedis = pool.getResource()) {
            for (int start = 0; start < physical.size(); start += DELETE_CHUNK)
                jedis.del(physical.subList(start, Math.min(start + DELETE_CHUNK, physical.size())).toArray(String[]::new));
        }
        return new BatchDeleteResult(Map.of());
    }

    /** SCAN is not a snapshot. De-duplicate results and filter the logical prefix literally. */
    @Override public List<ListedObject> list(String namespace, String prefix) {
        String base = keys.namespace(namespace);
        String requested = prefix == null ? "" : prefix;
        var objects = new LinkedHashMap<String, ListedObject>();
        try (var jedis = pool.getResource()) {
            var params = new ScanParams().match(base + "*").count(1000);
            String cursor = ScanParams.SCAN_POINTER_START;
            do {
                var page = jedis.scan(cursor, params);
                for (String physical : page.getResult()) {
                    String logical = keys.decode(base, physical);
                    if (!logical.startsWith(requested)) continue;
                    var stat = (List<?>) jedis.eval(RedisObjectScripts.STAT, List.of(bytes(physical)), List.of());
                    if (((Number) stat.getFirst()).intValue() == 0) continue; // Concurrent expiry/delete during SCAN.
                    requireResult(stat, MAX_VALUE_BYTES, logical);
                    long modified;
                    try { modified = Long.parseLong(text(stat.get(2))); }
                    catch (NumberFormatException invalid) { throw new BlobStoreException(BlobStoreException.Code.DATA_LOSS, "Invalid Redis object timestamp", invalid); }
                    objects.put(logical, new ListedObject(logical, ((Number) stat.get(1)).longValue(), modified));
                }
                cursor = page.getCursor();
            } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
        }
        return List.copyOf(objects.values());
    }

    @Override public void headBucket(String namespace) {
        keys.namespace(namespace);
        try (var jedis = pool.getResource()) { jedis.ping(); }
    }
    @Override public void headObject(String namespace, String key) {
        try (var jedis = pool.getResource()) {
            requireResult((List<?>) jedis.eval(RedisObjectScripts.STAT, List.of(bytes(keys.object(namespace, key))), List.of()), MAX_VALUE_BYTES, key);
        }
    }
    private RedisTransfer openTransfer() throws IOException {
        synchronized (transfers) {
            if (closed) throw new IllegalStateException("Redis store is closed");
        }
        var transfer = new RedisTransfer(uri, transfers::remove);
        synchronized (transfers) {
            if (closed) {
                transfer.close();
                throw new IllegalStateException("Redis store is closed");
            }
            transfers.add(transfer);
        }
        return transfer;
    }

    @Override public void close() {
        List<RedisTransfer> active;
        synchronized (transfers) {
            if (closed) return;
            closed = true;
            active = List.copyOf(transfers);
        }
        IOException failure = null;
        for (var transfer : active) {
            try { transfer.close(); }
            catch (IOException close) {
                if (failure == null) failure = close; else failure.addSuppressed(close);
            }
        }
        try { pool.close(); }
        catch (RuntimeException close) {
            if (failure != null) close.addSuppressed(failure);
            throw close;
        }
        if (failure != null) throw new UncheckedIOException(failure);
    }

    private void requireSize(long size) {
        if (size < 0 || size > MAX_VALUE_BYTES || (maxObjectBytes > 0 && size > maxObjectBytes))
            throw new IllegalArgumentException("Object length exceeds Redis value or configured maxObjectBytes limit");
        if (createOnly && size > MAX_CONDITIONAL_BYTES)
            throw new IllegalArgumentException("Create-only object exceeds 9 MiB");
    }
    private static void requireResult(List<?> result, int bound, String key) {
        int status = ((Number) result.getFirst()).intValue();
        if (status == 0) throw new BlobNotFoundException("Redis object does not exist: " + key);
        if (status == 2) throw new BlobReadLimitException(bound);
        if (status != 1) throw new BlobStoreException(BlobStoreException.Code.DATA_LOSS, "Redis object is missing required metadata", null);
    }
    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private static String text(Object value) { return new String((byte[]) value, StandardCharsets.UTF_8); }
    private static String sha256(byte[] body) {
        return HexFormat.of().formatHex(digest().digest(body));
    }
    private static MessageDigest digest() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException("SHA-256 unavailable", impossible); }
    }
}
