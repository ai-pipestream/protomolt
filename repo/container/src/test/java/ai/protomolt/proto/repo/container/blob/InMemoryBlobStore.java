package ai.protomolt.proto.repo.container.blob;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link BlobStore} over a concurrent map — the no-docker test double for the
 * part IO layer. Honours the port's contracts: not-found raises
 * {@link BlobNotFoundException}, delete-of-absent reports {@code false}, and
 * batch delete treats missing keys as success.
 */
final class InMemoryBlobStore implements BlobStore {

    private record Stored(byte[] data, String contentType, Map<String, String> metadata) {
    }

    private final Map<String, Map<String, Stored>> buckets = new ConcurrentHashMap<>();

    @Override
    public PutResult put(PutSpec spec, byte[] body) {
        bucket(spec.bucket()).put(spec.key(),
                new Stored(body.clone(), spec.contentType(), spec.metadata()));
        return new PutResult("\"mem-" + Integer.toHexString(Arrays.hashCode(body)) + "\"", null);
    }

    @Override
    public PutResult put(PutSpec spec, InputStream body, long contentLength) {
        try {
            return put(spec, body.readNBytes(Math.toIntExact(contentLength)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public GetResult get(String bucket, String key, String versionId) {
        Stored s = bucket(bucket).get(key);
        if (s == null) {
            throw new BlobNotFoundException("blob not found: mem://" + bucket + "/" + key
                    + (versionId != null ? "@" + versionId : ""));
        }
        return new GetResult(s.data().clone(), s.contentType(),
                "\"mem-" + Integer.toHexString(Arrays.hashCode(s.data())) + "\"", null);
    }

    @Override
    public void copy(String srcBucket, String srcKey, String dstBucket, String dstKey) {
        if (srcBucket == null || srcBucket.isBlank() || srcKey == null || srcKey.isBlank()) {
            throw new BlobNotFoundException("copy source not addressable: blank source");
        }
        GetResult src = get(srcBucket, srcKey, null);
        put(new PutSpec(dstBucket, dstKey, src.contentType(), null, null), src.data());
    }

    @Override
    public boolean delete(String bucket, String key) {
        return bucket(bucket).remove(key) != null;
    }

    @Override
    public BatchDeleteResult deleteAll(String bucket, List<String> keys) {
        Map<String, Stored> b = bucket(bucket);
        for (String key : keys) {
            if (key != null && !key.isBlank()) {
                b.remove(key);
            }
        }
        return new BatchDeleteResult(Map.of());
    }

    @Override
    public List<ListedObject> list(String bucket, String prefix) {
        List<ListedObject> out = new ArrayList<>();
        bucket(bucket).forEach((key, stored) -> {
            if (prefix == null || prefix.isBlank() || key.startsWith(prefix)) {
                out.add(new ListedObject(key, stored.data().length, 0L));
            }
        });
        return out;
    }

    @Override
    public void headBucket(String bucket) {
        if (!buckets.containsKey(bucket)) {
            throw new IllegalStateException("bucket not reachable: " + bucket);
        }
    }

    @Override
    public void headObject(String bucket, String key) {
        if (!bucket(bucket).containsKey(key)) {
            throw new BlobNotFoundException("blob not found: mem://" + bucket + "/" + key);
        }
    }

    private Map<String, Stored> bucket(String bucket) {
        return buckets.computeIfAbsent(bucket, b -> new ConcurrentHashMap<>());
    }
}
