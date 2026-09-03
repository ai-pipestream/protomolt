package ai.protomolt.proto.search.snapshot.s3;

import ai.protomolt.proto.search.service.SnapshotStore;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * The S3 implementation of the service's snapshot seam: one bucket, an
 * optional root prefix, whole-file puts and gets. Only immutable commit
 * files move through here; the service never runs a live Lucene directory
 * over object storage.
 */
public final class S3SnapshotStore implements SnapshotStore {

    private final S3Client s3;
    private final String bucket;
    private final String root;

    /**
     * @param s3 the client; owned by the caller
     * @param bucket the bucket snapshots live in
     * @param rootPrefix a key prefix within the bucket; may be empty
     */
    public S3SnapshotStore(S3Client s3, String bucket, String rootPrefix) {
        if (s3 == null) {
            throw new IllegalArgumentException("s3 must not be null");
        }
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalArgumentException("bucket must not be blank");
        }
        this.s3 = s3;
        this.bucket = bucket;
        this.root = rootPrefix == null || rootPrefix.isBlank()
                ? ""
                : rootPrefix.endsWith("/") ? rootPrefix : rootPrefix + "/";
    }

    @Override
    public List<String> list(String prefix) {
        List<String> keys = new ArrayList<>();
        String token = null;
        do {
            ListObjectsV2Request.Builder request = ListObjectsV2Request.builder()
                    .bucket(bucket).prefix(root + prefix);
            if (token != null) {
                request.continuationToken(token);
            }
            ListObjectsV2Response response = s3.listObjectsV2(request.build());
            for (S3Object object : response.contents()) {
                keys.add(object.key().substring(root.length()));
            }
            token = response.nextContinuationToken();
        } while (token != null);
        return keys;
    }

    @Override
    public void put(String key, Path file) {
        s3.putObject(b -> b.bucket(bucket).key(root + key), file);
    }

    @Override
    public void download(String key, Path target) {
        try {
            java.nio.file.Files.deleteIfExists(target);
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException("cannot replace " + target, e);
        }
        s3.getObject(b -> b.bucket(bucket).key(root + key), target);
    }

    @Override
    public void delete(String key) {
        s3.deleteObject(b -> b.bucket(bucket).key(root + key));
    }
}
