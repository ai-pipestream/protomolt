package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.ObjectReclaimer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

/** One bounded batch per pass; version-list permission is required, never bypassed. */
public final class S3ObjectReclaimer implements ObjectReclaimer {
    private final S3Client client;
    public S3ObjectReclaimer(S3Client client) { this.client = Objects.requireNonNull(client); }

    @Override public boolean reclaim(String bucket, String key) {
        if (bucket == null || bucket.isBlank() || key == null || key.isEmpty())
            throw new IllegalArgumentException("Exact bucket and key are required for reclamation");
        List<ObjectIdentifier> versions = findVersions(bucket, key);
        if (!versions.isEmpty()) {
            var deleted = client.deleteObjects(r -> r.bucket(bucket).delete(d -> d.objects(versions)));
            if (!deleted.errors().isEmpty())
                throw new IllegalStateException("Physical reclamation failed for one or more object versions");
        }
        if (!findVersions(bucket, key).isEmpty()) return false;
        try {
            client.headObject(r -> r.bucket(bucket).key(key));
            return false; // A late PUT or inconsistent listing: require another pass.
        } catch (S3Exception absent) {
            if (absent.statusCode() == 404) return true;
            throw absent;
        }
    }

    private List<ObjectIdentifier> findVersions(String bucket, String key) {
        String keyMarker = null;
        String versionMarker = null;
        for (int page = 0; page < 100; page++) {
            var response = client.listObjectVersions(ListObjectVersionsRequest.builder().bucket(bucket)
                    .prefix(key).maxKeys(1000).keyMarker(keyMarker).versionIdMarker(versionMarker).build());
            var found = new ArrayList<ObjectIdentifier>();
            response.versions().stream().filter(v -> key.equals(v.key())).forEach(v -> found.add(
                    ObjectIdentifier.builder().key(key).versionId(v.versionId()).build()));
            response.deleteMarkers().stream().filter(v -> key.equals(v.key())).forEach(v -> found.add(
                    ObjectIdentifier.builder().key(key).versionId(v.versionId()).build()));
            if (!found.isEmpty()) return List.copyOf(found);
            // S3 version listings order by key. Every other key matching this
            // prefix follows the exact key, so unrelated neighbors cannot hide
            // target versions on later pages or exhaust the bounded scan.
            if (response.versions().stream().anyMatch(v -> !key.equals(v.key()))
                    || response.deleteMarkers().stream().anyMatch(v -> !key.equals(v.key()))) return List.of();
            if (!Boolean.TRUE.equals(response.isTruncated())) return List.of();
            String nextKey = response.nextKeyMarker();
            String nextVersion = response.nextVersionIdMarker();
            if (nextKey == null || (Objects.equals(keyMarker, nextKey) && Objects.equals(versionMarker, nextVersion)))
                throw new IllegalStateException("Object version listing did not advance");
            keyMarker = nextKey;
            versionMarker = nextVersion;
        }
        throw new IllegalStateException("Object version listing exceeded the bounded scan");
    }
}
