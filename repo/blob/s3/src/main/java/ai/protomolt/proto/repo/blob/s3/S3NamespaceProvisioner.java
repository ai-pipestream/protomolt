package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.NamespaceProvisioner;
import java.util.Objects;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Bucket lifecycle over a borrowed client; its owner retains cleanup responsibility.
 *
 * <p>A provisioned namespace is always versioned. Retained object bindings record the
 * provider version id of every committed object, and S3 only returns one for a bucket
 * with versioning enabled; an unversioned bucket leaves the binding with the literal
 * {@code null} version, which is not an immutable identity and cannot be restored by
 * exact version. Creating or adopting a bucket therefore enables versioning and verifies
 * the reported status before the namespace is handed to a drive. A bucket whose
 * versioning cannot be enabled or verified is refused rather than used unversioned.
 */
public final class S3NamespaceProvisioner implements NamespaceProvisioner {
    private final S3Client client;

    public S3NamespaceProvisioner(S3Client client) {
        this.client = Objects.requireNonNull(client, "client");
    }

    @Override public void ensureNamespace(String namespace) {
        boolean present;
        try {
            client.headBucket(b -> b.bucket(namespace));
            present = true;
        } catch (S3Exception e) {
            if (e.statusCode() != 404) throw e;
            present = false;
        }
        if (!present) {
            client.createBucket(b -> b.bucket(namespace));
            client.headBucket(b -> b.bucket(namespace));
        }
        ensureVersioned(namespace);
    }

    /** Enables versioning when the bucket does not report it, then requires the enabled status. */
    private void ensureVersioned(String namespace) {
        if (status(namespace) != BucketVersioningStatus.ENABLED) {
            client.putBucketVersioning(b -> b.bucket(namespace)
                    .versioningConfiguration(v -> v.status(BucketVersioningStatus.ENABLED)));
        }
        BucketVersioningStatus status = status(namespace);
        if (status != BucketVersioningStatus.ENABLED) {
            throw new IllegalStateException("Bucket versioning is not enabled on namespace " + namespace
                    + " (reported " + (status == null ? "unset" : status) + "); managed retention requires provider version ids");
        }
    }

    private BucketVersioningStatus status(String namespace) {
        return client.getBucketVersioning(b -> b.bucket(namespace)).status();
    }
}
