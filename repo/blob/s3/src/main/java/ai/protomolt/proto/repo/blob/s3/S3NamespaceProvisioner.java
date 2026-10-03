package ai.protomolt.proto.repo.blob.s3;

import ai.protomolt.proto.repo.blob.spi.NamespaceProvisioner;
import java.util.Objects;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** Bucket lifecycle over a borrowed client; its owner retains cleanup responsibility. */
public final class S3NamespaceProvisioner implements NamespaceProvisioner {
    private final S3Client client;

    public S3NamespaceProvisioner(S3Client client) {
        this.client = Objects.requireNonNull(client, "client");
    }

    @Override public void ensureNamespace(String namespace) {
        try {
            client.headBucket(b -> b.bucket(namespace));
            return;
        } catch (S3Exception e) {
            if (e.statusCode() != 404) throw e;
        }
        client.createBucket(b -> b.bucket(namespace));
        client.headBucket(b -> b.bucket(namespace));
    }
}
