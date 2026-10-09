package ai.protomolt.proto.repo.blob.spi;

import java.util.Map;

/** Trusted installed factory. Construction and identity lookup must perform no I/O. */
public interface BlobStoreProvider {
    String id();

    /**
     * Describe the original physical location without acquiring resources. The
     * provider validates identity-bearing options and returns only nonsecret
     * fields. Other runtime options never become persisted identity implicitly.
     * Unsupported providers must fail; this is not a durability qualification.
     */
    default BackendIdentity managedIdentity(Map<String, String> options) {
        throw new UnsupportedOperationException("Provider does not define a managed backend identity");
    }

    /** Validate all options before acquisition; clean up acquired resources on failure. */
    OpenedBlobStore open(Map<String, String> options);
}
