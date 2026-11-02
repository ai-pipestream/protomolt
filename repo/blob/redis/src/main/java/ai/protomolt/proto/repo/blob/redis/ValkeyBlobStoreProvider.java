package ai.protomolt.proto.repo.blob.redis;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.blob.spi.BlobStoreProvider;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;
import java.util.Map;

/**
 * Preferred Valkey selection for the shared Redis-compatible byte layout.
 * Keeps existing Redis identities and keys; selecting this factory never migrates
 * data, discovers a different endpoint, or adds streaming/durability capabilities.
 */
public final class ValkeyBlobStoreProvider implements BlobStoreProvider {
    private final RedisBlobStoreProvider delegate = new RedisBlobStoreProvider();

    @Override public String id() { return "valkey"; }
    @Override public String identityProviderId() { return delegate.identityProviderId(); }
    @Override public BackendIdentity managedIdentity(Map<String, String> options) {
        return delegate.managedIdentity(options);
    }
    @Override public OpenedBlobStore open(Map<String, String> options) {
        return delegate.open(options);
    }
}
