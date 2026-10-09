package ai.protomolt.proto.repo.blob.spi;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;

/** Factory discovery with explicit selection and no fallback backend. */
public final class BlobStores {
    private final Map<String, BlobStoreProvider> providers;

    private BlobStores(Map<String, BlobStoreProvider> providers) {
        this.providers = Map.copyOf(providers);
    }

    public static BlobStores discover() {
        return of(ServiceLoader.load(BlobStoreProvider.class));
    }

    public static BlobStores of(Iterable<? extends BlobStoreProvider> providers) {
        var registered = new LinkedHashMap<String, BlobStoreProvider>();
        for (var provider : Objects.requireNonNull(providers, "providers")) {
            Objects.requireNonNull(provider, "provider");
            String id = requireId(provider.id());
            if (registered.putIfAbsent(id, provider) != null) {
                throw new IllegalArgumentException("Duplicate byte storage provider: " + id);
            }
        }
        return new BlobStores(registered);
    }

    public Set<String> providerIds() { return providers.keySet(); }

    /** Obtain identity from the same installed factory used by open, without I/O. */
    public BackendIdentity managedIdentity(String id, Map<String, String> options) {
        var provider = providers.get(requireId(id));
        if (provider == null) throw new IllegalArgumentException("Byte storage provider is not installed: " + id);
        var identity = Objects.requireNonNull(provider.managedIdentity(Map.copyOf(options)),
                "Byte storage provider returned no identity");
        if (!id.equals(identity.provider()))
            throw new IllegalArgumentException("Selected provider returned a different backend identity provider");
        return identity;
    }

    public OpenedBlobStore open(String id, Map<String, String> options) {
        return open(id, options, Set.of());
    }

    /** Reject incompatible selections and release their lifetime before returning control. */
    public OpenedBlobStore open(String id, Map<String, String> options, Set<BlobCapability> required) {
        var requirements = Set.copyOf(required);
        var provider = providers.get(requireId(id));
        if (provider == null) throw new IllegalArgumentException("Byte storage provider is not installed: " + id);
        var opened = Objects.requireNonNull(provider.open(Map.copyOf(options)),
                "Byte storage provider returned no handle");
        if (!opened.capabilities().containsAll(requirements)) {
            var failure = new UnsupportedOperationException("Selected byte storage provider lacks required capabilities");
            try { opened.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
        return opened;
    }

    private static String requireId(String id) {
        if (id == null || !id.matches("[a-z][a-z0-9-]*")) {
            throw new IllegalArgumentException("Invalid byte storage provider ID");
        }
        return id;
    }
}
