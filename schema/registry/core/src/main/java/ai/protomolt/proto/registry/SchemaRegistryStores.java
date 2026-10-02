package ai.protomolt.proto.registry;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;

/** Installed store providers, with explicit selection and no fallback backend. */
public final class SchemaRegistryStores {
    private final Map<String, SchemaRegistryStoreProvider> providers;

    private SchemaRegistryStores(Map<String, SchemaRegistryStoreProvider> providers) {
        this.providers = Map.copyOf(providers);
    }

    /** Discovers installed provider factories without opening a store. */
    public static SchemaRegistryStores discover() {
        return of(ServiceLoader.load(SchemaRegistryStoreProvider.class));
    }

    /** Registers explicitly supplied providers; duplicate or malformed IDs fail. */
    public static SchemaRegistryStores of(Iterable<? extends SchemaRegistryStoreProvider> providers) {
        var registered = new LinkedHashMap<String, SchemaRegistryStoreProvider>();
        for (var provider : Objects.requireNonNull(providers, "providers")) {
            Objects.requireNonNull(provider, "provider");
            String id = requireId(provider.id());
            if (registered.putIfAbsent(id, provider) != null) {
                throw new IllegalArgumentException("Duplicate registry storage provider: " + id);
            }
        }
        return new SchemaRegistryStores(registered);
    }

    /** Installed provider IDs. This does not establish storage readiness. */
    public Set<String> providerIds() {
        return providers.keySet();
    }

    /** Opens only the named provider, forwarding the caller's gate unchanged. */
    public SchemaRegistryStore open(String id, Map<String, String> options,
            SchemaRegistryStore.WriteGate writeGate) {
        requireId(id);
        var provider = providers.get(id);
        if (provider == null) {
            throw new IllegalArgumentException("Registry storage provider is not installed: " + id);
        }
        return Objects.requireNonNull(provider.open(Map.copyOf(options), writeGate),
                "Registry storage provider " + id + " returned null");
    }

    private static String requireId(String id) {
        if (id == null || !id.matches("[a-z][a-z0-9-]*")) {
            throw new IllegalArgumentException("Invalid registry storage provider ID: " + id);
        }
        return id;
    }
}
