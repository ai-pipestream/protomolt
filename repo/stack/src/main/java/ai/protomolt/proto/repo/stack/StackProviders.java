package ai.protomolt.proto.repo.stack;

import ai.protomolt.proto.repo.v2.StackRecord;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;

/** Installed stack providers, selected explicitly by name with no fallback. */
public final class StackProviders {
    private final Map<String, StackProvider> providers;

    private StackProviders(Map<String, StackProvider> providers) { this.providers = Map.copyOf(providers); }

    /** Providers registered under {@code META-INF/services}. */
    public static StackProviders discover() { return of(ServiceLoader.load(StackProvider.class)); }

    public static StackProviders of(Iterable<? extends StackProvider> providers) {
        var registered = new LinkedHashMap<String, StackProvider>();
        for (var provider : Objects.requireNonNull(providers, "providers")) {
            var id = Objects.requireNonNull(provider, "provider").id();
            if (id == null || !id.matches("[a-z][a-z0-9-]*")) throw new IllegalArgumentException("Invalid stack provider ID");
            if (registered.putIfAbsent(id, provider) != null)
                throw new IllegalArgumentException("Duplicate stack provider: " + id);
        }
        return new StackProviders(registered);
    }

    public Set<String> ids() { return providers.keySet(); }

    /** Opens the stack the record names with the provider the record names. */
    public Stack open(StackRecord record, Map<String, String> options) throws IOException {
        var provider = providers.get(record.getProvider());
        if (provider == null) throw new IllegalArgumentException("Stack provider is not installed: " + record.getProvider());
        var stack = Objects.requireNonNull(provider.open(record, Map.copyOf(options)), "Stack provider returned no stack");
        if (!stack.stackId().equals(record.getStackId())) {
            var failure = new IllegalStateException("Stack provider opened another stack ID");
            try { stack.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
        return stack;
    }
}
