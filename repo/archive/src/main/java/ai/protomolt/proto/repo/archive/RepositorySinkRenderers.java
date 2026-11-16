package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.emit.renderer.RendererProvider;
import ai.protomolt.proto.emit.renderer.RendererRegistry;
import ai.protomolt.proto.emit.v1.RendererCapability;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Empty;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * Immutable bundle of an enabled {@link RendererRegistry} and the protobuf options
 * {@link Descriptor}s of its enabled capabilities.
 */
public final class RepositorySinkRenderers {
    private static final RepositorySinkRenderers NONE =
            new RepositorySinkRenderers(new RendererRegistry(List.of(), Set.of()), Map.of());

    private final RendererRegistry registry;
    private final Map<String, Descriptor> optionsDescriptors;

    private RepositorySinkRenderers(
            RendererRegistry registry, Map<String, Descriptor> optionsDescriptors) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.optionsDescriptors = Map.copyOf(optionsDescriptors);
    }

    /** Returns an empty renderer set with no enabled capabilities. */
    public static RepositorySinkRenderers none() {
        return NONE;
    }

    /**
     * Builds a {@link RepositorySinkRenderers} from explicitly supplied {@link RendererProvider}s
     * and the set of enabled provider IDs.
     *
     * @param providers installed local or remote {@link RendererProvider} instances
     * @param enabledIds provider IDs enabled in this host
     * @return the configured renderer bundle
     */
    public static RepositorySinkRenderers of(
            Iterable<RendererProvider> providers, Set<String> enabledIds) {
        Objects.requireNonNull(providers, "providers");
        Objects.requireNonNull(enabledIds, "enabledIds");
        List<RendererProvider> collected = new ArrayList<>();
        Map<String, Descriptor> descriptors = new LinkedHashMap<>();
        for (RendererProvider provider : providers) {
            collected.add(provider);
            RendererCapability capability = Objects.requireNonNull(provider.capability());
            if (enabledIds.contains(capability.getProviderId())) {
                descriptors.put(
                        capability.getProviderId(),
                        Objects.requireNonNull(provider.optionsDescriptor()));
            }
        }
        return new RepositorySinkRenderers(new RendererRegistry(collected, enabledIds), descriptors);
    }

    /**
     * Discovers {@link RendererProvider}s via {@link ServiceLoader} on {@code loader} and enables
     * {@code enabledIds}.
     *
     * @param loader class loader searched by {@link ServiceLoader}
     * @param enabledIds provider IDs enabled in this host
     * @return the configured renderer bundle
     */
    public static RepositorySinkRenderers load(ClassLoader loader, Set<String> enabledIds) {
        Objects.requireNonNull(loader, "loader");
        return of(ServiceLoader.load(RendererProvider.class, loader), enabledIds);
    }

    /**
     * Wraps an already-constructed {@link RendererRegistry} with explicit options descriptors
     * keyed by {@code provider_id} (defaulting {@code google.protobuf.Empty} capabilities to
     * {@link Empty#getDescriptor()} when omitted).
     *
     * @param registry the enabled renderer registry
     * @param optionsDescriptors map from {@code provider_id} to options {@link Descriptor}
     * @return the configured renderer bundle
     */
    public static RepositorySinkRenderers of(
            RendererRegistry registry, Map<String, Descriptor> optionsDescriptors) {
        Objects.requireNonNull(registry, "registry");
        Objects.requireNonNull(optionsDescriptors, "optionsDescriptors");
        Map<String, Descriptor> resolved = new LinkedHashMap<>(optionsDescriptors);
        for (RendererCapability capability : registry.capabilities()) {
            if (!resolved.containsKey(capability.getProviderId())
                    && Empty.getDescriptor().getFullName().equals(capability.getOptionsType())) {
                resolved.put(capability.getProviderId(), Empty.getDescriptor());
            }
        }
        return new RepositorySinkRenderers(registry, resolved);
    }

    /** Returns the underlying {@link RendererRegistry}. */
    public RendererRegistry registry() {
        return registry;
    }

    /** Returns the options {@link Descriptor} registered for {@code providerId}, if known. */
    public Optional<Descriptor> optionsDescriptor(String providerId) {
        return Optional.ofNullable(optionsDescriptors.get(providerId));
    }
}
