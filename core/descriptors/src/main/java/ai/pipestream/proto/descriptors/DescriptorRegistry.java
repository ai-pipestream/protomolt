package ai.pipestream.proto.descriptors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Registry for managing Protocol Buffer descriptors.
 * Provides lookup capabilities for descriptors by type name and caching.
 * Supports loading descriptors from various sources via DescriptorLoader implementations.
 */
public class DescriptorRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(DescriptorRegistry.class);

    /** Bound on the negative-lookup cache; when reached, the cache is reset rather than grown. */
    private static final int MAX_KNOWN_MISSING_TYPES = 1024;

    private final Map<String, Descriptor> descriptorsByFullName = new ConcurrentHashMap<>();
    private final Map<String, Descriptor> descriptorsBySimpleName = new ConcurrentHashMap<>();
    private final List<DescriptorLoader> manualLoaders = new CopyOnWriteArrayList<>();

    /**
     * Full type names that on-demand resolution already failed to find. Without this, every
     * {@link #findDescriptorByFullName} miss would re-consult (and typically re-parse) every
     * loader. Cleared by {@link #clear()} and {@link #addLoader(DescriptorLoader)}.
     */
    private final Set<String> knownMissingTypes = ConcurrentHashMap.newKeySet();

    private volatile boolean autoLoadAttempted = false;

    /**
     * Creates a new DescriptorRegistry and registers well-known types.
     */
    public DescriptorRegistry() {
        registerWellKnownTypes();
    }

    /**
     * Creates a new DescriptorRegistry.
     *
     * @return a new registry instance
     */
    public static DescriptorRegistry create() {
        return new DescriptorRegistry();
    }

    /**
     * Creates a new DescriptorRegistry with optional auto-loading.
     *
     * @param autoLoad whether to automatically load descriptors from available loaders
     * @return a new registry instance
     */
    public static DescriptorRegistry create(boolean autoLoad) {
        DescriptorRegistry registry = new DescriptorRegistry();
        if (autoLoad) {
            registry.autoLoadDescriptors();
        }
        return registry;
    }

    /**
     * Creates a new DescriptorRegistry with optional auto-loading.
     *
     * @param autoLoad if true, automatically load descriptors from all available loaders
     */
    public DescriptorRegistry(boolean autoLoad) {
        registerWellKnownTypes();
        if (autoLoad) {
            autoLoadDescriptors();
        }
    }

    /**
     * Registers well-known Google protobuf types.
     */
    private void registerWellKnownTypes() {
        try {
            register(com.google.protobuf.Struct.getDescriptor());
            register(com.google.protobuf.Value.getDescriptor());
            register(com.google.protobuf.ListValue.getDescriptor());
            register(com.google.protobuf.Timestamp.getDescriptor());
            register(com.google.protobuf.Duration.getDescriptor());
            register(com.google.protobuf.Any.getDescriptor());
            register(com.google.protobuf.Empty.getDescriptor());
        } catch (Exception e) {
            LOG.warn("Failed to register some well-known protobuf types", e);
        }
    }

    /**
     * Registers a descriptor in the registry.
     *
     * <p>Full-name registrations always win (last write). For simple-name lookups, the FIRST
     * registration of a given simple name wins: registering a different type with the same simple
     * name later logs a WARN and leaves the original mapping in place, so
     * {@link #findDescriptorBySimpleName(String)} stays deterministic. Use full names to
     * disambiguate colliding types.
     *
     * @param descriptor The descriptor to register
     */
    public void register(Descriptor descriptor) {
        descriptorsByFullName.put(descriptor.getFullName(), descriptor);
        Descriptor existing = descriptorsBySimpleName.putIfAbsent(descriptor.getName(), descriptor);
        if (existing != null) {
            if (existing.getFullName().equals(descriptor.getFullName())) {
                // Same type re-registered (possibly a rebuilt descriptor instance): keep current.
                descriptorsBySimpleName.put(descriptor.getName(), descriptor);
            } else {
                LOG.warn("Simple name collision for '{}': keeping first registration {} and ignoring {}; "
                        + "use findDescriptorByFullName to disambiguate",
                    descriptor.getName(), existing.getFullName(), descriptor.getFullName());
            }
        }
        knownMissingTypes.remove(descriptor.getFullName());
    }

    /**
     * Registers all message types from a file descriptor.
     *
     * @param fileDescriptor The file descriptor to register
     */
    public void registerFile(FileDescriptor fileDescriptor) {
        for (Descriptor messageType : fileDescriptor.getMessageTypes()) {
            register(messageType);
            registerNestedTypes(messageType);
        }
    }

    private void registerNestedTypes(Descriptor descriptor) {
        for (Descriptor nested : descriptor.getNestedTypes()) {
            register(nested);
            registerNestedTypes(nested);
        }
    }

    /**
     * Registers a descriptor from a message instance.
     *
     * @param message The message whose descriptor should be registered
     */
    public void registerFromMessage(Message message) {
        register(message.getDescriptorForType());
    }

    /**
     * Finds a descriptor by its full name.
     *
     * @param fullName The full name (e.g., "ai.pipestream.data.v1.SearchMetadata")
     * @return The descriptor, or null if not found
     */
    public Descriptor findDescriptorByFullName(String fullName) {
        Descriptor d = descriptorsByFullName.get(fullName);
        if (d == null) {
            // Try to resolve on-demand
            d = resolveOnDemand(fullName);
        }
        return d;
    }

    private Descriptor resolveOnDemand(String typeName) {
        if (knownMissingTypes.contains(typeName)) {
            return null;
        }

        List<DescriptorLoader> allLoaders = getLoaders();
        for (DescriptorLoader loader : allLoaders) {
            if (loader.isAvailable()) {
                try {
                    // Ask for the file DEFINING this type; loadDescriptor(name) would treat the
                    // type name as a proto file name and never match.
                    FileDescriptor fd = loader.loadDescriptorForType(typeName);
                    if (fd != null) {
                        registerFile(fd);
                        // Look up directly (non-recursive): re-entering resolution for the same
                        // name would loop forever when the loaded file lacks the requested type.
                        Descriptor resolved = descriptorsByFullName.get(typeName);
                        if (resolved != null) {
                            return resolved;
                        }
                    }
                } catch (DescriptorLoader.DescriptorLoadException e) {
                    // Ignore and try next loader
                }
            }
        }

        // Negative-cache the miss so repeated lookups do not re-consult every loader.
        if (knownMissingTypes.size() >= MAX_KNOWN_MISSING_TYPES) {
            knownMissingTypes.clear();
        }
        knownMissingTypes.add(typeName);
        return null;
    }

    private List<DescriptorLoader> getLoaders() {
        return new ArrayList<>(manualLoaders);
    }

    /**
     * Finds a descriptor by its simple name.
     */
    public Descriptor findDescriptorBySimpleName(String simpleName) {
        Descriptor d = descriptorsBySimpleName.get(simpleName);
        if (d == null) {
            autoLoadDescriptors();
            d = descriptorsBySimpleName.get(simpleName);
        }
        return d;
    }

    /**
     * Finds a descriptor by either full or simple name.
     */
    public Descriptor findDescriptor(String name) {
        Descriptor descriptor = findDescriptorByFullName(name);
        if (descriptor == null) {
            descriptor = findDescriptorBySimpleName(name);
        }
        return descriptor;
    }

    /**
     * Checks if a descriptor is registered.
     */
    public boolean isRegistered(String fullName) {
        return descriptorsByFullName.containsKey(fullName);
    }

    /**
     * Returns the number of registered descriptors (by full name).
     */
    public int size() {
        return descriptorsByFullName.size();
    }

    /**
     * Returns a snapshot of currently registered message descriptors (by full name).
     * Useful for building {@code JsonFormat.TypeRegistry} and OpenAPI schemas.
     */
    public List<Descriptor> registeredDescriptors() {
        return List.copyOf(descriptorsByFullName.values());
    }

    /**
     * Loads descriptors from a specific loader and registers them.
     *
     * @param loader the loader to load from
     * @return the number of message types registered
     * @throws DescriptorLoader.DescriptorLoadException if loading fails
     */
    public int loadFrom(DescriptorLoader loader) throws DescriptorLoader.DescriptorLoadException {
        List<FileDescriptor> fileDescriptors = loader.loadDescriptors();
        int count = 0;
        for (FileDescriptor fd : fileDescriptors) {
            registerFile(fd);
            count += fd.getMessageTypes().size();
        }
        return count;
    }

    /**
     * Clears all registered descriptors except well-known types.
     * Also resets the auto-load and negative-lookup state so descriptors can be reloaded.
     */
    public void clear() {
        descriptorsByFullName.clear();
        descriptorsBySimpleName.clear();
        knownMissingTypes.clear();
        // Allow auto-loading to run again so cleared descriptors are reloadable.
        autoLoadAttempted = false;
        registerWellKnownTypes();
    }

    /**
     * Adds a manual descriptor loader.
     */
    public void addLoader(DescriptorLoader loader) {
        if (loader != null) {
            manualLoaders.add(loader);
            // Allow the next auto-load to pick up the new loader, and retry lookups that
            // previously missed since this loader may supply them.
            autoLoadAttempted = false;
            knownMissingTypes.clear();
        }
    }

    /**
     * Loads descriptors from available loaders.
     */
    public synchronized void autoLoadDescriptors() {
        if (autoLoadAttempted) {
            return;
        }
        autoLoadAttempted = true;

        List<DescriptorLoader> allLoaders = new ArrayList<>(manualLoaders);

        for (DescriptorLoader loader : allLoaders) {
            if (loader.isAvailable()) {
                try {
                    List<FileDescriptor> fileDescriptors = loader.loadDescriptors();
                    int count = 0;
                    for (FileDescriptor fd : fileDescriptors) {
                        registerFile(fd);
                        count += fd.getMessageTypes().size();
                    }
                    LOG.info("Loaded {} descriptors from {}", count, loader.getLoaderType());
                } catch (DescriptorLoader.DescriptorLoadException e) {
                    LOG.warn("Failed to load descriptors from {}: {}", loader.getLoaderType(), e.getMessage());
                }
            }
        }
    }

    /**
     * Creates a new builder for DescriptorRegistry.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Fluent builder for constructing a DescriptorRegistry with pre-configured loaders and descriptors.
     */
    public static class Builder {
        private final List<Descriptor> descriptors = new ArrayList<>();
        private final List<FileDescriptor> fileDescriptors = new ArrayList<>();
        private final List<Message> messages = new ArrayList<>();
        private final List<DescriptorLoader> loaders = new ArrayList<>();
        private boolean autoLoad = false;

        /**
         * Registers a descriptor.
         */
        public Builder register(Descriptor descriptor) {
            descriptors.add(descriptor);
            return this;
        }

        /**
         * Registers all message types from a file descriptor.
         */
        public Builder registerFile(FileDescriptor fileDescriptor) {
            fileDescriptors.add(fileDescriptor);
            return this;
        }

        /**
         * Registers a descriptor from a message instance.
         */
        public Builder registerFromMessage(Message message) {
            messages.add(message);
            return this;
        }

        /**
         * Adds a GoogleDescriptorLoader with the default path.
         */
        public Builder withGoogleDescriptorLoader() {
            loaders.add(new GoogleDescriptorLoader());
            return this;
        }

        /**
         * Adds a GoogleDescriptorLoader with a custom path.
         */
        public Builder withGoogleDescriptorLoader(String descriptorPath) {
            loaders.add(new GoogleDescriptorLoader(descriptorPath));
            return this;
        }

        /**
         * Enables auto-loading of descriptors from all loaders on build.
         */
        public Builder withAutoLoad() {
            this.autoLoad = true;
            return this;
        }

        /**
         * Builds the DescriptorRegistry.
         */
        public DescriptorRegistry build() {
            DescriptorRegistry registry = new DescriptorRegistry();
            for (Descriptor d : descriptors) {
                registry.register(d);
            }
            for (FileDescriptor fd : fileDescriptors) {
                registry.registerFile(fd);
            }
            for (Message m : messages) {
                registry.registerFromMessage(m);
            }
            for (DescriptorLoader loader : loaders) {
                registry.addLoader(loader);
            }
            if (autoLoad) {
                registry.autoLoadDescriptors();
            }
            return registry;
        }
    }
}
