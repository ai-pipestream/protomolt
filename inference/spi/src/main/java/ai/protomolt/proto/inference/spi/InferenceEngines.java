package ai.protomolt.proto.inference.spi;

import ai.protomolt.proto.inference.v1.DescribeModelRequest;
import ai.protomolt.proto.inference.v1.DescribeModelResponse;
import ai.protomolt.proto.inference.v1.GenerateRequest;
import ai.protomolt.proto.inference.v1.GenerateResponse;
import ai.protomolt.proto.inference.v1.GenerateStreamRequest;
import ai.protomolt.proto.inference.v1.ListModelsRequest;
import ai.protomolt.proto.inference.v1.ListModelsResponse;
import ai.protomolt.proto.inference.v1.ModelEntry;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * The resolution facade every inference surface (the gRPC service, workflow
 * steps, jobs) shares: it maps a catalog model id to the entry and the
 * {@link InferenceProvider} that executes it.
 *
 * <p>Providers are discovered with {@link ServiceLoader} at construction;
 * registration through this facade rejects entries whose provider is not
 * loaded, so a typo'd provider id fails at registration time, not at first
 * request. Instances are thread-safe.</p>
 */
public final class InferenceEngines {

    private final InferenceCatalog catalog;
    private final Map<String, InferenceProvider> providers;

    /**
     * Creates the facade over the given catalog, discovering providers with
     * {@link ServiceLoader}.
     *
     * @param catalog the model catalog to resolve against
     * @throws InferenceException when two providers claim the same id
     */
    public InferenceEngines(InferenceCatalog catalog) {
        this(catalog, ServiceLoader.load(InferenceProvider.class));
    }

    /**
     * Creates the facade with an explicit provider set instead of
     * {@link ServiceLoader} discovery — the wiring path for tests and for
     * servers that construct providers programmatically.
     *
     * @param catalog the model catalog to resolve against
     * @param discovered the providers to use
     * @throws InferenceException when two providers claim the same id
     */
    public InferenceEngines(InferenceCatalog catalog, Iterable<InferenceProvider> discovered) {
        this.catalog = catalog;
        this.providers = new HashMap<>();
        for (InferenceProvider provider : discovered) {
            InferenceProvider clash = providers.putIfAbsent(provider.id(), provider);
            if (clash != null) {
                throw new InferenceException("provider id " + provider.id() + " claimed by both "
                        + clash.getClass().getName() + " and " + provider.getClass().getName());
            }
        }
    }

    /**
     * Registers a model entry after verifying its provider is loaded.
     *
     * @param entry the entry to register
     * @throws InferenceException on an incomplete entry, a duplicate id, or
     *     an unknown provider
     */
    public void register(ModelEntry entry) {
        if (!providers.containsKey(entry.getProvider())) {
            throw new InferenceException("catalog entry " + entry.getId() + " names provider "
                    + entry.getProvider() + ", which is not loaded (loaded: "
                    + providers.keySet().stream().sorted().toList() + ")");
        }
        catalog.register(entry);
    }

    /**
     * Resolves the request's model id and executes the generation.
     *
     * @param request the generate request
     * @return the completed generation
     * @throws InferenceException on an unknown model or a provider failure
     */
    public GenerateResponse generate(GenerateRequest request) {
        ModelEntry model = catalog.get(request.getModel());
        if (request.hasStructuredOutput()
                && !model.getCapabilities().getStructuredOutput()) {
            throw new InferenceException("model '" + model.getId()
                    + "' does not declare the structured-output capability");
        }
        return provider(model.getProvider()).generate(model, request);
    }

    /**
     * Resolves the request's model id and executes the streaming generation.
     *
     * @param request the stream request
     * @param observer the chunk sink
     * @throws InferenceException on an unknown model or a provider failure
     */
    public void generateStream(GenerateStreamRequest request, ChunkObserver observer) {
        ModelEntry model = catalog.get(request.getModel());
        provider(model.getProvider()).generateStream(model, request, observer);
    }

    /**
     * Lists catalog entries, optionally filtered by provider.
     *
     * @param request the list request
     * @return the entries and the catalog generation
     */
    public ListModelsResponse listModels(ListModelsRequest request) {
        return ListModelsResponse.newBuilder()
                .addAllModels(catalog.list(request.getProvider()))
                .setCatalogGeneration(catalog.generation())
                .build();
    }

    /**
     * Describes one catalog entry.
     *
     * @param request the describe request
     * @return the entry
     * @throws InferenceException when the id is not registered
     */
    public DescribeModelResponse describe(DescribeModelRequest request) {
        return DescribeModelResponse.newBuilder()
                .setEntry(catalog.get(request.getModel()))
                .build();
    }

    /**
     * The loaded provider ids, for diagnostics.
     *
     * @return provider ids by id
     */
    public Map<String, InferenceProvider> providers() {
        return Map.copyOf(providers);
    }

    private InferenceProvider provider(String id) {
        InferenceProvider provider = providers.get(id);
        if (provider == null) {
            // A registration through the bare catalog (not this facade) can
            // land an entry whose provider is absent; fail loud at use.
            throw new InferenceException("no loaded provider for id: " + id);
        }
        return provider;
    }
}
