package ai.protomolt.proto.inference.spi;

import ai.protomolt.proto.inference.v1.GenerateRequest;
import ai.protomolt.proto.inference.v1.GenerateResponse;
import ai.protomolt.proto.inference.v1.GenerateStreamRequest;
import ai.protomolt.proto.inference.v1.ModelEntry;

/**
 * The service-provider interface every inference backend plugs into:
 * OpenVINO model servers today, NVIDIA TensorRT/Triton and llama.cpp edge
 * boxes (Jetson, Raspberry Pi) as later providers.
 *
 * <p>Providers are discovered with {@link java.util.ServiceLoader}: an
 * implementation module lists its provider class in
 * {@code META-INF/services/ai.protomolt.proto.inference.spi.InferenceProvider}.
 * Implementations must be stateless and thread-safe — all per-model state
 * lives in the {@link ModelEntry} (endpoint, backend model name), so one
 * provider instance serves every catalog entry of its kind.</p>
 *
 * <p>Every failure is an {@link InferenceException}; providers never fall
 * back to another backend, another model, or a default endpoint.</p>
 */
public interface InferenceProvider {

    /**
     * The provider id catalog entries reference (e.g. {@code "openvino"}).
     * Must be stable across releases and unique across loaded providers.
     *
     * @return the provider id
     */
    String id();

    /**
     * Executes one unary generation against the model's backend.
     *
     * @param model the catalog entry (endpoint, backend model name)
     * @param request the typed request
     * @return the completed generation with provenance filled by the provider
     * @throws InferenceException on any transport or backend failure
     */
    GenerateResponse generate(ModelEntry model, GenerateRequest request);

    /**
     * Executes one streaming generation against the model's backend.
     *
     * @param model the catalog entry (endpoint, backend model name)
     * @param request the typed request
     * @param observer the chunk sink
     * @throws InferenceException on any transport or backend failure
     */
    void generateStream(ModelEntry model, GenerateStreamRequest request, ChunkObserver observer);
}
