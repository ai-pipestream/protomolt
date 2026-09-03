package ai.protomolt.proto.inference.openai;

import ai.protomolt.proto.inference.openvino.OpenAiChatTransport;
import ai.protomolt.proto.inference.spi.ChunkObserver;
import ai.protomolt.proto.inference.spi.CredentialResolver;
import ai.protomolt.proto.inference.spi.InferenceProvider;
import ai.protomolt.proto.inference.v1.GenerateRequest;
import ai.protomolt.proto.inference.v1.GenerateResponse;
import ai.protomolt.proto.inference.v1.GenerateStreamRequest;
import ai.protomolt.proto.inference.v1.ModelEntry;

import java.time.Duration;

/**
 * The OpenAI-compatible provider: the {@code openai} profile of the shared
 * chat transport, pointed at the {@code /v1} surface that Ollama, vLLM, and
 * llama.cpp's server expose. This is the NVIDIA lane (Ollama on CUDA) and
 * the edge-box lane (llama.cpp on Jetson, Raspberry Pi) — one provider id
 * with honest provenance: responses name {@code openai}, not the transport
 * they share with OpenVINO.
 *
 * <p>Instances are stateless and thread-safe; the work lives in
 * {@link OpenAiChatTransport}.</p>
 */
public final class OpenAiCompatProvider implements InferenceProvider {

    /** Provider id catalog entries reference: {@value}. */
    public static final String ID = "openai";

    private final OpenAiChatTransport transport;

    /** Creates the provider with a 15-minute request timeout (long prefills). */
    public OpenAiCompatProvider() {
        this(Duration.ofMinutes(15));
    }

    /**
     * Creates the provider with an explicit per-request timeout.
     *
     * @param requestTimeout the timeout for one generation call
     */
    public OpenAiCompatProvider(Duration requestTimeout) {
        this.transport = new OpenAiChatTransport(ID, "/v1/chat/completions", requestTimeout);
    }

    /**
     * Creates the provider with an explicit per-request timeout and credential
     * resolver (tests inject a fake; production uses the environment
     * resolver via the other constructors).
     *
     * @param requestTimeout the timeout for one generation call
     * @param credentialResolver resolves catalog credential references to
     *     bearer material at request time
     */
    public OpenAiCompatProvider(Duration requestTimeout, CredentialResolver credentialResolver) {
        this.transport = new OpenAiChatTransport(ID, "/v1/chat/completions", requestTimeout,
                credentialResolver);
    }

    @Override
    public String id() {
        return ID;
    }

    @Override
    public GenerateResponse generate(ModelEntry model, GenerateRequest request) {
        return transport.generate(model, request);
    }

    @Override
    public void generateStream(ModelEntry model, GenerateStreamRequest request,
                               ChunkObserver observer) {
        transport.generateStream(model, request, observer);
    }
}
