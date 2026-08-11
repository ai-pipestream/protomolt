package ai.pipestream.proto.inference.openvino;

import ai.pipestream.proto.inference.spi.ChunkObserver;
import ai.pipestream.proto.inference.spi.CredentialResolver;
import ai.pipestream.proto.inference.spi.InferenceProvider;
import ai.pipestream.proto.inference.v1.GenerateRequest;
import ai.pipestream.proto.inference.v1.GenerateResponse;
import ai.pipestream.proto.inference.v1.GenerateStreamRequest;
import ai.pipestream.proto.inference.v1.ModelEntry;

import java.time.Duration;

/**
 * The OpenVINO provider: the {@code openvino} profile of the shared
 * OpenAI-compatible transport, pointed at the {@code /v3} surface every
 * OpenVINO model server exposes.
 *
 * <p>Instances are stateless and thread-safe; the work lives in
 * {@link OpenAiChatTransport}.</p>
 */
public final class OpenVinoProvider implements InferenceProvider {

    /** Provider id catalog entries reference: {@value}. */
    public static final String ID = "openvino";

    private final OpenAiChatTransport transport;

    /** Creates the provider with a 15-minute request timeout (long prefills). */
    public OpenVinoProvider() {
        this(Duration.ofMinutes(15));
    }

    /**
     * Creates the provider with an explicit per-request timeout.
     *
     * @param requestTimeout the timeout for one generation call
     */
    public OpenVinoProvider(Duration requestTimeout) {
        this.transport = new OpenAiChatTransport(ID, "/v3/chat/completions", requestTimeout);
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
    public OpenVinoProvider(Duration requestTimeout, CredentialResolver credentialResolver) {
        this.transport = new OpenAiChatTransport(ID, "/v3/chat/completions", requestTimeout,
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
