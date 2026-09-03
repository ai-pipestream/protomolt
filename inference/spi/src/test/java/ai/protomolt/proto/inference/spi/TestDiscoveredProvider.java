package ai.protomolt.proto.inference.spi;

import ai.protomolt.proto.inference.v1.GenerateRequest;
import ai.protomolt.proto.inference.v1.GenerateResponse;
import ai.protomolt.proto.inference.v1.GenerateStreamRequest;
import ai.protomolt.proto.inference.v1.ModelEntry;

/**
 * A provider that only exists so the ServiceLoader discovery test has
 * something to find (registered in test META-INF/services).
 */
public final class TestDiscoveredProvider implements InferenceProvider {

    @Override
    public String id() {
        return "test-discovered";
    }

    @Override
    public GenerateResponse generate(ModelEntry model, GenerateRequest request) {
        throw new InferenceException("test-discovered does not generate");
    }

    @Override
    public void generateStream(ModelEntry model, GenerateStreamRequest request,
                               ChunkObserver observer) {
        throw new InferenceException("test-discovered does not stream");
    }
}
