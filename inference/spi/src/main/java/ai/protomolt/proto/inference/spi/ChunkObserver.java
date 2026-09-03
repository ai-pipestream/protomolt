package ai.protomolt.proto.inference.spi;

import ai.protomolt.proto.inference.v1.GenerateStreamResponse;

/**
 * The sink a provider pushes streamed generation chunks into.
 *
 * <p>Implementations must tolerate chunks arriving from a single provider
 * thread in order; providers must deliver {@code onNext} zero or more times,
 * then exactly one of {@code onComplete} or {@code onError}.</p>
 */
public interface ChunkObserver {

    /**
     * Receives one streamed chunk. The final chunk carries usage and finish
     * reason and is immediately followed by {@link #onComplete()}.
     *
     * @param chunk one incremental piece of the generation
     */
    void onNext(GenerateStreamResponse chunk);

    /** Called exactly once after the final chunk. */
    void onComplete();

    /**
     * Called exactly once when the stream fails.
     *
     * @param error the loud failure that ended the stream
     */
    void onError(InferenceException error);
}
