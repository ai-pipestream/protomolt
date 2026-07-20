package ai.pipestream.proto.connector;

import com.google.protobuf.Message;

/**
 * A push-style streaming input: open it, and messages arrive on a {@link Listener} until
 * the stream ends, fails, or is closed. The consumer never pulls and never blocks waiting
 * on the source; the only flow control is {@link Handle#pause()} / {@link Handle#resume()},
 * which a bounded buffer such as {@link SourcePump} uses to keep a fast source from
 * outrunning a slow pipeline.
 *
 * <p>Threading: {@code onMessage} calls are sequential and ordered per open stream, and
 * exactly one terminal signal ({@code onComplete} or {@code onError}) fires. There is no
 * cursor and no pull method; offset and resume-token ownership stays in the deployment
 * layer, not in this interface. {@code pause}, {@code resume}, and {@code close} may be
 * called from any thread.</p>
 *
 * @param <P> the plan type this source opens from
 */
public interface StreamSource<P extends SourcePlan> {

    /** A short stable name for the source kind ("grpc", "kafka"), for plan registries. */
    String name();

    /**
     * Opens the stream described by {@code plan} and starts pushing to {@code listener}.
     * Returns immediately; the first message may arrive before the caller has seen the
     * returned handle, so attach the handle to a pump (or otherwise retain it) right away.
     */
    Handle open(P plan, Listener listener);

    /** The consumer side: ordered messages, then exactly one terminal signal. */
    interface Listener {

        /**
         * The next message. Called sequentially from one source thread. Implementations
         * must not throw; a throwing listener kills the stream without a terminal signal.
         */
        void onMessage(Message message);

        /** The stream ended normally. Never fires after {@link #onError}. */
        default void onComplete() {
        }

        /** The stream failed. Never fires after {@link #onComplete}. */
        default void onError(Throwable error) {
        }
    }

    /** Flow control and shutdown for one open stream. */
    interface Handle extends AutoCloseable {

        /**
         * Asks the source to stop producing. Not a wall: messages already in flight still
         * arrive, so the listener must tolerate a bounded overshoot.
         */
        void pause();

        /** Resumes production after {@link #pause()}. */
        void resume();

        /** Stops the stream and releases it. Idempotent; safe from any thread. */
        @Override
        void close();
    }
}
