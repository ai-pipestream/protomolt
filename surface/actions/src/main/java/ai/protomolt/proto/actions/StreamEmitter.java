package ai.protomolt.proto.actions;

import com.google.protobuf.Message;

/** Sink for incrementally produced results of a {@link StreamingAction}. */
@FunctionalInterface
public interface StreamEmitter {

    /**
     * Accepts one result as it is produced.
     *
     * @param message a {@link ProtoAction#responseType()} message
     * @throws ActionException when the emission cannot be rendered for the front receiving it
     */
    void emit(Message message) throws ActionException;
}
