package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.node.ObjectNode;

/** Sink for incrementally produced results of a {@link StreamingAction}. */
@FunctionalInterface
public interface StreamEmitter {

    /**
     * Accepts one result document as it is produced.
     *
     * @param node the result fragment; shape is defined by the emitting action
     */
    void emit(ObjectNode node);
}
