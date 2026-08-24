package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.node.ObjectNode;

/** The {@link StreamEmitter} a {@link JsonStreamingAction} sees, taking result documents. */
@FunctionalInterface
public interface JsonStreamEmitter {

    /** Accepts one result document as it is produced. */
    void emit(ObjectNode node) throws ActionException;
}
