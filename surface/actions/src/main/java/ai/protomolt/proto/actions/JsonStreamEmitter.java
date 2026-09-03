package ai.protomolt.proto.actions;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The emitter a JSON front hands {@link ActionCatalog#executeStreaming}: each emission is
 * rendered from the verb's response message into the document the front reads.
 */
@FunctionalInterface
public interface JsonStreamEmitter {

    /** Accepts one result document as it is produced. */
    void emit(ObjectNode node) throws ActionException;
}
