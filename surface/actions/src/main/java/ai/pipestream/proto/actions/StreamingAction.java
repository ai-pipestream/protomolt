package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * A {@link ProtoAction} whose results arrive incrementally: server-streaming gRPC calls,
 * long-running scans, pipeline runs. The unary {@link #execute} contract is unchanged —
 * fronts that collect (REST, MCP) use it as before; fronts that stream (ACP) dispatch
 * through {@link ActionCatalog#executeStreaming} and render each emission as it arrives.
 */
public interface StreamingAction extends ProtoAction {

    /**
     * Executes the action, emitting result documents as they are produced.
     *
     * @param input   the input envelope; must satisfy {@link #inputSchema()}
     * @param context type resolution and JSON machinery shared across actions
     * @param emitter the sink for incremental results
     * @throws ActionException with a stable code on any failure before streaming starts;
     *         mid-stream failures are emitted as terminal status documents instead
     */
    void executeStreaming(ObjectNode input, ActionContext context, StreamEmitter emitter)
            throws ActionException;
}
