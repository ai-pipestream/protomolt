package ai.pipestream.proto.actions;

import com.google.protobuf.Message;

/**
 * A {@link ProtoAction} whose results arrive incrementally: server-streaming gRPC calls,
 * long-running scans, pipeline runs. The unary {@link ProtoAction#execute} contract is
 * unchanged — fronts that collect (REST, MCP) use it as before; fronts that stream (ACP)
 * dispatch through {@link ActionCatalog#executeStreaming} and render each emission as it
 * arrives. Every emission is a {@link ProtoAction#responseType()} message, so a streaming
 * verb answers under the same contract as a unary one.
 */
public interface StreamingAction extends ProtoAction {

    /**
     * Executes the action, emitting results as they are produced.
     *
     * @param request a {@link ProtoAction#requestType()} message, already checked
     * @param context type resolution and JSON machinery shared across actions
     * @param emitter the sink for incremental results
     * @throws ActionException with a stable code on any failure before streaming starts;
     *         mid-stream failures are emitted as terminal status messages instead
     */
    void executeStreaming(Message request, ActionContext context, StreamEmitter emitter)
            throws ActionException;
}
