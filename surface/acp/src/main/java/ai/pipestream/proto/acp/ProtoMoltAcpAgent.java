package ai.pipestream.proto.acp;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.grpc.service.ProtoMoltCatalog;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * The ProtoMolt action catalog as an Agent Client Protocol agent. An ACP-capable IDE
 * (JetBrains AI chat, Zed) launches this process and drives it over stdio: newline-delimited
 * JSON-RPC 2.0, one message per line, no Content-Length headers. Each session is a console
 * where a prompt of the form {@code <verb> <json>} runs the catalog verb and the JSON result
 * streams back as {@code session/update} message chunks. The agent declares no file, terminal,
 * or permission capabilities; it is read-only. {@code main} only wires the process streams;
 * {@link #buildAgent} takes the streams and catalog as arguments so tests drive the agent over
 * in-memory pipes.
 *
 * <p>The transport is first-party ({@link AcpConnection}): virtual threads, Jackson, no
 * reactive runtime.</p>
 */
public final class ProtoMoltAcpAgent {

    public static void main(String[] args) {
        buildAgent(System.in, System.out, ProtoMoltCatalog.full(ActionContext.create())).run();
    }

    /**
     * Builds the catalog agent over any pair of streams.
     *
     * @param in the stream client messages are read from (stdin in production)
     * @param out the stream responses and notifications are written to (stdout in production)
     * @param catalog the action catalog to expose
     * @return the agent, ready to {@code start()} or {@code run()}
     */
    public static AcpAgent buildAgent(InputStream in, OutputStream out, ActionCatalog catalog) {
        return AcpAgent.over(in, out, catalog);
    }

    private ProtoMoltAcpAgent() {
    }
}
