/**
 * The action catalog exposed as an Agent Client Protocol agent.
 *
 * <p>{@link ProtoMoltAcpAgent} is the entry point. An ACP-capable IDE launches it as a process
 * and drives it over stdio; each session behaves as a console where a prompt of the form
 * {@code <verb> <json>} runs one entry of an
 * {@link ai.pipestream.proto.actions.ActionCatalog} and the JSON result streams back as
 * message chunks. The agent declares no file, terminal, or permission capabilities. Its
 * {@code buildAgent} factory takes the transport and catalog as arguments, so tests drive the
 * agent in memory while {@code main} only wires the process streams.</p>
 *
 * <p>{@code ai.pipestream.proto.mcp} serves the same catalog to agents over the Model Context
 * Protocol, and {@code ai.pipestream.proto.cli} over a terminal; all three share the line
 * contract that a failing verb prints its error and leaves the session running.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/acp.md">ACP
 * agent guide</a>.</p>
 */
package ai.pipestream.proto.acp;
