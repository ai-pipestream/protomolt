/**
 * The ProtoMolt command line over the action catalog.
 *
 * <p>{@link ProtoMoltCli} is the entry point. Every verb the servers expose over gRPC, REST,
 * and MCP is reachable here with the same JSON envelope in and out:
 * {@code protomolt <verb> <json>} runs one, {@code protomolt list} names them all, and
 * {@code protomolt console} opens an interactive session over the same
 * {@link ai.pipestream.proto.actions.ActionCatalog}. Dispatch takes its streams and catalog
 * as arguments so tests drive it directly, while {@code main} only wires the process streams
 * and the exit code.</p>
 *
 * <p>The catalog is assembled from the action modules rather than defined here, so this
 * package adds no verbs of its own; {@code ai.pipestream.proto.mcp} and
 * {@code ai.pipestream.proto.acp} serve the same catalog to agents.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/actions.md">action
 * catalog guide</a> for the verbs and their envelopes.</p>
 */
package ai.pipestream.proto.cli;
