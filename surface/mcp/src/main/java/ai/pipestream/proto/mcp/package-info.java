/**
 * A Model Context Protocol server over the action catalog and the schema registry.
 *
 * <p>{@link McpServer} is the server itself: every entry in an
 * {@link ai.pipestream.proto.actions.ActionCatalog} becomes an MCP tool without translation,
 * because the catalog manifest already carries the name, description, and JSON Schema input
 * the protocol asks for. Its {@code handle} method is a message-in, message-out core, so
 * tests and alternative transports drive it without streams. {@link RegistryResources}
 * optionally adapts a {@link ai.pipestream.proto.registry.SchemaRegistryStore} to MCP
 * resources, letting an agent browse subjects and read schema versions without spending tool
 * calls.</p>
 *
 * <p>{@link McpMain} is the stdio entry point, framing one JSON-RPC 2.0 message per line and
 * keeping diagnostics off stdout as the transport requires. Nothing here is framework-aware;
 * a Spring or Quarkus MCP host can register the same catalog through its own APIs.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/mcp.md">MCP
 * guide</a>; the verbs themselves are documented with the
 * <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/actions.md">action
 * catalog</a>.</p>
 */
package ai.pipestream.proto.mcp;
