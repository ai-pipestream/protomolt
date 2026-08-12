/**
 * Persistent agent processes attached to the ProtoMolt delegation surface. The host keeps the
 * MCP cursor and provider session identity durable, validates every model response as a bounded
 * command batch, and performs long polls on virtual threads.
 */
package ai.pipestream.proto.agenthost;
