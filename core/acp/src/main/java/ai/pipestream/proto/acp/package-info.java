/**
 * A first-party Agent Client Protocol implementation: newline-delimited JSON-RPC 2.0 over a
 * pair of byte streams (stdio in production, one message per line, no Content-Length headers),
 * on virtual threads, with Jackson for the JSON and no reactive runtime. Everything is plain
 * blocking Java over {@link java.io.InputStream}/{@link java.io.OutputStream}, so the library
 * stays GraalVM-friendly and zero-reflection.
 *
 * <p>{@link ai.pipestream.proto.acp.AcpAgent} is the agent runtime: it answers
 * {@code initialize}, {@code session/new}, and {@code session/prompt}, delegating each prompt
 * turn to the {@link ai.pipestream.proto.acp.PromptHandler} it was built with and streaming the
 * handler's output back as {@code session/update} chunks through
 * {@link ai.pipestream.proto.acp.PromptContext}. {@link ai.pipestream.proto.acp.AcpClient} is
 * the small blocking client that drives an agent the way an IDE does, over in-memory pipes or
 * a launched child process. Protocol failures surface as
 * {@link ai.pipestream.proto.acp.AcpError}.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/acp.md">ACP
 * guide</a>.</p>
 */
package ai.pipestream.proto.acp;
