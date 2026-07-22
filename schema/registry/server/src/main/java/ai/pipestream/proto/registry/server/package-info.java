/**
 * HTTP server exposing a schema registry store over the Confluent subjects protocol.
 *
 * <p>{@link SchemaRegistryServer} is the entry point: it fronts any
 * {@link ai.pipestream.proto.registry.SchemaRegistryStore} with the subjects, schemas-by-id and
 * config endpoints that existing Schema Registry clients and serializers expect, plus a native
 * prefix serving a compiled {@code FileDescriptorSet} for a subject's latest schema and its
 * transitive references. {@link SchemaRegistryServerConfig} carries the bind address, path
 * prefixes, request body cap and optional shared-secret token.</p>
 *
 * <p>The server is built on the JDK {@link com.sun.net.httpserver.HttpServer} running on virtual
 * threads and adds no HTTP framework dependency. It holds no schema logic of its own: storage,
 * the registration pipeline and compatibility gating all belong to the store in
 * {@link ai.pipestream.proto.registry}, and errors surface as Confluent-style
 * {@code {error_code, message}} JSON.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/registry.md">registry
 * guide</a> for usage.</p>
 */
package ai.pipestream.proto.registry.server;
