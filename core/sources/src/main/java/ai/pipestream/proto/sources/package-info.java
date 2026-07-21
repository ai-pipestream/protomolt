/**
 * The {@code .proto} source-set model and the compiler that turns it into runtime descriptors.
 *
 * <p>{@link ProtoSource} is one {@code .proto} file as text keyed by its import
 * path, and {@link ProtoSourceSet} is an immutable, insertion-ordered set of
 * them — the unit of work shared by the gather modules (which produce one), the
 * compiler here, and the publishers in
 * {@link ai.pipestream.proto.sources.publish}. {@link ProtoImports} extracts
 * import statements from source text without a full parse, for dependency
 * ordering and reachability.</p>
 *
 * <p>{@link ProtoSourceCompiler} compiles a set using Square Wire's schema
 * library rather than a {@code protoc} binary, staging sources on an in-memory
 * filesystem. It returns {@link CompiledProtos}, which carries both the encoded
 * {@code FileDescriptorSet} and the linked runtime descriptors indexed by
 * import path; failures surface as {@link ProtoCompilationException}. This is
 * the single compilation pipeline behind every text-based descriptor source.</p>
 *
 * <p>Descriptors produced here are consumed through
 * {@link ai.pipestream.proto.descriptors.DescriptorRegistry} like any other.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/gathering.md">Gathering
 * proto sources guide</a>.</p>
 */
package ai.pipestream.proto.sources;
