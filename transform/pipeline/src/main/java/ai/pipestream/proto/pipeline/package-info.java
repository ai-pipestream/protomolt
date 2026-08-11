/**
 * The pipeline execution contract, offline static checker, and in-process executor.
 *
 * <p>A {@link ai.pipestream.proto.pipeline.v1.Pipeline} is the compiled, streaming-aware form
 * of a {@link ai.pipestream.proto.grpc.recipe.v1.GrpcRecipe}: every gRPC or structured step is
 * fed by an explicit typed edge (embedded unchanged from the recipe contract, so mapping, CEL,
 * projection, bounded fan-out, and branch failure policy keep exactly one meaning), every
 * method's streaming shape is declared, and every scope binding carries an explicit
 * cardinality. {@link RecipePipelineCompiler} performs the recipe-to-pipeline compilation;
 * {@link PipelineChecker} verifies a pipeline against a descriptor set without any network,
 * reflection call, or execution, rejecting cardinality and streaming-shape mismatches with
 * descriptor-precise messages.</p>
 *
 * <p>{@link PipelineExecutor} runs a checked pipeline through a host-owned
 * {@link PipelineTransport}. The transport resolves the persisted service-profile and endpoint
 * references to live channels, keeping raw targets and credential material outside the pipeline.
 * Unary, server-streaming, client-streaming, and bidi calls share the same typed dataflow;
 * materialized streams have an explicit message cap, and fan-out branches run on virtual threads
 * behind their declared concurrency semaphore.</p>
 *
 * <p>Cardinality discipline: the pipeline input binds {@code input} at cardinality ONE. An
 * edge whose declared sources are all ONE produces one message; an edge reading a MANY
 * binding is a stream edge whose rules apply per element. A unary or server-streaming
 * request slot takes ONE, a client-streaming or bidi slot takes MANY, and a server-streaming
 * or bidi response binds MANY. Unnest steps turn a repeated field of a ONE binding into a
 * stream; collect steps collapse a stream into one message's repeated field. There is no
 * implicit per-element invocation anywhere.</p>
 */
package ai.pipestream.proto.pipeline;
