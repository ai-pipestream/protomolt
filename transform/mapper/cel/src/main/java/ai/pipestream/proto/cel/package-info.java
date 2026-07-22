/**
 * CEL compilation and evaluation against protobuf descriptors, and CEL-gated mapping rules.
 *
 * <p>{@link ai.pipestream.proto.cel.CelEnvironmentFactory} builds an environment from message
 * descriptors, variable declarations, and application-supplied function bindings.
 * {@link ai.pipestream.proto.cel.CelEvaluator} compiles and evaluates expressions in that
 * environment, caching both compiled programs and compile failures so a broken expression is
 * reported once rather than per message. Compile errors surface as
 * {@link ai.pipestream.proto.cel.CelCompilationException} and evaluation errors as
 * {@link ai.pipestream.proto.cel.CelEvaluationException}, so callers can tell a bad rule from a
 * bad message. {@link ai.pipestream.proto.cel.CelValidation} compiles an expression for
 * diagnostics alone, without evaluating it.</p>
 *
 * <p>{@link ai.pipestream.proto.cel.CelProtoMapper} applies
 * {@link ai.pipestream.proto.cel.CelMappingRule}s to a message builder, where each rule may carry
 * a boolean filter, a value selector, or a fallback to the text rules of
 * {@code ai.pipestream.proto.mapper}. Rules observe the progressive state of the builder, so
 * later expressions see earlier writes. This package layers on
 * {@link ai.pipestream.proto.mapper.ProtoFieldMapper} for the field access itself and is in turn
 * used by {@code ai.pipestream.proto.metadata} and {@code ai.pipestream.proto.projection}.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/mapping.md">Field
 * mapping guide</a> for filter and selector examples.</p>
 */
package ai.pipestream.proto.cel;
