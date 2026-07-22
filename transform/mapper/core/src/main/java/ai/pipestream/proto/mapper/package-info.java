/**
 * Path-based field access and a text rule language for reshaping protobuf messages at runtime.
 *
 * <p>{@link ai.pipestream.proto.mapper.ProtoFieldMapper} is the entry point: it reads and writes
 * fields by dot-notation path and applies lists of text rules to a message builder.
 * {@link ai.pipestream.proto.mapper.ProtoFieldMapperImpl} is the descriptor-driven implementation,
 * built over a {@link ai.pipestream.proto.descriptors.DescriptorRegistry} so dynamic messages,
 * {@code google.protobuf.Struct} values, and {@code google.protobuf.Any} payloads resolve without
 * generated classes. Paths that cannot be resolved raise
 * {@link ai.pipestream.proto.mapper.MappingException}.</p>
 *
 * <p>The rule language covers assignment, append, and clear;
 * {@link ai.pipestream.proto.mapper.TextRuleParser} parses rule text into
 * {@link ai.pipestream.proto.mapper.TextMappingRule} records. Conditional mapping and computed
 * values are layered on top by {@code ai.pipestream.proto.cel}, which wraps this package with CEL
 * filters and selectors. Mapping is separate from validation: reshaping a message and judging it
 * are distinct concerns.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/mapping.md">Field
 * mapping guide</a> for the rule syntax and worked examples.</p>
 */
package ai.pipestream.proto.mapper;
