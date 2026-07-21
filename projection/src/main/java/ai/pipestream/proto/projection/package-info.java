/**
 * Self-describing message-to-message projections driven by descriptor options on the target
 * schema.
 *
 * <p>A projection lives on the target message itself: the target's {@code .proto} declares its
 * source types and, per field, where the value comes from — candidate source paths, a CEL
 * expression, or a literal. The result is compliant proto that any {@code protoc} can parse and
 * that executes at runtime from descriptors alone, with no generated classes required.</p>
 *
 * <p>{@link ai.pipestream.proto.projection.MessageProjection} is the entry point: it compiles a
 * target descriptor into an immutable, thread-safe projection, projects source messages onto it,
 * and derives {@link com.google.protobuf.FieldMask}s for the populated target fields and for what
 * each source type must supply. Descriptors read from a runtime descriptor set must be parsed
 * with the extensions registered by
 * {@link ai.pipestream.proto.projection.MessageProjection#registerExtensions(com.google.protobuf.ExtensionRegistry)}.
 * {@link ai.pipestream.proto.projection.SourceResolver} is the pluggable lookup from declared
 * source names to descriptors, used for eager validation when a projection is built; failures on
 * either path raise {@link ai.pipestream.proto.projection.ProjectionException}.</p>
 *
 * <p>Field access and expression evaluation come from
 * {@link ai.pipestream.proto.mapper.ProtoFieldMapper} and
 * {@link ai.pipestream.proto.cel.CelEvaluator}; this package adds the descriptor-option layer
 * that makes a mapping part of the schema rather than external configuration.</p>
 *
 * <p>See the
 * <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/projection.md">Projections
 * guide</a> for the option syntax and resolution semantics.</p>
 */
package ai.pipestream.proto.projection;
