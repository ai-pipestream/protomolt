/**
 * Joins, unions, and derived message shapes over several source messages at once.
 *
 * <p>{@link MessageScope} is the shared resolution scope: an ordered set of named messages
 * that scoped mapping rules and CEL expressions both read from. {@link ScopedProtoMapper}
 * applies the text rule dialect over that scope and {@link MessageJoiner} combines it with
 * CEL rules to build one target message. {@link ShapeSynthesizer} produces the target types
 * that have no authored {@code .proto} — envelopes, projections, and tagged unions linked
 * in-process and emitted as registerable proto source — while {@link SchemaMerger} performs
 * the schema-level merge of two or more message types, reporting field clashes before
 * anything is generated.</p>
 *
 * <p>{@link RuleChecker} statically validates both rule dialects against descriptors, so a
 * ruleset fails at configuration time rather than on a message; it is also what
 * {@code ai.pipestream.proto.chain} uses to verify a chain. {@link SchemaInferrer} works in
 * the other direction, deriving a message type from sample {@code Struct} data.</p>
 *
 * <p>The single-message counterparts of these rules live in
 * {@code ai.pipestream.proto.mapper} and {@code ai.pipestream.proto.cel}; this package adds
 * the named-source scope on top of them.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/design/join-shapes.md">join
 * shapes design note</a> for the rule syntax and the derived-shape kinds.</p>
 */
package ai.pipestream.proto.shapes;
