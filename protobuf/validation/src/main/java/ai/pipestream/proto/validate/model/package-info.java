/**
 * The neutral constraint model the validator evaluates, independent of any annotation dialect.
 *
 * <p>{@link ai.pipestream.proto.validate.model.FieldConstraints} holds the rules for a single
 * field, delegating to a per-category record — {@link ai.pipestream.proto.validate.model.StringConstraints},
 * {@link ai.pipestream.proto.validate.model.IntegralConstraints},
 * {@link ai.pipestream.proto.validate.model.RepeatedConstraints} and the rest — selected by the
 * field's type. {@link ai.pipestream.proto.validate.model.MessageConstraints} holds the
 * message-level rules: CEL predicates, synthetic oneof rules, and required protobuf oneofs.
 * {@link ai.pipestream.proto.validate.model.CelConstraint} carries a single custom CEL predicate
 * on either level, and {@link ai.pipestream.proto.validate.model.IgnoreMode} states when a field's
 * rules are skipped.
 *
 * <p>These records are the contract between the two halves of the module: a
 * {@link ai.pipestream.proto.validate.spi.ValidationRuleSource} produces them from its own
 * descriptor options, and {@link ai.pipestream.proto.validate.ProtoValidator} consumes them without
 * reading any dialect directly. Rule ids emitted for these constraints are stable and align with
 * protovalidate's naming, so results interoperate across dialects.
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/validation.md">
 * validation guide</a> for the rule surface each record covers.
 */
package ai.pipestream.proto.validate.model;
