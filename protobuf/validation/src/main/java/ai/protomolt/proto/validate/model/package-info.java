/**
 * The neutral constraint model the validator evaluates, independent of any annotation dialect.
 *
 * <p>{@link ai.protomolt.proto.validate.model.FieldConstraints} holds the rules for a single
 * field, delegating to a per-category record — {@link ai.protomolt.proto.validate.model.StringConstraints},
 * {@link ai.protomolt.proto.validate.model.IntegralConstraints},
 * {@link ai.protomolt.proto.validate.model.RepeatedConstraints} and the rest — selected by the
 * field's type. {@link ai.protomolt.proto.validate.model.MessageConstraints} holds the
 * message-level rules: CEL predicates, synthetic oneof rules, and required protobuf oneofs.
 * {@link ai.protomolt.proto.validate.model.CelConstraint} carries a single custom CEL predicate
 * on either level, and {@link ai.protomolt.proto.validate.model.IgnoreMode} states when a field's
 * rules are skipped.
 *
 * <p>These records are the contract between the two halves of the module: a
 * {@link ai.protomolt.proto.validate.spi.ValidationRuleSource} produces them from its own
 * descriptor options, and {@link ai.protomolt.proto.validate.ProtoValidator} consumes them without
 * reading any dialect directly. Rule ids emitted for these constraints are stable and align with
 * protovalidate's naming, so results interoperate across dialects.
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/transform/validation.md">
 * validation guide</a> for the rule surface each record covers.
 */
package ai.protomolt.proto.validate.model;
