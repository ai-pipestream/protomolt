/**
 * Validates protobuf messages against constraint rules carried on their descriptors as options.
 *
 * <p>{@link ai.protomolt.proto.validate.ProtoValidator} is the entry point: it compiles the rules
 * for a message type once — regular expressions and CEL programs eagerly — and evaluates them
 * against message instances. The outcome is a
 * {@link ai.protomolt.proto.validate.ValidationResult}, whose violations name the field path, rule
 * path, rule id and message. A malformed rule set surfaces as
 * {@link ai.protomolt.proto.validate.RuleCompilationException} at construction; a rule that errors
 * against a particular value surfaces as
 * {@link ai.protomolt.proto.validate.RuleEvaluationException}.
 *
 * <p>The validator core is dialect-neutral. It evaluates the constraint records in
 * {@link ai.protomolt.proto.validate.model}, and the translation from a specific annotation
 * dialect onto that model lives behind
 * {@link ai.protomolt.proto.validate.spi.ValidationRuleSource}. The built-in reader for the
 * Pipestream {@code validate.v1} options is
 * {@link ai.protomolt.proto.validate.source.AiPipestreamRuleSource}; further dialects are
 * discovered on the classpath through {@link java.util.ServiceLoader}. Custom rules are evaluated
 * as CEL, with the format functions in {@link ai.protomolt.proto.validate.cel} registered into the
 * validation environment.
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/transform/validation.md">
 * validation guide</a> for the rule surface and its semantics.
 */
package ai.protomolt.proto.validate;
