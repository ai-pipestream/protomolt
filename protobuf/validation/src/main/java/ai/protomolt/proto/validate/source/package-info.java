/**
 * The built-in rule source for the Pipestream {@code validate.v1} annotation dialect.
 *
 * <p>{@link ai.protomolt.proto.validate.source.AiPipestreamRuleSource} reads the
 * {@code (ai.protomolt.proto.validate.v1.field)} and {@code (ai.protomolt.proto.validate.v1.message)}
 * options off protobuf descriptors and translates them into the neutral
 * {@link ai.protomolt.proto.validate.model.FieldConstraints} and
 * {@link ai.protomolt.proto.validate.model.MessageConstraints} model.
 *
 * <p>It is an ordinary implementation of
 * {@link ai.protomolt.proto.validate.spi.ValidationRuleSource} and holds no privileged position
 * in the validator; it is the source
 * {@link ai.protomolt.proto.validate.spi.ValidationRuleSources} places first in every chain.
 * Readers for other dialects live in their own modules and join the chain through
 * {@link java.util.ServiceLoader}.
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/transform/validation.md">
 * validation guide</a> for the annotation syntax this source reads.
 */
package ai.protomolt.proto.validate.source;
