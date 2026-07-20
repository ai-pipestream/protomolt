/**
 * Extension point for reading constraint-annotation dialects off protobuf descriptors.
 *
 * <p>{@link ai.pipestream.proto.validate.spi.ValidationRuleSource} is the interface a dialect
 * implements: it reads its own options off a field or message descriptor and returns the
 * equivalent {@link ai.pipestream.proto.validate.model.FieldConstraints} or
 * {@link ai.pipestream.proto.validate.model.MessageConstraints}. Implementations must be
 * thread-safe and side-effect free, since descriptors are shared.
 *
 * <p>{@link ai.pipestream.proto.validate.spi.ValidationRuleSources} assembles the chain a
 * validator uses. {@code defaults()} places the built-in
 * {@link ai.pipestream.proto.validate.source.AiPipestreamRuleSource} first and appends any
 * implementation found through {@link java.util.ServiceLoader}, so an optional dialect module
 * takes effect by being on the classpath; {@code pipestreamOnly()} pins the built-in reader.
 * Every source in the chain is consulted for every field and message and all violations are
 * merged.
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/validation.md">
 * validation guide</a> for how rule sources are configured.
 */
package ai.pipestream.proto.validate.spi;
