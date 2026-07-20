/**
 * Rule source for the protovalidate ({@code buf.validate}) annotation dialect.
 *
 * <p>{@link ai.pipestream.proto.validate.protovalidate.ProtovalidateRuleSource} reads
 * {@code (buf.validate.field)} and {@code (buf.validate.message)} options off descriptors and
 * translates them into the neutral {@link ai.pipestream.proto.validate.model.FieldConstraints} and
 * {@link ai.pipestream.proto.validate.model.MessageConstraints} model, so schemas annotated for
 * protovalidate validate through {@link ai.pipestream.proto.validate.ProtoValidator} unchanged.
 * Coverage includes the scalar and collection rule families, {@code Any} and {@code FieldMask}
 * rules, message {@code oneof} rules, the ignore modes, custom CEL, and predefined-rule
 * extensions. Rules are recovered even from descriptors linked without the {@code buf.validate}
 * extension registry.
 *
 * <p>The class implements {@link ai.pipestream.proto.validate.spi.ValidationRuleSource} and is
 * registered through {@link java.util.ServiceLoader}, so putting this module on the classpath
 * enables the dialect and dropping the dependency removes it. The
 * {@code buf/validate/validate.proto} schema is vendored verbatim at a pinned version, under its
 * original {@code build.buf.validate} package, and attributed in this module's {@code NOTICE}.
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/validation.md">
 * validation guide</a> for the interoperability notes and conformance results.
 */
package ai.pipestream.proto.validate.protovalidate;
