/**
 * Harness that scores the validator against protovalidate's conformance suite.
 *
 * <p>{@link ai.pipestream.proto.validate.conformance.ConformanceRunner} is the shared core: it
 * runs {@link ai.pipestream.proto.validate.ProtoValidator} over a single case and expresses the
 * outcome the way the suite compares it, expanding each
 * {@link ai.pipestream.proto.validate.ValidationResult.Violation} into the structured field and
 * rule paths the runner checks with message equality.
 *
 * <p>{@link ai.pipestream.proto.validate.conformance.ConformanceMain} is the executor entry point
 * for buf's {@code protovalidate-conformance} binary: it reads one request from stdin, links the
 * descriptor set it carries, runs every case through the same runner, and writes the response to
 * stdout. The in-build JUnit harness drives that same runner, so the gated build and the
 * authoritative pass rate exercise identical code.
 *
 * <p>Rules in the cases come from the {@code buf.validate} dialect supplied by
 * {@link ai.pipestream.proto.validate.protovalidate.ProtovalidateRuleSource}, which this module
 * puts on the classpath.
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/validation.md">
 * validation guide</a> for how the two conformance modes are run.
 */
package ai.pipestream.proto.validate.conformance;
