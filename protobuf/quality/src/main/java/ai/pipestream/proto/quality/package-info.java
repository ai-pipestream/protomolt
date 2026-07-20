/**
 * Data quality scored from dimensions a message type declares on its own descriptor.
 *
 * <p>{@link QualityScorer} is the entry point: it reads the
 * {@code (ai.pipestream.proto.quality.v1.quality)} message option — a list of
 * {@link QualityDimension} entries, each a CEL expression over {@code this} returning a score —
 * compiles them once per message type, and returns a {@link QualityReport} holding the score per
 * dimension and their weighted composite. Dimensions that fail on a particular message are
 * reported as measurement gaps rather than zeros, so they weigh nothing in the composite.
 * A dimension that cannot be honored at all is a schema error and raises
 * {@link QualitySchemaException} deterministically on the type's first scoring.</p>
 *
 * <p>The annotations are generated from {@code quality.proto} into this package —
 * {@link QualityRules} and {@link QualityDimension} — and travel with the descriptor, so they
 * survive descriptors linked without the extension registered. Expressions are evaluated on the
 * shared CEL environment from {@link ai.pipestream.proto.cel.CelEvaluator}, extended with
 * {@code exp} and {@code clamp} for decay curves and explicit range bounds.</p>
 *
 * <p>This package is the measuring counterpart of {@code protomolt-protobuf-validation}:
 * validation decides whether data is admissible, quality says how good the admissible data is.
 * Consumers hold it at the boundaries — the Kafka serde and the validating gRPC server
 * interceptor both turn a composite score into an admission decision.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/quality.md">Quality
 * scoring guide</a> for the annotation form and the scoring rules.</p>
 */
package ai.pipestream.proto.quality;
