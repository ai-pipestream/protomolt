/**
 * Extraction of named metadata values from protobuf messages using CEL selectors.
 *
 * <p>{@link ai.pipestream.proto.metadata.MetadataExtractor} takes a map of metadata name to CEL
 * selector and evaluates each against a message, returning the extracted values by name. The
 * message descriptor is used to build a typed environment for the {@code input} binding, so
 * unknown fields and type errors are reported before any selector runs; validation results are
 * cached per descriptor.</p>
 *
 * <p>Compilation and evaluation are delegated to
 * {@link ai.pipestream.proto.cel.CelEvaluator} from {@code ai.pipestream.proto.cel}. This package
 * covers metadata read from message contents at runtime, as distinct from descriptive metadata
 * declared on schemas as descriptor options under {@code ai.pipestream.proto.meta}.</p>
 *
 * <p>See the
 * <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/metadata.md">Schema metadata
 * guide</a> for how the two kinds relate.</p>
 */
package ai.pipestream.proto.metadata;
