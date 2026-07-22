/**
 * Bundles of rendered files and the sinks that deliver them.
 *
 * <p>A {@link Bundle} is an ordered, path-validated set of rendered files — the unit every
 * emitter produces. {@link BundleSink} is the extension point on the delivery side;
 * {@link DirectorySink} writes under a root directory and {@link GitSink} writes into a git
 * working tree and commits. A sink is always constructed with a destination the caller named,
 * so nothing in the emit pipeline chooses a location on its own. {@link Bundles} covers the
 * cases that need no destination at all, such as the deterministic in-memory zip a verb
 * response carries a whole bundle in.</p>
 *
 * <p>This package is the mirror image of the {@code ai.pipestream.proto.gather} modules:
 * gatherers turn a place into proto sources, renderers turn schemas or messages into a bundle,
 * and sinks turn a bundle back into a place. The renderers themselves live in sibling modules —
 * {@code ai.pipestream.proto.emit.okf} for Open Knowledge Format documents and
 * {@code ai.pipestream.proto.emit.parquet} for columnar message data.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/emitting.md">Emitting
 * bundles guide</a> for the end-to-end flow.</p>
 */
package ai.pipestream.proto.emit;
