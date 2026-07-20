/**
 * A proto gatherer backed by Maven artifacts resolved by coordinate.
 *
 * <p>{@link MavenProtoGatherer} implements
 * {@link ai.pipestream.proto.gather.ProtoGatherer} using the standalone Maven Resolver, so no
 * Maven installation is required: coordinates are resolved against the configured remote
 * repositories through a local repository cache, optionally following each coordinate's
 * runtime dependency graph, and the resulting jars are scanned for {@code .proto} entries.
 * Extraction is shared with the jar gatherer through
 * {@link ai.pipestream.proto.gather.JarProtoExtraction}, so in-jar paths become import paths
 * and the well-known types the compiler already supplies are skipped.</p>
 *
 * <p>The SPI and the local sources live in {@code ai.pipestream.proto.gather};
 * {@code ai.pipestream.proto.gather.git} is the git-backed sibling.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/gathering.md">gathering
 * guide</a> for how a gathered source set is compiled and registered.</p>
 */
package ai.pipestream.proto.gather.maven;
