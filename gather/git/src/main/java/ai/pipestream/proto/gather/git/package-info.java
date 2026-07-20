/**
 * A proto gatherer backed by git repositories.
 *
 * <p>{@link GitProtoGatherer} implements
 * {@link ai.pipestream.proto.gather.ProtoGatherer} over a persistent clone cache: a
 * repository is cloned on first use and fetched and hard reset to the requested ref on
 * reuse, with concurrent access serialized so parallel builds may share one cache. Three
 * layout modes — multi-module, explicit paths, and single subdirectory — determine how the
 * checkout is mapped onto import paths.</p>
 *
 * <p>{@link GatherGitAction} exposes the same capability as the {@code gather-git} verb,
 * returning both the source texts and the compiled descriptor set, which makes git a usable
 * contract source for services that do not enable gRPC reflection.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/gathering.md">gathering
 * guide</a>; the SPI and the local sources live in {@code ai.pipestream.proto.gather}.</p>
 */
package ai.pipestream.proto.gather.git;
