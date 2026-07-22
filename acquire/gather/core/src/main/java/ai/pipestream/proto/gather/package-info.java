/**
 * The proto gatherer SPI and its local sources: filesystem trees and jars.
 *
 * <p>{@link ProtoGatherer} is the extension point — an implementation collects {@code .proto}
 * sources from one kind of origin into a self-contained
 * {@link ai.pipestream.proto.sources.ProtoSourceSet} whose import paths resolve against each
 * other. {@link FilesystemProtoGatherer} and {@link JarProtoGatherer} cover local directories
 * and jar files, {@link JarProtoExtraction} holds the jar reading they share with the Maven
 * gatherer, and {@link CompositeProtoGatherer} merges an ordered list of gatherers into a
 * single source set. Every conflict rule is the same: an import path may be produced more
 * than once only with identical content, otherwise a {@link GatherException} names both
 * origins.</p>
 *
 * <p>{@link GatheringDescriptorLoader} bridges to the descriptor layer, adapting any gatherer
 * to {@link ai.pipestream.proto.descriptors.DescriptorLoader} so gathered types resolve on
 * demand through a {@link ai.pipestream.proto.descriptors.DescriptorRegistry}. Compilation of
 * the gathered set is {@link ai.pipestream.proto.sources.ProtoSourceCompiler}'s job, not this
 * package's.</p>
 *
 * <p>Remote origins live in sibling modules: {@code ai.pipestream.proto.gather.git} for git
 * repositories and {@code ai.pipestream.proto.gather.maven} for artifacts resolved by
 * coordinate.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/gathering.md">gathering
 * guide</a> for how source sets reach the registry.</p>
 */
package ai.pipestream.proto.gather;
