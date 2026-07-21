/**
 * Descriptor lookup for protobuf types, and the loader SPI that feeds it.
 *
 * <p>{@link DescriptorRegistry} — also published under the name
 * {@link DescriptorCatalog} — is the entry point. It aggregates any number of
 * loaders, caches descriptors by full and simple type name, resolves types on
 * demand, and negative-caches misses so a repeated lookup for an unknown type
 * does not re-consult every loader. Google's well-known types are registered at
 * construction.</p>
 *
 * <p>{@link DescriptorLoader} is the extension point. An implementation answers
 * for one descriptor source and reports its availability; failures surface as
 * {@link DescriptorLoader.DescriptorLoadException}. Two implementations ship
 * here: {@link ClasspathDescriptorLoader}, which resolves descriptors from
 * generated protobuf classes on the classpath, and
 * {@link GoogleDescriptorLoader}, which reads a binary
 * {@code FileDescriptorSet} from a classpath resource. Loaders backed by schema
 * registries, Git repositories and Maven artifacts live in their own modules
 * and are registered against the same registry.</p>
 *
 * <p>Every ProtoMolt feature that needs a descriptor obtains it through this
 * package; {@code ai.pipestream.proto.helpers} builds its conversion and
 * mapping utilities on a registry instance, and
 * {@code ai.pipestream.proto.sources} produces descriptors that can be fed
 * back into one.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/descriptor-sources.md">Descriptor
 * sources guide</a> for the full loader table and usage.</p>
 */
package ai.pipestream.proto.descriptors;
