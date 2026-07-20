/**
 * Integration with Confluent Schema Registry and compatible endpoints.
 *
 * <p>Reading is covered by two {@link ai.pipestream.proto.descriptors.DescriptorLoader}
 * implementations with different inputs. {@link ConfluentSchemaRegistryLoader} speaks the
 * subjects REST API and consumes what a registry actually serves — {@code .proto} schema text
 * with optional schema references, resolved recursively and compiled into descriptors.
 * {@link ConfluentDescriptorSource} consumes a pre-compiled binary {@code FileDescriptorSet}
 * served over HTTP or read from the classpath, and does not speak the subjects API.</p>
 *
 * <p>{@link ConfluentSchemaPublisher} is the write-side counterpart, implementing
 * {@link ai.pipestream.proto.sources.publish.SchemaPublisher}: it registers a
 * {@link ai.pipestream.proto.sources.ProtoSourceSet} in reverse-topological import order so
 * every schema reference exists before the file that declares the import, and treats identical
 * content as a no-op. Well-known {@code google/protobuf} imports are never registered.</p>
 *
 * <p>All three types target Apicurio Registry's ccompat facade as well as Confluent itself.
 * Apicurio's native v3 API is handled instead by {@code protomolt-schema-apicurio}.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/descriptor-sources.md">descriptor
 * sources guide</a> and the
 * <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/publishing.md">publishing
 * guide</a> for usage.</p>
 */
package ai.pipestream.proto.schema.confluent;
