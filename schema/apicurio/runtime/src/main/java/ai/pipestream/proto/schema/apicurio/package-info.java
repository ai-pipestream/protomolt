/**
 * Integration with Apicurio Registry v3, packaged as a Quarkus extension runtime.
 *
 * <p>{@link ApicurioDescriptorLoader} implements
 * {@link ai.pipestream.proto.descriptors.DescriptorLoader} over the Apicurio v3 SDK: it
 * searches a group for {@code PROTOBUF} artifacts and resolves registry references recursively
 * so an artifact whose {@code .proto} imports other registered artifacts can be parsed. It is
 * usable as plain Java through its builder, with no CDI required.
 * {@link ApicurioSchemaPublisher} is the write-side counterpart, implementing
 * {@link ai.pipestream.proto.sources.publish.SchemaPublisher} and registering each file's
 * imports as first-class registry references.</p>
 *
 * <p>Under Quarkus, {@link ApicurioDescriptorLoaderProducer} owns the single
 * {@link io.apicurio.registry.rest.client.RegistryClient} and its transport, and
 * {@link ApicurioDescriptorInstaller} wires the loader into an available
 * {@link ai.pipestream.proto.descriptors.DescriptorRegistry} at startup, optionally bulk-loading
 * then. {@link ProtoToolsApicurioConfig} maps the {@code pipestream.proto.apicurio} configuration
 * root. The matching build-time processor lives in the {@code deployment} subpackage.</p>
 *
 * <p>{@link ApicurioProtobufParseFallback} covers the case where the registry is unreachable but
 * the message type is known ahead of time, stripping the Kafka wire-format prefix and parsing
 * the payload with a generated message class. Apicurio's Confluent-compatible ccompat facade is
 * served instead by {@code protomolt-schema-confluent}.</p>
 *
 * <p>See the <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/descriptor-sources.md">descriptor
 * sources guide</a> and the
 * <a href="https://github.com/ai-pipestream/protomolt/blob/main/docs/publishing.md">publishing
 * guide</a> for usage.</p>
 */
package ai.pipestream.proto.schema.apicurio;
