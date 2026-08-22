package ai.pipestream.proto.kafka.serde;

import ai.pipestream.proto.descriptors.DescriptorLoader.DescriptorLoadException;
import ai.pipestream.proto.schema.confluent.ConfluentSchemaRegistryLoader;
import com.google.protobuf.Descriptors.FileDescriptor;
import java.net.URI;
import java.util.OptionalInt;

/**
 * The two questions {@link SchemaIds} asks a registry, and nothing else.
 *
 * <p>It exists so the caching and backoff around those questions can be tested without a
 * registry to point at. Those are the parts that decide whether an outage costs one lookup
 * or one per record, and they are invisible from the outside: a serde with broken backoff
 * still produces correct bytes, just slowly enough to take a cluster down with it.
 *
 * <p>Declared here rather than on the loader so the dependency runs the way the modules do:
 * the serde knows about the registry client, not the other way round.
 */
interface SchemaLookup extends AutoCloseable {

    /** The id registered for a subject, or empty when the registry has none. */
    OptionalInt idForSubject(String subject) throws DescriptorLoadException;

    /** The schema an id names. */
    FileDescriptor schemaById(int schemaId) throws DescriptorLoadException;

    @Override
    void close();

    /**
     * The production lookup: a Confluent-compatible registry over HTTP. The loader keeps its
     * own untimed id cache, so a schema resolved once is never fetched again.
     */
    static SchemaLookup over(String registryUrl) {
        ConfluentSchemaRegistryLoader loader =
                new ConfluentSchemaRegistryLoader(URI.create(registryUrl.trim()));
        return new SchemaLookup() {
            @Override
            public OptionalInt idForSubject(String subject) throws DescriptorLoadException {
                return loader.idForSubject(subject);
            }

            @Override
            public FileDescriptor schemaById(int schemaId) throws DescriptorLoadException {
                return loader.schemaById(schemaId);
            }

            @Override
            public void close() {
                loader.close();
            }
        };
    }
}
