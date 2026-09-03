package ai.protomolt.proto.kafka.connect.opensearch;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.search.index.opensearch.OpenSearchDocumentMapper;
import ai.protomolt.proto.search.index.opensearch.OpenSearchSink;
import ai.protomolt.proto.search.index.spi.IndexerContext;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.kafka.wire.ConfluentWireFormat;
import ai.protomolt.proto.mapper.MappingException;
import ai.protomolt.proto.mapper.ProtoFieldMapperImpl;
import ai.protomolt.proto.validate.ValidationResult;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * The sink task: each {@code put} batch is decoded into document messages, mapped through
 * the shared {@link IndexMapping} (descriptor indexing hints, {@code google.protobuf.Any}
 * expansion, and the declared-rules payload gate included), and written as one OpenSearch
 * bulk request. Document ids are deterministic — the configured {@code document.id.path}
 * or topic-partition-offset — so a redelivered batch overwrites its own documents and
 * at-least-once delivery converges to the latest write.
 *
 * <p>Undecodable, invalid, or unmappable values are {@link DataException}s, routed by the
 * worker's {@code errors.tolerance}. A failed bulk write is a {@link RetriableException}
 * so the framework redelivers the batch.
 */
public final class OpenSearchSinkTask extends SinkTask {

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSinkTask.class);

    /** The slice of {@link OpenSearchSink} the task uses; the test hook's seam. */
    interface IndexClient extends AutoCloseable {
        boolean ensureIndex(String index, IndexMapping mapping) throws IOException;

        void bulkWrite(String index, Map<String, Map<String, Object>> documentsById,
                       boolean refresh) throws IOException;

        @Override
        void close();
    }

    /** Test hook: replaces the HTTP-backed client (e.g. an in-memory recorder). */
    Function<OpenSearchSinkConfig, IndexClient> clientFactory =
            config -> asClient(new OpenSearchSink(config.url()));

    private OpenSearchSinkConfig config;
    private Descriptor descriptor;
    private OpenSearchDocumentMapper mapper;
    private ProtoFieldMapperImpl fieldMapper;
    private IndexMapping mapping;
    private IndexClient client;

    @Override
    public void start(Map<String, String> props) {
        config = new OpenSearchSinkConfig(props);
        List<FileDescriptor> files = ConnectDescriptors.linkedFiles(config.descriptorSetBase64());
        descriptor = ConnectDescriptors.messageType(files, config.messageType());
        DescriptorRegistry registry = DescriptorRegistry.create();
        files.forEach(registry::registerFile);
        fieldMapper = new ProtoFieldMapperImpl(registry);
        IndexerContext context = new IndexerContext(fieldMapper, registry, null);
        // validate=false is the operator's whole-surface switch: it suspends the Any
        // payload gate too, not just the top-level rules. The schema-level
        // validate_payloads opt-out remains the per-field control.
        mapper = config.validate()
                ? new OpenSearchDocumentMapper(context)
                : new OpenSearchDocumentMapper(context, false, List.of());
        mapping = context.mappingFactory().create(descriptor);
        if (client != null) {
            client.close();
        }
        client = clientFactory.apply(config);
        if (config.ensureIndex()) {
            try {
                boolean created = client.ensureIndex(config.index(), mapping);
                if (created) {
                    LOG.info("Created OpenSearch index '{}' from the {} mapping",
                            config.index(), descriptor.getFullName());
                }
            } catch (IOException e) {
                throw new ConnectException("Ensuring OpenSearch index '" + config.index()
                        + "' failed: " + e.getMessage(), e);
            }
        }
        LOG.info("OpenSearch sink started: {} -> index {} ({} mapped field(s))",
                descriptor.getFullName(), config.index(), mapping.fields().size());
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        Map<String, Map<String, Object>> documents = new LinkedHashMap<>(records.size());
        for (SinkRecord record : records) {
            Message message = decode(record);
            if (config.validate()) {
                validate(message, record);
            }
            documents.put(documentId(message, record), document(message, record));
        }
        try {
            client.bulkWrite(config.index(), documents, config.refresh());
        } catch (IOException e) {
            throw new RetriableException("OpenSearch bulk write to '" + config.index()
                    + "' failed: " + e.getMessage(), e);
        }
    }

    private void validate(Message message, SinkRecord record) {
        ValidationResult result = ValidationResult.validate(message);
        if (!result.valid()) {
            throw new DataException("Record violates the declared rules of "
                    + descriptor.getFullName() + " (topic " + record.topic() + ", offset "
                    + record.kafkaOffset() + "): " + result.violations());
        }
    }

    private Map<String, Object> document(Message message, SinkRecord record) {
        try {
            return mapper.map(message, mapping);
        } catch (ValidationResult.ValidationException e) {
            throw new DataException("Record's google.protobuf.Any payload violates its "
                    + "declared rules (topic " + record.topic() + ", offset "
                    + record.kafkaOffset() + "): " + e.getMessage(), e);
        } catch (MappingException | RuntimeException e) {
            // Malformed/unknown Any, bad mapping path: properties of this record, not of the
            // connector — data errors.
            throw new DataException("Record does not map to an OpenSearch document as "
                    + descriptor.getFullName() + " (topic " + record.topic() + ", offset "
                    + record.kafkaOffset() + "): " + e.getMessage(), e);
        }
    }

    private String documentId(Message message, SinkRecord record) {
        String path = config.documentIdPath();
        if (path.isEmpty()) {
            return record.topic() + "-" + record.kafkaPartition() + "-" + record.kafkaOffset();
        }
        Object id;
        try {
            // includeDefaults: a numeric id of 0 is a legal id, not an unset field.
            id = fieldMapper.getValue(message, path, true);
        } catch (MappingException e) {
            throw new DataException("document.id.path '" + path + "' does not resolve on "
                    + descriptor.getFullName() + ": " + e.getMessage(), e);
        }
        if (id == null) {
            throw new DataException("document.id.path '" + path + "' is unset on the record "
                    + "(topic " + record.topic() + ", offset " + record.kafkaOffset() + ")");
        }
        String rendered = scalarId(id, path);
        if (rendered.isBlank()) {
            throw new DataException("document.id.path '" + path + "' is blank on the record "
                    + "(topic " + record.topic() + ", offset " + record.kafkaOffset() + ")");
        }
        return rendered;
    }

    /**
     * Document ids must come from scalar leaves: a message would stringify as an unstable
     * text-format dump and a repeated field as a bracketed list — an empty one, "[]",
     * would even silently collapse every id-less record into one document.
     */
    private static String scalarId(Object id, String path) {
        return switch (id) {
            case String text -> text;
            case Number number -> String.valueOf(number);
            case Boolean flag -> String.valueOf(flag);
            case Descriptors.EnumValueDescriptor enumValue -> enumValue.getName();
            default -> throw new DataException("document.id.path '" + path + "' must resolve to "
                    + "a scalar leaf, but resolved to " + id.getClass().getName());
        };
    }

    private DynamicMessage decode(SinkRecord record) {
        Object value = record.value();
        if (value == null) {
            throw new DataException("Record value is null (topic " + record.topic()
                    + ", offset " + record.kafkaOffset() + ")");
        }
        try {
            return switch (config.valueFormat()) {
                case PROTOBUF -> DynamicMessage.parseFrom(descriptor, asBytes(value));
                case CONFLUENT -> DynamicMessage.parseFrom(descriptor,
                        ConfluentWireFormat.payload(asBytes(value)));
                case JSON -> {
                    String json = value instanceof byte[] bytes
                            ? new String(bytes, StandardCharsets.UTF_8)
                            : value.toString();
                    DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
                    JsonFormat.parser().ignoringUnknownFields().merge(json, builder);
                    yield builder.build();
                }
            };
        } catch (DataException e) {
            throw e;
        } catch (Exception e) {
            throw new DataException("Record value does not decode as " + descriptor.getFullName()
                    + " (" + config.valueFormat() + ", topic " + record.topic() + ", offset "
                    + record.kafkaOffset() + "): " + e.getMessage(), e);
        }
    }

    private static byte[] asBytes(Object value) {
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        throw new DataException("Record value must be byte[] for this format; got "
                + value.getClass().getName()
                + " (use the ByteArrayConverter for value.converter)");
    }

    @Override
    public void stop() {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public String version() {
        return OpenSearchSinkConnector.pluginVersion();
    }

    private static IndexClient asClient(OpenSearchSink sink) {
        return new IndexClient() {
            @Override
            public boolean ensureIndex(String index, IndexMapping mapping) throws IOException {
                return sink.ensureIndex(index, mapping);
            }

            @Override
            public void bulkWrite(String index, Map<String, Map<String, Object>> documentsById,
                                  boolean refresh) throws IOException {
                sink.bulkWrite(index, documentsById, refresh);
            }

            @Override
            public void close() {
                sink.close();
            }
        };
    }
}
