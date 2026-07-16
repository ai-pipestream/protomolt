package ai.pipestream.proto.kafka.connect.iceberg;

import ai.pipestream.proto.lake.iceberg.IcebergSink;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * The sink task: each {@code put} batch is decoded into row messages and committed as one Iceberg
 * snapshot through {@link IcebergSink}. Because the whole batch is one snapshot, offsets advance
 * only after the commit succeeds; a failed append surfaces as a {@link RetriableException} so the
 * framework redelivers. Delivery is therefore at-least-once: a redelivered batch appends again
 * (Iceberg does not deduplicate), so a downstream reader may see duplicate rows after a retry.
 * Undecodable values are {@link DataException}s, routed by the worker's error tolerance.
 */
public final class IcebergSinkTask extends SinkTask {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkTask.class);

    /** Test hook: replaces catalog construction (e.g. an in-memory catalog). */
    Function<IcebergSinkConfig, Catalog> catalogFactory =
            config -> CatalogUtil.buildIcebergCatalog(
                    config.catalogName(), config.catalogProperties(), null);

    private IcebergSinkConfig config;
    private Catalog catalog;
    private Descriptor descriptor;
    private Table table;

    @Override
    public void start(Map<String, String> props) {
        config = new IcebergSinkConfig(props);
        List<FileDescriptor> files = ConnectDescriptors.linkedFiles(config.descriptorSetBase64());
        descriptor = ConnectDescriptors.messageType(files, config.messageType());
        catalog = catalogFactory.apply(config);
        table = IcebergSink.ensureTable(catalog, config.table(), descriptor,
                config.tableLocation(), config.partitionBy());
        LOG.info("Iceberg sink started: {} -> table {} ({} partition column(s))",
                descriptor.getFullName(), config.table(), config.partitionBy().size());
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        List<Message> batch = new ArrayList<>(records.size());
        for (SinkRecord record : records) {
            batch.add(decode(record));
        }
        try {
            IcebergSink.append(table, descriptor, batch);
        } catch (IOException e) {
            throw new RetriableException("Iceberg append to " + config.table() + " failed: "
                    + e.getMessage(), e);
        }
    }

    private DynamicMessage decode(SinkRecord record) {
        Object value = record.value();
        if (value == null) {
            throw new DataException("Record value is null (topic " + record.topic()
                    + ", offset " + record.kafkaOffset() + ")");
        }
        try {
            switch (config.valueFormat()) {
                case PROTOBUF -> {
                    return DynamicMessage.parseFrom(descriptor, asBytes(value));
                }
                case CONFLUENT -> {
                    return DynamicMessage.parseFrom(descriptor,
                            ConfluentFraming.payload(asBytes(value)));
                }
                default -> {
                    String json = value instanceof byte[] bytes
                            ? new String(bytes, StandardCharsets.UTF_8)
                            : value.toString();
                    DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
                    JsonFormat.parser().ignoringUnknownFields().merge(json, builder);
                    return builder.build();
                }
            }
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
        if (catalog instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                LOG.warn("Closing the Iceberg catalog failed: {}", e.getMessage());
            }
        }
    }

    @Override
    public String version() {
        return IcebergSinkConnector.pluginVersion();
    }
}
