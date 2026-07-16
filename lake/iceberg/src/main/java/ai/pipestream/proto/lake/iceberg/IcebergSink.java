package ai.pipestream.proto.lake.iceberg;

import ai.pipestream.proto.emit.parquet.ParquetEmitter;
import ai.pipestream.proto.emit.parquet.ProtoParquetSchemas;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMappingParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Protobuf messages into an Apache Iceberg table: the batch becomes one Parquet data file
 * written by ProtoMolt's own descriptor-driven emitter, delivered through the table's
 * {@code FileIO}, and committed as an append — a real snapshot any engine can read. Works
 * against any Iceberg catalog: REST, JDBC, in-memory for tests. The table carries a name
 * mapping derived from its schema, so the emitter's files (which carry names, not Iceberg
 * field ids) resolve columns exactly.
 */
public final class IcebergSink {

    private IcebergSink() {
    }

    /**
     * Loads the table, creating it (unpartitioned, schema from the descriptor, name
     * mapping installed) when absent.
     */
    public static Table ensureTable(Catalog catalog, TableIdentifier identifier,
                                    Descriptor descriptor) {
        return ensureTable(catalog, identifier, descriptor, null, List.of());
    }

    /**
     * @param location explicit table base location, or null for the catalog's default —
     *        useful when the caller must own the directory tree (shared-volume rigs where
     *        the catalog service and the writer run as different users)
     */
    public static Table ensureTable(Catalog catalog, TableIdentifier identifier,
                                    Descriptor descriptor, String location) {
        return ensureTable(catalog, identifier, descriptor, location, List.of());
    }

    /**
     * @param partitionBy partition columns (see {@link IcebergPartitions#fromHints}); empty for
     *        an unpartitioned table. The spec is bound by column name, so it survives the fresh
     *        field ids a catalog assigns on creation.
     */
    public static Table ensureTable(Catalog catalog, TableIdentifier identifier,
                                    Descriptor descriptor, String location,
                                    List<IcebergPartitions.PartitionField> partitionBy) {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(identifier, "identifier");
        if (catalog.tableExists(identifier)) {
            return catalog.loadTable(identifier);
        }
        Schema schema = IcebergSchemas.fromDescriptor(descriptor);
        PartitionSpec spec = partitionBy.isEmpty()
                ? PartitionSpec.unpartitioned()
                : IcebergPartitions.bind(schema, partitionBy);
        var builder = catalog.buildTable(identifier, schema).withPartitionSpec(spec);
        if (location != null) {
            builder.withLocation(location);
        }
        Table table = builder.create();
        // The mapping must be derived AFTER creation: catalogs assign fresh field ids to
        // the schema they store, and a mapping minted from the pre-creation ids would
        // resolve nested columns to the wrong (or no) fields.
        table.updateProperties()
                .set(TableProperties.DEFAULT_NAME_MAPPING,
                        NameMappingParser.toJson(MappingUtil.create(table.schema())))
                .commit();
        return table;
    }

    /**
     * Appends one batch and commits it as a single snapshot. An unpartitioned table takes the
     * whole batch as one data file; a partitioned table routes each message to its partition and
     * writes one data file per partition the batch touches. Every file carries column metrics
     * (bounds, value and null counts) read from the Parquet footer, so engines can prune it.
     *
     * @return the committed data files (one for an unpartitioned table, one per partition
     *         otherwise), each with path, size, record count, column metrics, and partition
     */
    public static List<DataFile> append(Table table, Descriptor descriptor,
                                        List<? extends Message> messages) throws IOException {
        Objects.requireNonNull(table, "table");
        if (messages.isEmpty()) {
            throw new IllegalArgumentException("An empty batch has nothing to commit");
        }
        // Stamp the table's own field ids into the file schema - the native way Iceberg
        // readers resolve columns, no name-mapping fallback in the read path.
        Map<String, Integer> idsByPath = org.apache.iceberg.types.TypeUtil
                .indexByName(table.schema().asStruct());
        ProtoParquetSchemas.FieldIdResolver ids = idsByPath::get;
        MetricsConfig metricsConfig = MetricsConfig.forTable(table);

        List<DataFile> committed = new ArrayList<>();
        if (table.spec().isUnpartitioned()) {
            committed.add(writeFile(table, descriptor, messages, ids, metricsConfig, null));
        } else {
            // One data file per partition the batch touches: a data file belongs to exactly one
            // partition, so a batch spanning partitions must be split. Group by the partition
            // tuple each message resolves to under the spec's transforms.
            Map<PartitionKey, List<Message>> byPartition = new LinkedHashMap<>();
            for (Message message : messages) {
                PartitionKey key = new PartitionKey(table.spec(), table.schema());
                key.partition(IcebergPartitions.row(message, table.schema()));
                byPartition.computeIfAbsent(key.copy(), unused -> new ArrayList<>()).add(message);
            }
            for (Map.Entry<PartitionKey, List<Message>> group : byPartition.entrySet()) {
                committed.add(writeFile(table, descriptor, group.getValue(), ids, metricsConfig,
                        group.getKey()));
            }
        }

        AppendFiles append = table.newAppend();
        committed.forEach(append::appendFile);
        append.commit();
        return committed;
    }

    private static DataFile writeFile(Table table, Descriptor descriptor,
                                      List<? extends Message> messages,
                                      ProtoParquetSchemas.FieldIdResolver ids,
                                      MetricsConfig metricsConfig, StructLike partition)
            throws IOException {
        byte[] parquet = ParquetEmitter.toBytes(descriptor, messages, ids);
        String location = table.locationProvider().newDataLocation(
                "protomolt-" + UUID.randomUUID() + ".parquet");
        OutputFile file = table.io().newOutputFile(location);
        try (PositionOutputStream out = file.create()) {
            out.write(parquet);
        }
        // Metrics come from the footer's own row count and per-column statistics; the record
        // count they carry is authoritative, so it is not set separately.
        DataFiles.Builder dataFile = DataFiles.builder(table.spec())
                .withPath(location)
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(parquet.length)
                .withMetrics(IcebergMetrics.forParquet(parquet, metricsConfig));
        if (partition != null) {
            dataFile.withPartition(partition);
        }
        return dataFile.build();
    }
}
