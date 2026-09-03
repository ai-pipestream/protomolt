package ai.protomolt.proto.metric.iceberg;

import ai.protomolt.proto.iceberg.IcebergSink;
import ai.protomolt.proto.iceberg.IcebergSchemas;
import ai.protomolt.proto.metric.MetricRow;
import ai.protomolt.proto.metric.spi.RollupSink;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMappingParser;

/**
 * The rollup sink over an Iceberg catalog: the rollup's schema is a flat
 * message synthesized from the member names (dimension columns are the
 * rendered strings, measure columns are the wire's doubles), and the
 * replace is one Iceberg transaction — the table swaps from the old
 * rollup to the new one atomically, never serving a mix. A rebuild is a
 * full replacement, not an append, because a rollup's contract is "the
 * whole answer as of its snapshot", and the snapshot id in the result is
 * the evidence.
 */
public final class IcebergRollupSink implements RollupSink {

    /** What a member name must look like to become a rollup column. */
    private static final Pattern COLUMN = Pattern.compile("[a-z_][a-z0-9_]{0,199}");

    private final Catalog catalog;
    private final String namespace;

    /**
     * @param catalog the lake catalog rollup tables live in
     * @param namespace the namespace rollup tables are created under; this
     *        sink never creates a namespace itself, so on a catalog that
     *        enforces namespace existence (REST) the operator provides it,
     *        while the JDBC catalog treats namespaces as implicit
     */
    public IcebergRollupSink(Catalog catalog, String namespace) {
        if (catalog == null) {
            throw new IllegalArgumentException("catalog must not be null");
        }
        if (namespace == null || namespace.isBlank()) {
            throw new IllegalArgumentException("namespace must not be blank");
        }
        this.catalog = catalog;
        this.namespace = namespace;
    }

    /** Table property carrying the source subject: {@value}. */
    public static final String PROPERTY_SOURCE = "protomolt.rollup.source";

    /** Table property carrying the dimension columns (csv): {@value}. */
    public static final String PROPERTY_DIMENSIONS = "protomolt.rollup.dimensions";

    /**
     * Table property carrying the measure columns as {@code name:AGGREGATE}
     * csv entries: {@value}.
     */
    public static final String PROPERTY_MEASURES = "protomolt.rollup.measures";

    @Override
    public Written replace(String sourceSubject, String table, List<String> dimensions,
            List<MeasureColumn> measures, List<MetricRow> rows) {
        List<String> measureNames = measures.stream().map(MeasureColumn::member).toList();
        Descriptor descriptor = rollupDescriptor(table, dimensions, measureNames);
        Schema schema = IcebergSchemas.fromDescriptor(descriptor);
        TableIdentifier identifier = TableIdentifier.of(namespace, table);
        Transaction replace;
        try {
            replace = catalog.buildTable(identifier, schema)
                    .createOrReplaceTransaction();
        } catch (org.apache.iceberg.exceptions.NoSuchNamespaceException e) {
            throw new IllegalStateException("namespace '" + namespace + "' does not exist"
                    + " in the lake: the operator provides it, this sink never creates"
                    + " one", e);
        }
        // The mapping derives from the transaction table's schema, which
        // carries the catalog-assigned field ids (the pre-assignment ids
        // would resolve columns wrongly, as the sink's ensureTable notes).
        // The rollup's own declaration rides the same properties, so the
        // table is self-describing: a resolver can serve it back as a
        // queryable subject with no side-channel configuration.
        replace.updateProperties()
                .set(TableProperties.DEFAULT_NAME_MAPPING,
                        NameMappingParser.toJson(MappingUtil.create(replace.table().schema())))
                .set(PROPERTY_SOURCE, sourceSubject)
                .set(PROPERTY_DIMENSIONS, String.join(",", dimensions))
                .set(PROPERTY_MEASURES, String.join(",", measures.stream()
                        .map(column -> column.member() + ":" + column.sourceAggregate().name())
                        .toList()))
                .commit();
        if (!rows.isEmpty()) {
            List<DynamicMessage> messages = new ArrayList<>(rows.size());
            for (MetricRow row : rows) {
                DynamicMessage.Builder message = DynamicMessage.newBuilder(descriptor);
                for (String dimension : dimensions) {
                    message.setField(descriptor.findFieldByName(dimension),
                            row.getDimensionsOrDefault(dimension, ""));
                }
                for (String measure : measureNames) {
                    if (row.containsMeasures(measure)) {
                        message.setField(descriptor.findFieldByName(measure),
                                row.getMeasuresOrThrow(measure));
                    }
                }
                messages.add(message.build());
            }
            try {
                IcebergSink.append(replace.table(), descriptor, messages);
            } catch (IOException e) {
                throw new UncheckedIOException(
                        "cannot write the rollup batch for '" + identifier + "'", e);
            }
        }
        replace.commitTransaction();
        Table committed = catalog.loadTable(identifier);
        long snapshotId = committed.currentSnapshot() == null
                ? 0L
                : committed.currentSnapshot().snapshotId();
        return new Written(identifier.toString(), rows.size(), snapshotId);
    }

    /**
     * The rollup row type: one flat message, dimension fields as strings
     * and measure fields as doubles, in the given column order. A member
     * whose name cannot be a proto field name refuses loudly naming it.
     */
    static Descriptor rollupDescriptor(
            String table, List<String> dimensions, List<String> measures) {
        DescriptorProto.Builder message = DescriptorProto.newBuilder().setName("RollupRow");
        int number = 1;
        for (String dimension : dimensions) {
            message.addField(column(dimension, number++,
                    FieldDescriptorProto.Type.TYPE_STRING));
        }
        for (String measure : measures) {
            message.addField(column(measure, number++,
                    FieldDescriptorProto.Type.TYPE_DOUBLE));
        }
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("rollup/" + table + ".proto")
                .setPackage("rollup")
                .setSyntax("proto3")
                .addMessageType(message)
                .build();
        try {
            return FileDescriptor.buildFrom(file, new FileDescriptor[0])
                    .findMessageTypeByName("RollupRow");
        } catch (DescriptorValidationException e) {
            throw new IllegalStateException(
                    "cannot synthesize the rollup row type for '" + table + "'", e);
        }
    }

    private static FieldDescriptorProto.Builder column(
            String member, int number, FieldDescriptorProto.Type type) {
        if (!COLUMN.matcher(member).matches()) {
            throw new IllegalArgumentException("member '" + member + "' cannot become a"
                    + " rollup column: names are lower_snake identifiers");
        }
        return FieldDescriptorProto.newBuilder()
                .setName(member).setNumber(number).setType(type)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }
}
