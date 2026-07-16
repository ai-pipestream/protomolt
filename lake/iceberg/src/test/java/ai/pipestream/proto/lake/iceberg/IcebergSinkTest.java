package ai.pipestream.proto.lake.iceberg;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The whole lane, end to end against a real Iceberg catalog implementation: descriptor
 * creates the table, ProtoMolt's own Parquet becomes committed snapshots, and Iceberg's own
 * generic reader (not ours) gets every value back — lists, maps, nested structs, real
 * timestamp columns, and JSON-carried Struct content. If the emitter's file shapes ever
 * drift from what Iceberg readers expect, this breaks loudly.
 */
class IcebergSinkTest {

    private static final String PROTO = """
            syntax = "proto3";
            package lake.test;
            import "google/protobuf/timestamp.proto";
            import "google/protobuf/struct.proto";
            message Event {
              string id = 1;
              int64 count = 2;
              double score = 3;
              bool active = 4;
              repeated string tags = 5;
              map<string, int64> attrs = 6;
              Origin origin = 7;
              google.protobuf.Timestamp at = 8;
              google.protobuf.Struct extra = 9;
            }
            message Origin { string host = 1; int32 port = 2; }
            """;

    private static FileDescriptor file;
    private static InMemoryCatalog catalog;

    @BeforeAll
    static void start() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("lake/test/lake.proto", PROTO, "test").build());
        file = compiled.descriptorFor("lake/test/lake.proto").orElseThrow();
        catalog = new InMemoryCatalog();
        catalog.initialize("test", Map.of());
        catalog.createNamespace(Namespace.of("protomolt"));
    }

    @AfterAll
    static void stop() throws Exception {
        catalog.close();
    }

    private static DynamicMessage event(Descriptor type, int i) throws Exception {
        Descriptor origin = file.findMessageTypeByName("Origin");
        Descriptor timestamp = type.findFieldByName("at").getMessageType();
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("id"), "e-" + i)
                .setField(type.findFieldByName("count"), (long) i)
                .setField(type.findFieldByName("score"), i + 0.25)
                .setField(type.findFieldByName("active"), i % 2 == 0)
                .setField(type.findFieldByName("origin"), DynamicMessage.newBuilder(origin)
                        .setField(origin.findFieldByName("host"), "h" + i)
                        .setField(origin.findFieldByName("port"), 9000 + i)
                        .build())
                .setField(type.findFieldByName("at"), DynamicMessage.newBuilder(timestamp)
                        .setField(timestamp.findFieldByName("seconds"), 1_700_000_000L + i)
                        .setField(timestamp.findFieldByName("nanos"), 0)
                        .build());
        builder.addRepeatedField(type.findFieldByName("tags"), "t" + i);
        builder.addRepeatedField(type.findFieldByName("tags"), "shared");
        Descriptor entry = type.findFieldByName("attrs").getMessageType();
        builder.addRepeatedField(type.findFieldByName("attrs"), DynamicMessage.newBuilder(entry)
                .setField(entry.findFieldByName("key"), "retries")
                .setField(entry.findFieldByName("value"), (long) i)
                .build());
        DynamicMessage.Builder extra = DynamicMessage.newBuilder(
                type.findFieldByName("extra").getMessageType());
        com.google.protobuf.util.JsonFormat.parser()
                .merge("{\"origin\": \"probe\"}", extra);
        builder.setField(type.findFieldByName("extra"), extra.build());
        return builder.build();
    }

    @Test
    void appendsBatchesAsSnapshotsIcebergReadersUnderstand() throws Exception {
        Descriptor type = file.findMessageTypeByName("Event");
        TableIdentifier id = TableIdentifier.of("protomolt", "events");

        Table table = IcebergSink.ensureTable(catalog, id, type);
        // Idempotent: a second ensure loads the same table.
        assertThat(IcebergSink.ensureTable(catalog, id, type).name())
                .isEqualTo(table.name());
        assertThat(table.schema().findField("origin.host")).isNotNull();
        assertThat(table.properties()).containsKey("schema.name-mapping.default");

        DataFile first = IcebergSink.append(table, type,
                List.of(event(type, 0), event(type, 1))).getFirst();
        assertThat(first.recordCount()).isEqualTo(2);
        IcebergSink.append(table, type, List.of(event(type, 2)));

        table.refresh();
        List<org.apache.iceberg.Snapshot> snapshots = new ArrayList<>();
        table.snapshots().forEach(snapshots::add);
        assertThat(snapshots).hasSize(2);

        // Read back through Iceberg's OWN reader - the interop proof.
        List<Record> rows = new ArrayList<>();
        try (CloseableIterable<Record> scan = IcebergGenerics.read(table).build()) {
            scan.forEach(rows::add);
        }
        assertThat(rows).hasSize(3);
        rows.sort(java.util.Comparator.comparing(r -> (String) r.getField("id")));

        Record row = rows.get(1);
        assertThat(row.getField("id")).isEqualTo("e-1");
        assertThat(row.getField("count")).isEqualTo(1L);
        assertThat(row.getField("score")).isEqualTo(1.25);
        assertThat(row.getField("active")).isEqualTo(false);
        List<?> tags = (List<?>) row.getField("tags");
        assertThat(tags).hasSize(2);
        assertThat(tags.get(0)).isEqualTo("t1");
        assertThat(tags.get(1)).isEqualTo("shared");
        Map<?, ?> attrs = (Map<?, ?>) row.getField("attrs");
        assertThat(attrs.get("retries")).isEqualTo(1L);
        Record origin = (Record) row.getField("origin");
        assertThat(origin.getField("host")).isEqualTo("h1");
        assertThat(origin.getField("port")).isEqualTo(9001);
        assertThat(row.getField("at")).hasToString("2023-11-14T22:13:21Z");
        assertThat((String) row.getField("extra")).contains("\"origin\":\"probe\"");
    }

    @Test
    void partitionedAppendWritesOneFilePerPartitionInOneSnapshot() throws Exception {
        Descriptor type = file.findMessageTypeByName("Event");
        Table table = IcebergSink.ensureTable(catalog,
                TableIdentifier.of("protomolt", "partitioned"), type, null,
                List.of(new IcebergPartitions.PartitionField("at", "day"),
                        new IcebergPartitions.PartitionField("active", "identity")));
        assertThat(table.spec().isPartitioned()).isTrue();

        // active == (i % 2 == 0): events 0 and 2 land in one partition, event 1 in another.
        List<DataFile> files = IcebergSink.append(table, type,
                List.of(event(type, 0), event(type, 1), event(type, 2)));
        assertThat(files).hasSize(2);
        assertThat(files).extracting(DataFile::recordCount).containsExactlyInAnyOrder(2L, 1L);

        table.refresh();
        List<org.apache.iceberg.Snapshot> snapshots = new ArrayList<>();
        table.snapshots().forEach(snapshots::add);
        assertThat(snapshots).as("one snapshot for the whole batch").hasSize(1);

        // A predicate on the partition column prunes to just the matching partition's rows.
        List<Record> activeRows = new ArrayList<>();
        try (CloseableIterable<Record> scan = IcebergGenerics.read(table)
                .where(org.apache.iceberg.expressions.Expressions.equal("active", true))
                .build()) {
            scan.forEach(activeRows::add);
        }
        assertThat(activeRows).hasSize(2);
        assertThat(activeRows).allSatisfy(r -> assertThat(r.getField("active")).isEqualTo(true));
    }

    @Test
    void appendStampsColumnMetricsKeyedByFieldId() throws Exception {
        Descriptor type = file.findMessageTypeByName("Event");
        Table table = IcebergSink.ensureTable(catalog,
                TableIdentifier.of("protomolt", "metrics"), type);
        // event(i).count == i, so this batch spans count 5..9 - the range a reader prunes on.
        DataFile dataFile = IcebergSink.append(table, type,
                List.of(event(type, 5), event(type, 9))).getFirst();

        int countId = table.schema().findField("count").fieldId();
        int idId = table.schema().findField("id").fieldId();
        assertThat(dataFile.recordCount()).isEqualTo(2);
        assertThat(dataFile.valueCounts()).containsEntry(countId, 2L);
        // Bounds are keyed by the stamped field id and decode to the real min/max.
        assertThat((Long) Conversions.fromByteBuffer(Types.LongType.get(),
                dataFile.lowerBounds().get(countId))).isEqualTo(5L);
        assertThat((Long) Conversions.fromByteBuffer(Types.LongType.get(),
                dataFile.upperBounds().get(countId))).isEqualTo(9L);
        assertThat(dataFile.lowerBounds()).containsKey(idId);
    }

    @Test
    void tableSchemasComeBackAsProtoSource() throws Exception {
        Descriptor type = file.findMessageTypeByName("Event");
        Table table = IcebergSink.ensureTable(catalog,
                TableIdentifier.of("protomolt", "reverse"), type);

        String source = IcebergSchemas.toProtoSource(table.schema(), "lake.reverse.v1", "Event");
        assertThat(source)
                .contains("package lake.reverse.v1;")
                .contains("repeated string tags")
                .contains("map<string, int64> attrs")
                .contains("google.protobuf.Timestamp at")
                .contains("message Origin");

        // The reverse-engineered contract must itself compile.
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("lake/reverse/v1/event.proto", source, "reverse").build());
        Descriptor reversed = compiled.descriptorFor("lake/reverse/v1/event.proto")
                .orElseThrow().findMessageTypeByName("Event");
        assertThat(reversed.findFieldByName("origin").getMessageType().getName())
                .isEqualTo("Origin");
    }
}
