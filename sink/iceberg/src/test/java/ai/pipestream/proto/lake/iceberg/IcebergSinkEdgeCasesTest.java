package ai.pipestream.proto.lake.iceberg;

import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The sink's argument and boundary handling, which the end-to-end suites never exercise:
 * an empty batch is refused before any file is written, null arguments fail on the parameter
 * they are null at, and an explicit table location is honored by the created table.
 */
class IcebergSinkEdgeCasesTest {

    private static InMemoryCatalog catalog;
    private static Descriptor type;

    @BeforeAll
    static void start() throws Exception {
        type = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                        .add("e/tick.proto", """
                                syntax = "proto3";
                                package e;
                                message Tick { string symbol = 1; int64 seq = 2; }
                                """, "test").build())
                .descriptorFor("e/tick.proto").orElseThrow().findMessageTypeByName("Tick");
        catalog = new InMemoryCatalog();
        catalog.initialize("edge", Map.of());
        catalog.createNamespace(Namespace.of("edge"));
    }

    @AfterAll
    static void stop() throws Exception {
        catalog.close();
    }

    private static DynamicMessage tick(long seq) {
        return DynamicMessage.newBuilder(type)
                .setField(type.findFieldByName("symbol"), "S")
                .setField(type.findFieldByName("seq"), seq)
                .build();
    }

    @Test
    void anEmptyBatchIsRefusedBeforeAnythingIsWritten() {
        Table table = IcebergSink.ensureTable(catalog,
                TableIdentifier.of("edge", "empty_batch"), type);

        assertThatThrownBy(() -> IcebergSink.append(table, type, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty");
        table.refresh();
        assertThat(table.snapshots()).as("no snapshot was committed").isEmpty();
    }

    @Test
    void nullArgumentsFailOnTheOffendingParameter() {
        TableIdentifier id = TableIdentifier.of("edge", "nulls");
        assertThatNullPointerException()
                .isThrownBy(() -> IcebergSink.ensureTable(null, id, type))
                .withMessage("catalog");
        assertThatNullPointerException()
                .isThrownBy(() -> IcebergSink.ensureTable(catalog, null, type))
                .withMessage("identifier");
        Table table = IcebergSink.ensureTable(catalog, id, type);
        assertThatNullPointerException()
                .isThrownBy(() -> IcebergSink.append(null, type, List.of(tick(1))))
                .withMessage("table");
    }

    @Test
    void anExplicitLocationIsWhereTheTableLivesAndWrites() throws Exception {
        String location = "file:///tmp/protomolt-edge/owned";
        TableIdentifier id = TableIdentifier.of("edge", "located");

        Table table = IcebergSink.ensureTable(catalog, id, type, location);
        assertThat(table.location()).isEqualTo(location);

        // The table is fully usable: the location is the base the data files are placed under.
        List<DataFile> files = IcebergSink.append(table, type, List.of(tick(7)));
        assertThat(files).hasSize(1);
        assertThat(files.getFirst().location()).startsWith(location);
        assertThat(files.getFirst().recordCount()).isEqualTo(1);
    }
}
